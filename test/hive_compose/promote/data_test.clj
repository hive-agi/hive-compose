(ns hive-compose.promote.data-test
  "Pure data lifecycle (hc-9). test.check properties: migrate precedes seed
   per pair; the plan is deterministic (idempotent); no step removes a
   volume; a shared instance is never reset globally (every step on it is
   tenancy-scoped and carries that consumer's prefix); prefix rotation is
   injective and stays under the consumer's base prefix."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.promote.capability :as capability]
            [hive-compose.promote.data :as data]
            [hive-compose.promote.names :as names]
            [hive-compose.promote.stack :as stack]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def projects
  {"kafka" {:project/id "kafka" :project/dir "/w/kafka"
            :project/provides {"kafka" {:service "kafka" :version "3.7"
                                        :tenancy {:env "TOPIC_PREFIX"}
                                        :migrate {:exec {:service "kafka" :argv ["create-topics.sh"]}}
                                        :seed {:command ["./seed-topics.sh"] :env {"MODE" "test"}}
                                        :reset-strategy :prefix-rotate}}}
   "pg" {:project/id "pg" :project/dir "/w/pg"
         :project/provides {"postgres" {:service "db" :version "16"
                                        :migrate {:exec {:service "db" :argv ["migrate" "up"]} :timeout-ms 60000}
                                        :seed {:exec {:service "db" :argv ["psql" "-f" "/seed.sql"]}}
                                        :reset-strategy :template-db
                                        :template {:database-env "PGDATABASE"}}}}
   "redis" {:project/id "redis" :project/dir "/w/redis"
            :project/provides {"redis" {:service "redis"
                                        :reset {:exec {:service "redis" :argv ["redis-cli" "--scan-delete"]}}
                                        :seed {:command ["./seed-redis.sh"]}}}}
   "shop" {:project/id "shop" :project/dir "/w/shop"
           :project/needs {"kafka" {} "postgres" {} "redis" {}}}
   "billing" {:project/id "billing" :project/dir "/w/billing"
              :project/needs {"kafka" {} "postgres" {}}}
   "audit" {:project/id "audit" :project/dir "/w/audit"
            :project/needs {"kafka" {:share? false} "redis" {:share? false}}}})

(defn- settings [ps] {:compose/projects ps :compose/profiles {}})

(defn- plan-of
  ([consumers] (plan-of projects consumers))
  ([ps consumers]
   (let [res (capability/resolve-stack ps {:stack/id "s" :stack/consumers (vec consumers)})]
     (assert (r/ok? res) (pr-str res))
     (let [sp (stack/stack-plan (settings ps) (:ok res) "run1" {:stack "s"})]
       (assert (r/ok? sp) (pr-str sp))
       (:ok sp)))))

(def all-plan (delay (plan-of ["audit" "billing" "shop"])))

(defn- lp
  ([phase] (lp phase {}))
  ([phase state] (data/lifecycle-plan (settings projects) @all-plan phase state)))

(defn- kinds [steps] (mapv (juxt :step/instance :step/consumer :step/kind) steps))

(deftest before-suite-migrates-then-seeds-each-pair-in-start-order
  (let [res (lp :before-suite)
        steps (get-in res [:ok :steps])]
    (is (r/ok? res) (pr-str res))
    (testing "postgres template: template db, migrate + seed into it, then the run's db from it"
      (is (= [:create-template :migrate :seed :create-database]
             (mapv :step/kind (filter #(and (= "shop" (:step/consumer %)) (= "postgres" (:step/capability %))) steps))))
      (let [[tpl mig _ db] (filter #(and (= "shop" (:step/consumer %)) (= "postgres" (:step/capability %))) steps)]
        (is (= ["psql" "-U" "postgres" "-d" "postgres" "--set=ON_ERROR_STOP=1" "-c"
                "CREATE DATABASE \"run1_shop_tpl\""] (:step/argv tpl)))
        (is (= "run1_shop_tpl" (get-in mig [:step/env "PGDATABASE"])) "migrate targets the template")
        (is (= 60000 (:step/timeout-ms mig)))
        (is (= "CREATE DATABASE \"run1_shop_1\" TEMPLATE \"run1_shop_tpl\"" (last (:step/argv db))))
        (is (= "run1_shop_1" (:step/database db)))
        (is (= "already exists" (:step/tolerate db)))))
    (testing "kafka: migrate in the container, seed on the host, both in the consumer's tenancy"
      (let [[m s] (filter #(and (= "billing" (:step/consumer %)) (= "kafka" (:step/capability %))) steps)]
        (is (= [:exec :migrate] [(:step/target m) (:step/kind m)]))
        (is (= [:host ["./seed-topics.sh"]] [(:step/target s) (:step/argv s)]))
        (is (= "run1.billing." (get-in s [:step/env "TOPIC_PREFIX"])))
        (is (= "test" (get-in s [:step/env "MODE"])) "the command's own env is kept")
        (is (= :tenancy (:step/scope s)))))
    (testing "a private instance is scoped to its instance"
      (is (every? #(= :instance (:step/scope %)) (filter #(= "audit" (:step/consumer %)) steps))))))

(deftest between-suites-resets-per-strategy
  (let [before (:ok (lp :before-suite))
        between (:ok (lp :between-suites (:state before)))
        steps (:steps between)
        of (fn [c cap] (filter #(and (= c (:step/consumer %)) (= cap (:step/capability %))) steps))]
    (testing "template-db: a fresh database from the template, generation 1 -> _2"
      (is (= [:create-database] (mapv :step/kind (of "shop" "postgres"))))
      (is (= "CREATE DATABASE \"run1_shop_2\" TEMPLATE \"run1_shop_tpl\""
             (last (:step/argv (first (of "shop" "postgres")))))))
    (testing "prefix-rotate: next prefix, then migrate + seed in it"
      (is (= [:rotate-prefix :migrate :seed] (mapv :step/kind (of "shop" "kafka"))))
      (is (= "run1.shop.g1." (:step/prefix (first (of "shop" "kafka")))))
      (is (every? #(= "run1.shop.g1." (get-in % [:step/env "TOPIC_PREFIX"])) (of "shop" "kafka"))))
    (testing "command: reset, then seed"
      (is (= [:reset :seed] (mapv :step/kind (of "shop" "redis")))))
    (testing "state advances: a second reset goes one further"
      (let [again (:ok (lp :between-suites (:state between)))]
        (is (= "run1.shop.g2." (some :step/prefix (filter #(= "shop" (:step/consumer %)) (:steps again)))))))
    (testing ":after resets only :command providers"
      (is (= #{:reset} (set (map :step/kind (:steps (:ok (lp :after (:state between)))))))))))

(deftest refusals
  (testing "a step that would remove volumes is refused"
    (let [ps (assoc-in projects ["redis" :project/provides "redis" :reset]
                       {:command ["docker" "compose" "down" "-v"]})]
      (is (= :compose/volume-removal-refused
             (:error (data/lifecycle-plan (settings ps) (plan-of ps ["shop"]) :between-suites))))))
  (testing "an exec into a service the instance does not run"
    (let [ps (assoc-in projects ["redis" :project/provides "redis" :seed]
                       {:exec {:service "nope" :argv ["x"]}})]
      (is (= :compose/unknown-data-service
             (:error (data/lifecycle-plan (settings ps) (plan-of ps ["shop"]) :before-suite))))))
  (testing "a malformed data command"
    (let [ps (assoc-in projects ["redis" :project/provides "redis" :seed] {:command [] :exec {:service "redis" :argv ["x"]}})]
      (is (= :compose/invalid-data-lifecycle
             (:error (data/lifecycle-plan (settings ps) (plan-of ps ["shop"]) :before-suite))))))
  (testing "an unknown phase"
    (is (= :compose/invalid-phase (:error (lp :sometime)))))
  (testing "a provider without lifecycle keys plans nothing"
    (let [ps (update-in projects ["redis" :project/provides "redis"] select-keys [:service])
          steps (get-in (data/lifecycle-plan (settings ps) (plan-of ps ["shop"]) :before-suite) [:ok :steps])]
      (is (not-any? #(= "redis" (:step/capability %)) steps)))))

(deftest volume-detection
  (is (data/removes-volumes? ["docker" "compose" "down" "-v"]))
  (is (data/removes-volumes? ["docker" "compose" "down" "--volumes"]))
  (is (data/removes-volumes? ["docker" "compose" "rm" "-fv"]))
  (is (data/removes-volumes? ["docker" "volume" "rm" "x"]))
  (is (data/removes-volumes? ["docker" "volume" "prune"]))
  (is (not (data/removes-volumes? ["docker" "compose" "down"])))
  (is (not (data/removes-volumes? (data/psql-argv {} "SELECT 1")))))

(deftest prefixes-and-databases
  (is (= (names/tenancy-prefix "run1" "shop") (data/rotated-prefix "run1" "shop" 0)))
  (is (= "run1.shop.g1." (data/next-prefix "run1" "shop" "run1.shop.")))
  (is (= "run1.shop.g3." (data/next-prefix "run1" "shop" "run1.shop.g2.")))
  (is (= "run1.shop.g1." (data/next-prefix "run1" "shop" "someone.else.")))
  (is (= "run1_billing_api" (data/tenant-database "run1" "billing/api")))
  (is (<= (count (data/tenant-database "run1" (apply str (repeat 80 "x")))) 50)))

;; ---------------------------------------------------------------------------
;; properties

(def gen-consumers
  (gen/fmap (comp vec sort) (gen/not-empty (gen/set (gen/elements ["audit" "billing" "shop"])))))

(def gen-phases (gen/vector (gen/elements data/phases) 0 6))

(defn- run-phases
  "[[phase plan] ...] of `phases` run in sequence from {}."
  [sp phases]
  (second (reduce (fn [[state acc] ph]
                    (let [p (:ok (data/lifecycle-plan (settings projects) sp ph state))]
                      [(:state p) (conj acc [ph p])]))
                  [{} []] phases)))

(defspec migrate-precedes-seed-in-every-pair 50
  (prop/for-all [cs gen-consumers phases gen-phases]
    (every? (fn [[_ p]]
              (let [by-pair (group-by (juxt :step/instance :step/consumer) (:steps p))]
                (every? (fn [[_ steps]]
                          (let [ks (mapv :step/kind steps)
                                im (.indexOf ks :migrate) is (.indexOf ks :seed)]
                            (or (neg? im) (neg? is) (< im is))))
                        by-pair)))
            (run-phases (plan-of cs) (into [:before-suite] phases)))))

(defspec the-plan-is-deterministic 50
  (prop/for-all [cs gen-consumers phases gen-phases]
    (let [sp (plan-of cs)]
      (= (run-phases sp phases) (run-phases sp phases)))))

(defspec no-step-removes-a-volume 50
  (prop/for-all [cs gen-consumers phases gen-phases]
    (every? (fn [[_ p]] (not-any? #(data/removes-volumes? (:step/argv %)) (:steps p)))
            (run-phases (plan-of cs) (into [:before-suite] phases)))))

(def gen-evil-argv
  (gen/let [prefix (gen/vector (gen/elements ["docker" "compose" "-p" "x" "down" "rm"]) 0 4)
            evil (gen/elements [["-v"] ["--volumes"] ["--volumes=true"] ["down" "-v"] ["rm" "-fv"]
                                ["volume" "rm" "data"] ["volume" "prune"]])
            suffix (gen/vector (gen/elements ["svc" "--remove-orphans"]) 0 2)]
    (vec (concat prefix evil suffix))))

(defspec any-volume-removing-command-is-refused 50
  (prop/for-all [argv gen-evil-argv phase (gen/elements data/phases)
                 k (gen/elements [:seed :migrate :reset])
                 exec? gen/boolean]
    (let [cmd (if exec? {:exec {:service "redis" :argv argv}} {:command argv})
          ps (assoc-in projects ["redis" :project/provides "redis" k] cmd)]
      (and (data/removes-volumes? argv)
           (= :compose/volume-removal-refused
              (:error (data/lifecycle-plan (settings ps) (plan-of ps ["shop"]) phase)))))))

(defspec a-shared-instance-is-never-reset-globally 50
  (prop/for-all [cs gen-consumers phases gen-phases]
    (let [sp (plan-of cs)
          shared? (fn [iid] (get-in sp [:stack/instances iid :instance/share?]))]
      (every? (fn [[_ p]]
                (every? (fn [s]
                          (or (not (shared? (:step/instance s)))
                              (and (= :tenancy (:step/scope s))
                                   (str/starts-with? (get-in s [:step/env data/tenant-prefix-var])
                                                     (names/tenancy-prefix "run1" (:step/consumer s))))))
                        (:steps p)))
              (run-phases sp (into [:before-suite] phases))))))

(defspec prefix-rotation-is-injective-and-stays-in-the-tenancy 100
  (prop/for-all [c (gen/elements ["shop" "billing/api" "suite:e2e"]) n (gen/choose 0 50)]
    (let [ps (take (inc n) (iterate #(data/next-prefix "run1" c %) (names/tenancy-prefix "run1" c)))]
      (and (apply distinct? ps)
           (every? #(str/starts-with? % (names/tenancy-prefix "run1" c)) ps)
           (= n (data/prefix-generation "run1" c (last ps)))))))
