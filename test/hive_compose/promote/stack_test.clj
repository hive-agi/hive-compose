(ns hive-compose.promote.stack-test
  "Stack plan (hc-4): compose projects, view networks, start order, plan diff,
   lockfile and the refcounted release of instances across stacks."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.boundary.files :as files]
            [hive-compose.promote.stack :as stack]
            [hive-compose.schema :as schema]
            [hive-dsl.result :as r]
            [malli.core :as m])
  (:import (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)))

;; SPDX-License-Identifier: MIT

(def settings
  {:compose/projects {"infra" {:project/id "infra" :project/dir "/w/Infra.Stack"}
                      "pg" {:project/id "pg" :project/dir "/w/pg" :project/name "pgsql"}
                      "shop" {:project/id "shop" :project/dir "/w/shop"}
                      "billing" {:project/id "billing" :project/dir "/w/billing"}
                      "legacy" {:project/id "legacy" :project/dir "/w/legacy"}}})

(deftest malformed-resolutions-are-results-never-exceptions
  (testing "the input is checked before the private-project fill, so a malformed one is a Result (hc4-rereviewer MAJOR)"
    (doseq [bad [{:resolution/instances {"infra/kafka" 42} :resolution/bindings {}}
                 {:resolution/instances {"infra/kafka" {:instance/id "infra/kafka" :instance/share? false
                                                        :instance/consumers 7}}
                  :resolution/bindings {}}
                 {:resolution/instances [] :resolution/bindings {}}
                 nil]]
      (is (= :compose/invalid-resolution (:error (stack/stack-plan settings bad "r01" {}))) (pr-str bad)))))

(defn- inst [provider cap consumers share? & {:as extra}]
  (merge {:instance/id (if share? (str provider "/" cap) (str provider "/" cap "@" (first consumers)))
          :instance/capability cap :instance/provider provider :instance/service cap
          :instance/share? share? :instance/consumers (vec consumers) :instance/aliases [cap]}
         extra))

(defn- resolution [instances bindings]
  {:resolution/instances (into {} (map (juxt :instance/id identity)) instances)
   :resolution/bindings bindings})

(def kafka (inst "infra" "kafka" ["billing" "shop"] true :instance/version "3.7"
                 :instance/aliases ["kafka" "broker"]))
(def kafka-legacy (inst "infra" "kafka" ["legacy"] false))
(def pg (inst "pg" "postgres" ["infra" "shop"] true))

(def res
  (resolution [kafka kafka-legacy pg]
              {"shop" {"kafka" "infra/kafka" "postgres" "pg/postgres"}
               "billing" {"kafka" "infra/kafka"}
               "legacy" {"kafka" "infra/kafka@legacy"}
               "infra" {"postgres" "pg/postgres"}}))

(deftest plan-of-a-shared-and-an-opted-out-kafka
  (let [sp (:ok (stack/stack-plan settings res "r01" {:stack "checkout"}))]
    (is (some? sp))
    (is (m/validate schema/StackPlan sp) (pr-str (schema/explain schema/StackPlan sp)))
    (testing "compose projects: the provider's own name when shared, a private name otherwise"
      (is (= "infrastack" (get-in sp [:stack/instances "infra/kafka" :instance/compose-project])))
      (is (= "pgsql" (get-in sp [:stack/instances "pg/postgres" :instance/compose-project])))
      (is (= "infrastack-legacy-r01" (get-in sp [:stack/instances "infra/kafka@legacy" :instance/compose-project]))))
    (testing "services and labels"
      (is (= ["kafka"] (get-in sp [:stack/instances "infra/kafka" :instance/services])))
      (is (= {"hive.instance" "infra/kafka"} (get-in sp [:stack/instances "infra/kafka" :instance/labels]))
          "a shared instance carries no per-run label (its config hash must not change per run)")
      (is (= {"hive.run" "r01" "hive.stack" "checkout" "hive.instance" "infra/kafka@legacy" "hive.consumer" "legacy"}
             (get-in sp [:stack/instances "infra/kafka@legacy" :instance/labels]))))
    (testing "the shared kafka joins every consumer's view network, the private one only legacy's; both join infra's own, where infra's postgres answers"
      (is (= ["hive-r01-billing" "hive-r01-infra" "hive-r01-shop"] (get-in sp [:stack/instances "infra/kafka" :instance/networks])))
      (is (= ["hive-r01-infra" "hive-r01-legacy"] (get-in sp [:stack/instances "infra/kafka@legacy" :instance/networks])))
      (is (some #{"hive-r01-infra"} (get-in sp [:stack/instances "pg/postgres" :instance/networks])))
      (is (= ["infra/kafka" "infra/kafka@legacy"] (get-in sp [:stack/consumers "infra" :consumer/provider-of]))))
    (testing "a consumer attaches to its own view network, instances join under their service names"
      (is (= {:consumer/id "shop" :consumer/network "hive-r01-shop"
              :consumer/labels {"hive.run" "r01" "hive.stack" "checkout"}
              :consumer/attach [{:instance "infra/kafka" :aliases ["kafka" "broker"]}
                                {:instance "pg/postgres" :aliases ["postgres"]}]
              :consumer/bindings {"kafka" "infra/kafka" "postgres" "pg/postgres"}
              :consumer/provider-of []}
             (get-in sp [:stack/consumers "shop"]))))
    (testing "start order: postgres before the kafka of infra, which binds it; consumers last"
      (is (= [[:instance "pg/postgres"] [:instance "infra/kafka"] [:instance "infra/kafka@legacy"]
              [:consumer "billing"] [:consumer "infra"] [:consumer "legacy"] [:consumer "shop"]]
             (mapv (juxt :step/kind :step/id) (:stack/start-order sp))))
      (is (= ["pg/postgres"] (get-in sp [:stack/instances "infra/kafka" :instance/depends-on]))))))

(deftest consumer-ids-are-slugged
  (let [r (resolution [(inst "infra" "kafka" ["billing/API"] false)]
                      {"billing/API" {"kafka" "infra/kafka@billing/API"}})
        sp (:ok (stack/stack-plan settings r "r01" {}))]
    (is (= "default" (:stack/id sp)))
    (is (= "infrastack-billing-api-r01" (get-in sp [:stack/instances "infra/kafka@billing/API" :instance/compose-project])))
    (is (= ["hive-r01-billing-api"] (:stack/networks sp)))))

(deftest refusals
  (testing "a bad run id"
    (is (= :compose/invalid-run (:error (stack/stack-plan settings res "R!" {})))))
  (testing "a binding to an instance that does not exist"
    (is (= :compose/inconsistent-resolution
           (:error (stack/stack-plan settings (assoc-in res [:resolution/bindings "x" "redis"] "infra/redis") "r01" {})))))
  (testing "a private instance bound by another consumer"
    (let [e (stack/stack-plan settings (assoc-in res [:resolution/bindings "shop" "kafka"] "infra/kafka@legacy") "r01" {})]
      (is (= :compose/inconsistent-resolution (:error e)))
      (is (= [{:consumer "shop" :capability "kafka" :instance "infra/kafka@legacy" :problem :private-instance-shared}]
             (:problems e)))))
  (testing "a provider that is not a configured project"
    (is (= :compose/unknown-provider
           (:error (stack/stack-plan settings (resolution [(inst "ghost" "redis" ["shop"] true)]
                                                          {"shop" {"redis" "ghost/redis"}})
                                     "r01" {})))))
  (testing "two consumers slugging to one view network"
    (is (= :compose/name-collision
           (:error (stack/stack-plan settings (resolution [kafka] {"a/b" {"kafka" "infra/kafka"}
                                                                   "a:b" {"kafka" "infra/kafka"}})
                                     "r01" {})))))
  (testing "two instances answering one alias on a consumer's network"
    (is (= :compose/alias-clash
           (:error (stack/stack-plan settings
                                     (resolution [kafka (inst "pg" "events" ["shop"] true :instance/aliases ["kafka"])]
                                                 {"shop" {"kafka" "infra/kafka" "events" "pg/events"}})
                                     "r01" {})))))
  (testing "an instance alias equal to one of the consumer's own services"
    (let [e (stack/stack-plan settings res "r01" {:services {"shop" ["web" "postgres"]}})]
      (is (= :compose/alias-shadows-service (:error e)))
      (is (= [{:consumer "shop" :alias "postgres" :instance "pg/postgres"}] (:shadows e)))))
  (testing "a private compose project the resolver set for another run is recomputed"
    (is (= "infrastack-legacy-r01"
           (get-in (:ok (stack/stack-plan settings
                                          (assoc-in res [:resolution/instances "infra/kafka@legacy" :instance/compose-project]
                                                    "infra-legacy-old")
                                          "r01" {}))
                   [:stack/instances "infra/kafka@legacy" :instance/compose-project]))))
  (testing "an instance nobody binds"
    (is (= [{:instance "infra/kafka@legacy" :problem :unbound-instance}]
           (:problems (stack/stack-plan settings (update res :resolution/bindings dissoc "legacy") "r01" {})))))
  (testing "a run-free private instance (as the resolver answers it) is planned, its project filled"
    (let [sp (:ok (stack/stack-plan settings res "r01" {}))]
      (is (nil? (:instance/compose-project kafka-legacy)) "the fixture carries no compose project")
      (is (= "infrastack-legacy-r01" (get-in sp [:stack/instances "infra/kafka@legacy" :instance/compose-project])))
      (is (m/validate schema/Instance (get-in res [:resolution/instances "pg/postgres"])))))
  (testing "one consumer's two private instances of one provider share its private project"
    (let [r (resolution [(inst "infra" "kafka" ["shop"] false) (inst "infra" "registry" ["shop"] false)]
                        {"shop" {"kafka" "infra/kafka@shop" "registry" "infra/registry@shop"}})
          sp (:ok (stack/stack-plan settings r "r01" {}))]
      (is (= #{"infrastack-shop-r01"} (set (map :instance/compose-project (vals (:stack/instances sp))))))))
  (testing "providers binding each other"
    (let [e (stack/stack-plan settings
                              (resolution [(inst "infra" "kafka" ["pg"] true) (inst "pg" "postgres" ["infra"] true)]
                                          {"pg" {"kafka" "infra/kafka"} "infra" {"postgres" "pg/postgres"}})
                              "r01" {})]
      (is (= :compose/dependency-cycle (:error e)))
      (is (= ["infra/kafka" "pg/postgres"] (:instances e))))))

(deftest plan-diff-and-its-rendering
  (let [sp (stack/with-needs (:ok (stack/stack-plan settings res "r01" {:stack "checkout"}))
                             {["infrastack" "kafka"] ["kafka" "zookeeper"]})
        d (stack/plan-diff sp #{{:project "pgsql" :service "postgres"}
                                {:project "infrastack" :service "kafka"}})]
    (is (= {:instances 3 :shared 2 :private 1 :running 1 :partial 1 :to-start 1} (:summary d)))
    (is (= [["pg/postgres" :shared :running] ["infra/kafka" :shared :partial] ["infra/kafka@legacy" :private :to-start]]
           (mapv (juxt :instance :mode :state) (:instances d))))
    (is (= ["zookeeper"] (:to-start (second (:instances d)))))
    (let [text (stack/render-diff d)]
      (is (str/starts-with? text "Stack checkout, run r01: 3 instances (2 shared, 1 private), 1 running, 1 partial, 1 to start."))
      (is (str/includes? text "= pg/postgres"))
      (is (str/includes? text "+ infra/kafka@legacy"))
      (is (str/includes? text "~ infra/kafka"))
      (is (str/includes? text "shop -> hive-r01-shop <- infra/kafka as kafka,broker, pg/postgres as postgres")))))

(defn- temp-dir []
  (str (Files/createTempDirectory "hive-compose-lock" (make-array FileAttribute 0))))

(deftest lockfile
  (let [sp (:ok (stack/stack-plan settings res "r01" {:stack "checkout"}))
        l (stack/lock sp {["infrastack" "kafka"] {:image "apache/kafka:3.7.0" :digest "sha256:aa" :id "x"}
                          ["pgsql" "postgres"] {:image "postgres:16" :digest "sha256:bb"}
                          ["infrastack-legacy-r01" "kafka"] {:image "apache/kafka:3.7.0"}})]
    (is (= {"infra/kafka" {:image "apache/kafka:3.7.0" :digest "sha256:aa"}
            "pg/postgres" {:image "postgres:16" :digest "sha256:bb"}}
           l))
    (is (m/validate schema/Lock l))
    (is (= "/s/hive-compose/locks/checkout.lock.edn" (stack/lock-path "/s/hive-compose/state.edn" "checkout")))
    (testing "the boundary writes and reads it back"
      (let [path (stack/lock-path (str (temp-dir) "/state.edn") "checkout")]
        (is (= (r/ok nil) (files/read-lock path)) "no lockfile yet")
        (is (r/ok? (files/write-lock! path l)))
        (is (= l (:ok (files/read-lock path))))))))

(deftest release-spares-what-another-stack-binds
  (let [a (stack/with-needs (:ok (stack/stack-plan settings res "r01" {:stack "checkout"}))
                            {["infrastack" "kafka"] ["kafka" "zookeeper"]})
        b (:ok (stack/stack-plan settings (resolution [(inst "infra" "kafka" ["billing"] true)]
                                                      {"billing" {"kafka" "infra/kafka"}})
                                 "r02" {:stack "billing"}))
        live {"r01" a "r02" b}]
    (is (= {"infrastack" #{"kafka" "zookeeper"} "infrastack-legacy-r01" #{"kafka"} "pgsql" #{"postgres"}}
           (stack/stack-held {"r01" a})))
    (testing "r02 still binds the shared kafka: only its zookeeper, which r02 does not hold, goes"
      (is (= {:run "r01" :instance "infra/kafka" :project "infrastack" :services ["zookeeper"]
              :kept {"kafka" [["r02" "infra/kafka"]]}}
             (stack/instance-release live "r01" "infra/kafka"))))
    (testing "an active section of the provider project keeps its services too"
      (is (= {"zookeeper" [[:section "infra/broker"]] "kafka" [["r02" "infra/kafka"]]}
             (:kept (stack/instance-release live "r01" "infra/kafka"
                                            {"infra/broker" {:profile/id "infra/broker" :project "infrastack"
                                                             :services ["zookeeper"] :started-at 0 :last-touch 0}})))))
    (testing "the private kafka is released whole"
      (is (= ["kafka"] (:services (stack/instance-release live "r01" "infra/kafka@legacy")))))
    (testing "once r02 is gone, r01 releases the kafka"
      (is (= ["kafka" "zookeeper"] (:services (stack/instance-release {"r01" a} "r01" "infra/kafka")))))
    (testing "stack-releases goes in reverse start order"
      (is (= ["infra/kafka@legacy" "infra/kafka" "pg/postgres"] (mapv :instance (stack/stack-releases live "r01")))))
    (is (nil? (stack/instance-release live "r01" "nope")))))

;; ---------------------------------------------------------------------------
;; properties

(def providers ["p0" "p1" "p2" "p3"])

(def gen-consumer
  (gen/elements ["app" "web" "Billing/API" "suite:smoke" "svc.x" "worker_2"]))

(def gen-world
  "A Resolution as the resolver would answer it, built directly: provider pi
   offers capability ci; pi may need cj only for j < i (so there is no cycle);
   apps need any capability, shared or opted out."
  (gen/let [provider-needs (gen/vector (gen/vector gen/boolean 4) 4)
            apps (gen/fmap (comp vec distinct) (gen/vector gen-consumer 1 4))
            app-needs (gen/vector (gen/vector (gen/elements [nil :shared :private]) 4) (count apps))
            run (gen/elements ["r01" "run7" "abc"])]
    (let [needs (concat
                 (for [i (range 4) j (range i) :when (get-in provider-needs [i j])]
                   [(providers i) j :shared])
                 (for [[a k] (map vector apps (range)) j (range 4)
                       :let [mode (get-in app-needs [k j])] :when mode]
                   [a j mode]))
          inst-of (fn [consumer j mode]
                    (if (= mode :shared)
                      (str "p" j "/c" j)
                      (str "p" j "/c" j "@" consumer)))
          instances (reduce (fn [acc [consumer j mode]]
                              (let [id (inst-of consumer j mode)]
                                (update acc id
                                        (fn [i]
                                          (if i
                                            (update i :instance/consumers conj consumer)
                                            {:instance/id id :instance/capability (str "c" j)
                                             :instance/provider (str "p" j) :instance/service (str "c" j)
                                             :instance/share? (= mode :shared)
                                             :instance/consumers [consumer]
                                             :instance/aliases [(str "c" j)]})))))
                            {} needs)
          bindings (reduce (fn [acc [consumer j mode]]
                             (assoc-in acc [consumer (str "c" j)] (inst-of consumer j mode)))
                           {} needs)]
      {:run run
       :settings {:compose/projects (into {} (map (fn [p] [p {:project/id p :project/dir (str "/w/" p)}])) providers)}
       :resolution {:resolution/instances instances :resolution/bindings bindings}})))

(defn- plan-of
  "The StackPlan of world `w`; throws when it is refused, so no property can
   pass vacuously on an error Result."
  [{:keys [settings resolution run]}]
  (let [res (stack/stack-plan settings resolution run {})]
    (when (r/err? res) (throw (ex-info "plan refused" res)))
    res))

(defn- position [sp]
  (into {} (map-indexed (fn [i {:step/keys [kind id]}] [[kind id] i])) (:stack/start-order sp)))

(defspec start-order-respects-bindings 200
  (prop/for-all [w gen-world]
    (let [sp (:ok (plan-of w))
          pos (position sp)
          last-instance (reduce max -1 (keep (fn [[[k _] i]] (when (= :instance k) i)) pos))]
      (and (m/validate schema/StackPlan sp)
           (= (count (:stack/start-order sp))
              (+ (count (:stack/instances sp)) (count (:stack/consumers sp))))
           (every? (fn [[iid ip]]
                     (every? #(< (pos [:instance %]) (pos [:instance iid])) (:instance/depends-on ip)))
                   (:stack/instances sp))
           (every? (fn [[c caps]]
                     (and (< last-instance (pos [:consumer c]))
                          (every? (fn [iid]
                                    ;; a provider project binding an instance: every instance
                                    ;; of that provider starts after it
                                    (every? (fn [[jid jp]]
                                              (or (not= c (:instance/provider jp)) (= jid iid)
                                                  (< (pos [:instance iid]) (pos [:instance jid]))))
                                            (:stack/instances sp)))
                                  (vals caps))))
                   (get-in w [:resolution :resolution/bindings]))))))

(defspec shared-instance-appears-once-with-every-consumer-network 200
  (prop/for-all [w gen-world]
    (let [sp (:ok (plan-of w))
          run (:run w)
          bindings (get-in w [:resolution :resolution/bindings])]
      (every? (fn [[iid ip]]
                (let [binders (sort (for [[c caps] bindings :when (some #{iid} (vals caps))] c))
                      nets (mapv #(get-in sp [:stack/consumers % :consumer/network]) binders)]
                  (and (= 1 (count (filter #(= iid (:step/id %)) (:stack/start-order sp))))
                       (every? (set (:instance/networks ip)) nets)
                       (every? (fn [c] (some #(= iid (:instance %)) (get-in sp [:stack/consumers c :consumer/attach])))
                               binders))))
              (:stack/instances sp)))))

(defspec an-opted-out-consumer-never-shares-a-network-with-the-shared-instance 200
  (prop/for-all [w gen-world]
    (let [sp (:ok (plan-of w))
          instances (vals (:stack/instances sp))
          networks (map :consumer/network (vals (:stack/consumers sp)))]
      (and (= (count networks) (count (distinct networks)))
           (every? (fn [priv]
                     (let [c (first (:instance/consumers priv))
                           net (get-in sp [:stack/consumers c :consumer/network])]
                       (every? (fn [shared]
                                 (not (some #{net} (:instance/networks shared))))
                               (filter #(and (:instance/share? %)
                                             (= (:instance/capability %) (:instance/capability priv))
                                             (some (set (:instance/aliases priv)) (:instance/aliases %)))
                                       instances))))
                   (remove :instance/share? instances))))))

(defspec private-compose-projects-are-valid-names 200
  (prop/for-all [w gen-world]
    (let [sp (:ok (plan-of w))]
      (every? #(re-matches #"^[a-z0-9][a-z0-9_-]*$" (:instance/compose-project %))
              (remove :instance/share? (vals (:stack/instances sp)))))))

(defspec slug-collisions-and-dns-shadowing-are-refused 200
  (prop/for-all [w gen-world
                 extra (gen/elements ["APP" "app/" "web:" "Svc.X"])
                 own (gen/vector (gen/elements ["c0" "c1" "c2" "c3" "api"]) 0 3)]
    (let [bindings (get-in w [:resolution :resolution/bindings])
          [c caps] (first (sort-by key bindings))]
      (and
       ;; a second consumer slugging to an existing consumer's name
       (let [twin (some (fn [b] (when (and (not= b extra) (= (schema/slug b) (schema/slug extra))) b)) (keys bindings))
             [cap shared] (when twin
                            (some (fn [[cap iid]]
                                    (when (get-in w [:resolution :resolution/instances iid :instance/share?]) [cap iid]))
                                  (get bindings twin)))]
         (or (nil? shared)
             (= :compose/name-collision
                (:error (stack/stack-plan (:settings w)
                                          (-> (:resolution w)
                                              (assoc-in [:resolution/bindings extra] {cap shared})
                                              (update-in [:resolution/instances shared :instance/consumers] conj extra))
                                          (:run w) {})))))
       ;; an alias equal to one of the consumer's own services
       (let [aliases (set (mapcat #(get-in w [:resolution :resolution/instances % :instance/aliases]) (vals caps)))
             res (stack/stack-plan (:settings w) (:resolution w) (:run w) {:services {c own}})]
         (if (and c (some aliases own))
           (= :compose/alias-shadows-service (:error res))
           (r/ok? res)))))))

(defspec release-spares-instances-bound-by-another-stack 200
  (prop/for-all [w1 gen-world
                 w2 gen-world]
    (let [live {"runa" (:ok (plan-of (assoc w1 :run "runa")))
                "runb" (:ok (plan-of (assoc w2 :run "runb")))}]
      (every? (fn [[run sp]]
                (every? (fn [iid]
                          (let [step (stack/instance-release live run iid)
                                cp (:project step)
                                others (stack/holders live run iid)]
                            (and (not-any? #(contains? others [cp %]) (:services step))
                                 (= (set (map second (stack/instance-footprint (get-in sp [:stack/instances iid]))))
                                    (into (set (:services step)) (keys (:kept step)))))))
                        (keys (:stack/instances sp))))
              live))))

(defspec taking-every-stack-down-releases-every-service-once 100
  (prop/for-all [w1 gen-world
                 w2 gen-world]
    (let [live {"runa" (:ok (plan-of (assoc w1 :run "runa")))
                "runb" (:ok (plan-of (assoc w2 :run "runb")))}
          steps (concat (stack/stack-releases live "runa")
                        (stack/stack-releases (dissoc live "runa") "runb"))
          released (frequencies (mapcat (fn [s] (map #(vector (:project s) %) (:services s))) steps))
          held (set (mapcat (fn [[_ sp]] (mapcat stack/instance-footprint (vals (:stack/instances sp)))) live))]
      (and (= held (set (keys released)))
           (every? #(= 1 %) (vals released))))))
