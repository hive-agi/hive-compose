(ns hive-compose.pipeline.stacks-test
  "Stack runs on the stub engine: up order and health gating, labels, view
   networks, refcounted down across runs and sections, claims and the reaper.
   test.check properties: down never stops an instance another live run binds;
   up order respects bindings; every created network and every container of a
   run-scoped project carries hive.run."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.pipeline.ops :as ops]
            [hive-compose.pipeline.stacks :as stacks]
            [hive-compose.port :as port]
            [hive-compose.stub :as stub]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def minute 60000)

(def projects
  {"kafka" {:project/id "kafka" :project/dir "/w/kafka"
            :project/provides {"kafka" {:service "kafka" :version "3.7"
                                        :env {"KAFKA_BROKERS" "kafka:9092"}
                                        :tenancy {:env "TOPIC_PREFIX"}}}}
   "pg" {:project/id "pg" :project/dir "/w/pg"
         :project/provides {"postgres" {:service "db" :version "16" :env {"PGHOST" "db"}}}}
   "shop" {:project/id "shop" :project/dir "/w/shop" :project/env {"MODE" "dev"}
           :project/needs {"kafka" {:version "^3"} "postgres" {}}}
   "billing" {:project/id "billing" :project/dir "/w/billing"
              :project/needs {"kafka" {:version "^3"}}}
   "audit" {:project/id "audit" :project/dir "/w/audit"
            :project/needs {"kafka" {:version "^3" :share? false}}}})

(def world
  "Compose models of every project: kafka depends on zookeeper."
  {"kafka" {"kafka" {:deps ["zookeeper"]} "zookeeper" {}}
   "pg" {"db" {}}
   "shop" {"api" {:deps ["worker"]} "worker" {}}
   "billing" {"billing" {}}
   "audit" {"audit" {}}})

(defn- settings [stacks]
  {:compose/default-ttl-minutes 60
   :compose/orphan-ttl-minutes 5
   :compose/claim-ttl-minutes 30
   :compose/default-idle-action :stop
   :compose/up-timeout-ms 120000
   :compose/profiles {}
   :compose/projects projects
   :compose/stacks stacks})

(def all-stacks
  {"shop" {:stack/id "shop" :stack/consumers ["shop"]}
   "billing" {:stack/id "billing" :stack/consumers ["billing"]}
   "all" {:stack/id "all" :stack/consumers ["audit" "billing" "shop"]}})

(defn- ctx
  ([] (ctx {}))
  ([running]
   (let [clock (atom 0) saved (atom nil) hex (atom 0)]
     {:engine (stub/engine {} running world)
      :settings (settings all-stacks)
      :state (atom {:active {} :current nil})
      :now #(deref clock)
      :clock clock
      :saved saved
      :rand-hex #(format "%06x" (swap! hex inc))
      :save! #(r/ok (reset! saved %))})))

(defn- e [c] (:engine c))

(defn- ups [c] (filter #(= :up-project (first %)) (stub/calls (e c))))

(deftest a-stack-comes-up-in-binding-order-on-its-view-network
  (let [c (ctx)
        res (stacks/stack-up! c {:stack "shop"})
        run (get-in res [:ok :run])]
    (is (r/ok? res) (pr-str res))
    (is (= "shop000001" run) "<stack><6 hex>, no dash (RunId)")
    (testing "instances before the consumer, each waited"
      (is (= [["kafka" ["kafka"]] ["pg" ["db"]] ["shop-shop000001" ["api" "worker"]]]
             (map (fn [[_ p s]] [p s]) (ups c))))
      (is (every? #(get-in % [3 :wait?]) (ups c))))
    (testing "shared instances run as their project does: no labels, no extra network"
      (is (= {} (select-keys (nth (first (ups c)) 3) [:labels :networks]))))
    (testing "the consumer: labelled, on its view network alone, stack env by name only"
      (let [[_ _ _ opts] (last (ups c))]
        (is (= {"hive.run" run "hive.stack" "shop" "hive.consumer" "shop"} (:labels opts)))
        (is (= ["hive-shop000001-shop"] (:networks opts)))
        (is (= ["HIVE_RUN_ID" "KAFKA_BROKERS" "OTEL_RESOURCE_ATTRIBUTES" "PGHOST" "TOPIC_PREFIX"]
               (:container-env opts))
            "MODE is the consumer's own env, already in its compose file")))
    (testing "instances joined the view network under their service names"
      (is (= {"hive-shop000001-shop" {"kafka-kafka-1" ["kafka"] "pg-db-1" ["db"]
                                      "shop-shop000001-api-1" ["api"] "shop-shop000001-worker-1" ["worker"]}}
             (update-vals (stub/networks (e c)) :members)))
      (is (= {"hive.run" run "hive.stack" "shop"} (get-in (stub/networks (e c)) ["hive-shop000001-shop" :labels]))))
    (testing "persisted additively"
      (is (= #{:active :current :stacks} (set (keys @(:saved c)))))
      (is (= :up (get-in @(:saved c) [:stacks run :status]))))))

(deftest a-consumer-waits-for-a-healthy-instance
  (let [c (ctx)]
    (stub/health! (e c) "pg" "db" :unhealthy)
    (let [res (stacks/stack-up! c {:stack "shop"})]
      (is (= :compose/instance-unhealthy (:error res)))
      (is (= "pg/postgres" (:instance res)) "names the instance")
      (is (string? (:run res)))
      (is (not-any? #(= "shop-shop000001" (second %)) (ups c)) "the consumer never started")
      (is (= :partial (get-in @(:state c) [:stacks (:run res) :status])))
      (testing "the partial run is released by down"
        (is (r/ok? (stacks/stack-down! c {:run (:run res)})))
        (is (empty? (stub/networks (e c))))
        (is (empty? (:stacks @(:state c))))))))

(deftest without-wait-nothing-is-gated
  (let [c (ctx)]
    (stub/health! (e c) "pg" "db" :starting)
    (is (r/ok? (stacks/stack-up! c {:stack "shop" :wait? false})))))

(deftest a-private-instance-is-labelled-and-removed-whole
  (let [c (ctx)
        res (stacks/stack-up! c {:stack "all"})
        run (get-in res [:ok :run])
        private (str "kafka-audit-" run)]
    (is (r/ok? res) (pr-str res))
    (let [[_ _ svcs opts] (first (filter #(= private (second %)) (ups c)))]
      (is (= ["kafka" "zookeeper"] svcs) "its whole closure, so every container is labelled")
      (is (= {"hive.run" run "hive.stack" "all" "hive.instance" "kafka/kafka@audit"}
             (select-keys (:labels opts) ["hive.run" "hive.stack" "hive.instance"]))))
    (is (every? #(= run (get-in % [:labels "hive.run"]))
                (filter #(= private (:project %)) (vals (stub/containers (e c))))))
    (is (r/ok? (stacks/stack-down! c {:stack "all"})))
    (is (some #(= [:down-project private :all] %) (stub/calls (e c))))
    (is (= #{} (stub/running (e c) private)))))

(deftest down-spares-what-another-run-binds
  (let [c (ctx)
        a (get-in (stacks/stack-up! c {:stack "shop"}) [:ok :run])
        b (get-in (stacks/stack-up! c {:stack "billing"}) [:ok :run])
        down (stacks/stack-down! c {:run a})]
    (is (r/ok? down))
    (is (= #{"kafka" "zookeeper"} (stub/running (e c) "kafka")) "billing still binds kafka")
    (is (= #{} (stub/running (e c) "pg")) "nobody else needs postgres")
    (is (= [{:instance "kafka/kafka" :project "kafka" :kept {"kafka" [[b "kafka/kafka"]] "zookeeper" [[b "kafka/kafka"]]}}]
           (get-in down [:ok :runs 0 :kept])))
    (is (= ["hive-shop000001-shop"] (get-in down [:ok :runs 0 :networks-removed])))
    (is (contains? (stub/networks (e c)) (str "hive-" b "-billing")) "billing's view network stays")
    (is (r/ok? (stacks/stack-down! c {:run b})))
    (is (= #{} (stub/running (e c) "kafka")))))

(deftest down-never-removes-what-ran-before
  (let [c (ctx {"kafka" #{"kafka" "zookeeper"}})
        run (get-in (stacks/stack-up! c {:stack "billing"}) [:ok :run])
        down (stacks/stack-down! c {:run run})]
    (is (r/ok? down))
    (is (= #{"kafka" "zookeeper"} (stub/running (e c) "kafka")))
    (is (= {"kafka" [[:preexisting]] "zookeeper" [[:preexisting]]}
           (get-in down [:ok :runs 0 :kept 0 :kept])))))

(deftest down-spares-what-an-active-section-needs
  (let [c (ctx)
        run (get-in (stacks/stack-up! c {:stack "billing"}) [:ok :run])]
    (swap! (:state c) assoc-in [:active "kafka/kafka"]
           {:profile/id "kafka/kafka" :project "kafka" :services ["kafka"] :needs ["kafka" "zookeeper"]
            :started-at 0 :last-touch 0})
    (is (r/ok? (stacks/stack-down! c {:run run})))
    (is (= #{"kafka" "zookeeper"} (stub/running (e c) "kafka")))))

(deftest a-section-down-spares-a-stack-instance
  (let [c (ctx)
        _ (stacks/stack-up! c {:stack "billing"})
        sec (r/bind (ops/subject c {:target "kafka/kafka"}) #(ops/up-profile! c %))]
    (is (r/ok? sec) (pr-str sec))
    (let [down (r/bind (ops/subject c {:target "kafka/kafka"}) #(ops/down-profile! c % :down))]
      (is (r/ok? down) (pr-str down))
      (is (= #{"kafka" "zookeeper"} (stub/running (e c) "kafka")) "the billing run still binds kafka")
      (is (= {"kafka" [[:stack]] "zookeeper" [[:stack]]}
             (select-keys (:kept (:ok down)) ["kafka" "zookeeper"])))
      (is (empty? (:services (:ok down)))))
    (testing "the reaper too"
      (r/bind (ops/subject c {:target "kafka/kafka"}) #(ops/up-profile! c %))
      (swap! (:clock c) + (* 61 minute))
      (ops/reap! c)
      (is (= #{"kafka" "zookeeper"} (stub/running (e c) "kafka"))))))

(deftest down-disconnects-and-removes-the-view-networks
  (let [c (ctx)
        run (get-in (stacks/stack-up! c {:stack "all"}) [:ok :run])
        nets (set (get-in @(:state c) [:stacks run :networks]))
        down (stacks/stack-down! c {:run run})]
    (is (= #{(str "hive-" run "-audit") (str "hive-" run "-billing") (str "hive-" run "-shop")} nets))
    (is (= nets (set (get-in down [:ok :runs 0 :networks-removed]))))
    (is (= (count nets) (count (filter #(= :network-rm (first %)) (stub/calls (e c))))))
    (is (empty? (stub/networks (e c))))
    (is (every? #(empty? (:networks %)) (vals (stub/containers (e c))))
        "a shared instance that stays up is detached from every view network of the run")))

(deftest a-consumer-never-re-ups-what-it-provides
  (let [c (assoc-in (ctx) [:settings :compose/stacks "self"]
                    {:stack/id "self" :stack/consumers ["kafka" "billing"]})
        res (stacks/stack-up! c {:stack "self"})]
    (is (r/ok? res) (pr-str res))
    (is (= 1 (count (filter #(and (= :up-project (first %)) (= "kafka" (second %))) (stub/calls (e c))))))
    (is (empty? (get-in res [:ok :consumers 1 :services])) "kafka's own consumer step starts nothing")))

(deftest adopt-never-takes-a-stack-instance
  (let [c (ctx)]
    (stacks/stack-up! c {:stack "billing"})
    (is (not-any? #(= "kafka/adopted" %) (ops/adopt! c)))
    (is (not (contains? (:active @(:state c)) "kafka/adopted")))))

(deftest a-consumer-whose-up-fails-is-still-taken-down
  (let [c (ctx)]
    (stub/health! (e c) "billing-billing000001" "billing" :unhealthy)
    (let [res (stacks/stack-up! c {:stack "billing"})]
      (is (r/err? res))
      (is (seq (stub/running (e c) "billing-billing000001")) "compose left the container behind")
      (is (r/ok? (stacks/stack-down! c {:run (:run res)})))
      (is (= #{} (stub/running (e c) "billing-billing000001"))))))

(deftest errors-are-results
  (let [c (ctx)]
    (is (= :compose/unknown-stack (:error (stacks/stack-up! c {:stack "nope"}))))
    (is (= :compose/invalid-run (:error (stacks/stack-up! c {:stack "shop" :run "Bad-Id"}))))
    (is (r/ok? (stacks/stack-up! c {:stack "shop" :run "fixed1"})))
    (is (= :compose/run-exists (:error (stacks/stack-up! c {:stack "shop" :run "fixed1"}))))
    (is (= :compose/unknown-run (:error (stacks/stack-down! c {:run "zzz"}))))
    (is (= :compose/missing-param (:error (stacks/stack-down! c {}))))
    (is (= :compose/stacks-unsupported
           (:error (stacks/stack-up! (assoc c :engine (reify port/IComposeEngine)) {:stack "shop"}))))))

(deftest status-and-plan
  (let [c (ctx)
        run (get-in (stacks/stack-up! c {:stack "shop"}) [:ok :run])
        st (stacks/stack-status c {:stack "shop"})]
    (is (r/ok? st))
    (is (= {"kafka" {:state "running" :health :healthy}}
           (get-in st [:ok :stacks 0 :instances "kafka/kafka" :services])))
    (is (= #{"api" "worker"} (set (keys (get-in st [:ok :stacks 0 :consumers "shop" :services])))))
    (let [p (stacks/plan c {:stack "billing"})]
      (is (r/ok? p))
      (is (= :running (get-in p [:ok :diff :instances 0 :state])) "kafka already runs for shop")
      (is (string? (get-in p [:ok :text]))))
    (is (= [] (get-in (stacks/stack-status c {:run "other"}) [:ok :stacks])))
    (is (= run (get-in st [:ok :stacks 0 :run])))))

(deftest claims-and-the-reaper
  (let [c (ctx)
        run (get-in (stacks/stack-up! c {:stack "shop"}) [:ok :run])]
    (is (= run (stacks/claim-run (str "stack:" run))))
    (is (nil? (stacks/claim-run "shop/api")))
    (is (r/ok? (stacks/claim! c run "ling-1" {:note "e2e"})))
    (swap! (:clock c) + (* 20 minute))
    (is (= [] (:reaped (stacks/reap! c))) "claimed")
    (is (r/ok? (stacks/release-claim! c run "ling-1" {})))
    (is (get-in @(:state c) [:stacks run :orphaned-at]))
    (swap! (:clock c) + (* 4 minute))
    (is (= [] (:reaped (stacks/reap! c))))
    (swap! (:clock c) + (* 2 minute))
    (is (= [{:run run :ok? true}] (:reaped (stacks/reap! c))) "orphan TTL")
    (is (empty? (:stacks @(:state c))))))

(deftest release-with-stop-takes-it-down
  (let [c (ctx)
        run (get-in (stacks/stack-up! c {:stack "shop"}) [:ok :run])]
    (stacks/claim! c run "a" {})
    (is (r/ok? (stacks/release-claim! c run "a" {:stop? true})))
    (is (empty? (:stacks @(:state c))))))

;; ---------------------------------------------------------------------------
;; properties

(def gen-op
  (gen/one-of [(gen/tuple (gen/return :up) (gen/elements (keys all-stacks)))
               (gen/tuple (gen/return :down) gen/nat)]))

(defn- live-binds
  "#{[compose-project service]} the instances of the live runs keep alive."
  [c]
  (into #{} (for [[_ ent] (:stacks @(:state c))
                  [_ ip] (get-in ent [:plan :stack/instances])
                  s (concat (:instance/services ip) (:instance/needs ip))]
              [(:instance/compose-project ip) s])))

(defn- run-ops! [c ops]
  (reduce (fn [ok? [op x]]
            (and ok?
                 (case op
                   :up (do (stacks/stack-up! c {:stack x}) true)
                   :down (let [rs (sort (keys (:stacks @(:state c))))]
                           (if (empty? rs)
                             true
                             (do (stacks/stack-down! c {:run (nth rs (mod x (count rs)))})
                                 (every? (fn [[cp s]] (contains? (stub/running (e c) cp) s)) (live-binds c))))))))
          true ops))

(defspec down-never-stops-an-instance-another-live-run-binds 100
  (prop/for-all [ops (gen/vector gen-op 1 8)]
    (run-ops! (ctx) ops)))

(defspec up-order-respects-bindings 100
  (prop/for-all [sid (gen/elements (keys all-stacks))]
    (let [c (ctx)
          res (stacks/stack-up! c {:stack sid})
          order (mapv second (ups c))
          pos (zipmap order (range))
          sp (get-in @(:state c) [:stacks (get-in res [:ok :run]) :plan])]
      (and (r/ok? res)
           (every? (fn [[cid cp]]
                     (let [mine (pos (str (:project/id (get projects (first (clojure.string/split cid #"/")))) "-" (:stack/run sp)))]
                       (every? #(< (pos (get-in sp [:stack/instances (:instance %) :instance/compose-project])) mine)
                               (:consumer/attach cp))))
                   (:stack/consumers sp))))))

(defspec every-created-network-and-run-container-carries-hive-run 100
  (prop/for-all [ops (gen/vector (gen/tuple (gen/return :up) (gen/elements (keys all-stacks))) 1 4)]
    (let [c (ctx)]
      (run-ops! c ops)
      (let [runs (set (keys (:stacks @(:state c))))
            run-scoped? (fn [cp] (some #(clojure.string/ends-with? cp (str "-" %)) runs))]
        (and (every? #(contains? runs (get-in % [:labels "hive.run"])) (vals (stub/networks (e c))))
             (every? #(contains? runs (get-in % [:labels "hive.run"]))
                     (filter #(run-scoped? (:project %)) (vals (stub/containers (e c))))))))))
