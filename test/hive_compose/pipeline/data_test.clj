(ns hive-compose.pipeline.data-test
  "run-phase! on the stub engine and stub runner (no with-redefs): a stack
   run brought up by stack-up!, then its data lifecycle phases. Exec steps
   land in the instance's container, host steps in the provider's dir, the
   data state advances only on success, and no call removes a volume."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.pipeline.data :as pdata]
            [hive-compose.pipeline.stacks :as stacks]
            [hive-compose.promote.data :as data]
            [hive-compose.stub :as stub]
            [hive-compose.stub-runner :as stub-runner]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def projects
  {"kafka" {:project/id "kafka" :project/dir "/w/kafka"
            :project/provides {"kafka" {:service "kafka" :version "3.7"
                                        :tenancy {:env "TOPIC_PREFIX"}
                                        :migrate {:exec {:service "kafka" :argv ["create-topics.sh"]}}
                                        :seed {:command ["./seed-topics.sh"]}
                                        :reset-strategy :prefix-rotate}}}
   "pg" {:project/id "pg" :project/dir "/w/pg"
         :project/provides {"postgres" {:service "db" :version "16"
                                        :migrate {:exec {:service "db" :argv ["migrate" "up"]}}
                                        :reset-strategy :template-db}}}
   "shop" {:project/id "shop" :project/dir "/w/shop"
           :project/needs {"kafka" {} "postgres" {}}}
   "billing" {:project/id "billing" :project/dir "/w/billing"
              :project/needs {"kafka" {}}}})

(def world
  {"kafka" {"kafka" {:deps ["zookeeper"]} "zookeeper" {}}
   "pg" {"db" {}}
   "shop" {"api" {}}
   "billing" {"billing" {}}})

(defn- ctx []
  (let [hex (atom 0) saved (atom nil)]
    {:engine (stub/engine {} {} world)
     :runner (stub-runner/runner)
     :settings {:compose/default-ttl-minutes 60 :compose/profiles {}
                :compose/up-timeout-ms 120000
                :compose/projects projects
                :compose/stacks {"all" {:stack/id "all" :stack/consumers ["billing" "shop"]}}}
     :state (atom {:active {} :current nil})
     :now (constantly 0)
     :saved saved
     :rand-hex #(format "%06x" (swap! hex inc))
     :save! #(r/ok (reset! saved %))}))

(defn- up! [c]
  (let [res (stacks/stack-up! c {:stack "all"})]
    (assert (r/ok? res) (pr-str res))
    (get-in res [:ok :run])))

(deftest before-suite-runs-in-the-containers-and-on-the-host
  (let [c (ctx) run (up! c)
        res (pdata/run-phase! c run :before-suite)
        execs (stub/execs (:engine c))
        runs (stub-runner/runs (:runner c))]
    (is (r/ok? res) (pr-str res))
    (is (every? :ok? (get-in res [:ok :steps])))
    (testing "kafka migrate execs into the shared kafka container once per consumer, each in its tenancy"
      (let [ks (filter #(= ["kafka" "kafka" ["create-topics.sh"]] (subvec % 1 4)) execs)]
        (is (= #{(str run ".billing.") (str run ".shop.")} (set (map #(get-in % [4 "TOPIC_PREFIX"]) ks))))))
    (testing "kafka seed runs on the host in the provider's dir"
      (is (= 2 (count runs)))
      (is (every? #(= "/w/kafka" (get-in % [2 :dir])) runs)))
    (testing "postgres: template, migrate, database from the template"
      (let [pg (filter #(= "pg" (second %)) execs)]
        (is (= 3 (count pg)))
        (is (= (str "CREATE DATABASE \"" run "_shop_1\" TEMPLATE \"" run "_shop_tpl\"")
               (last (nth (last pg) 3))))))
    (testing "the data state is recorded on the run and persisted"
      (is (= (get-in res [:ok :state]) (pdata/data-state c run)))
      (is (= (pdata/data-state c run) (get-in @(:saved c) [:stacks run :data]))))
    (testing "no engine call removes a volume"
      (is (not-any? #(and (vector? (get % 3)) (data/removes-volumes? (get % 3))) (stub/calls (:engine c)))))))

(deftest between-suites-rotates-and-recreates-and-advances-state
  (let [c (ctx) run (up! c)
        _ (pdata/run-phase! c run :before-suite)
        res (pdata/run-phase! c run :between-suites)]
    (is (r/ok? res) (pr-str res))
    (is (= (str run ".shop.g1.") (pdata/current-prefix c run "kafka/kafka" "shop")))
    (is (some #(= (str run "_shop_2") (:database %)) (get-in res [:ok :steps])))
    (testing "a second reset moves one further"
      (pdata/run-phase! c run :between-suites)
      (is (= (str run ".shop.g2.") (pdata/current-prefix c run "kafka/kafka" "shop"))))))

(deftest a-failed-step-leaves-the-state-and-an-existing-template-is-tolerated
  (testing "already exists is tolerated"
    (let [c (ctx) run (up! c)]
      (stub/exec-fails! (:engine c) "_tpl\"" "ERROR:  database \"x_tpl\" already exists")
      (let [res (pdata/run-phase! c run :before-suite)]
        (is (r/ok? res) (pr-str res))
        (is (some :tolerated (get-in res [:ok :steps]))))))
  (testing "any other failure stops the phase and keeps the state"
    (let [c (ctx) run (up! c)]
      (pdata/run-phase! c run :before-suite)
      (let [before (pdata/data-state c run)]
        (stub-runner/fail! (:runner c) :run)
        (let [res (pdata/run-phase! c run :between-suites)]
          (is (= :compose/data-step-failed (:error res)))
          (is (false? (:ok? (last (:steps res)))))
          (is (= before (pdata/data-state c run))))))))

(deftest unknown-runs-and-missing-ports
  (let [c (ctx)]
    (is (= :compose/unknown-run (:error (pdata/run-phase! c "nope123" :before-suite))))
    (let [run (up! c)
          c' (dissoc c :runner)
          res (pdata/run-phase! c' run :before-suite)]
      (is (= :compose/data-step-failed (:error res)))
      (is (= :compose/data-runner-unsupported (get-in (last (:steps res)) [:error :error]))))))

(deftest a-stack-without-lifecycle-is-a-no-op
  (let [c (update-in (ctx) [:settings :compose/projects]
                     (fn [ps] (-> ps
                                  (update-in ["kafka" :project/provides "kafka"] select-keys [:service :version :tenancy])
                                  (update-in ["pg" :project/provides "postgres"] select-keys [:service :version]))))
        run (up! c)
        res (pdata/run-phase! c run :between-suites)]
    (is (= [] (get-in res [:ok :steps])))
    (is (empty? (stub/execs (:engine c))))))
