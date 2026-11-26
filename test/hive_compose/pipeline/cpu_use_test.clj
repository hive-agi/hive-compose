(ns hive-compose.pipeline.cpu-use-test
  "CPU use of a section: a running service busier than
   :compose/cpu-busy-percent counts as use, opt-in per project with :cpu in
   :project/activity, so a section working with no client connected (a batch
   job, a queue worker) is not reaped at its TTL."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.pipeline.ops :as ops]
            [hive-compose.pipeline.swarm :as swarm]
            [hive-compose.promote.swarm :as sw]
            [hive-compose.stub :as stub]
            [hive-compose.stub-runner :as stub-runner]
            [hive-compose.stub-swarm :as stub-swarm]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def minute 60000)

(def settings
  {:compose/default-ttl-minutes 60
   :compose/default-idle-action :stop
   :compose/claim-ttl-minutes 30
   :compose/orphan-ttl-minutes 10
   :compose/swarm? true
   :compose/notify? true
   :compose/activity #{:nrepl}
   :compose/cpu-busy-percent 5
   :compose/profiles {}
   :compose/projects {"etl" {:project/id "etl" :project/dir "/w/etl"
                             :project/activity #{:nrepl :cpu}}
                      "blog" {:project/id "blog" :project/dir "/w/blog"}}})

(def model
  {"etl" {"worker" {:deps ["db"]} "db" {}}
   "blog" {"ghost" {}}})

(defn- ctx []
  (let [clock (atom 0)]
    {:engine (stub/engine {} {} model)
     :runner (stub-runner/runner)
     :swarm (stub-swarm/swarm [])
     :activity (stub-swarm/activity)
     :settings settings
     :state (atom {:active {} :current nil})
     :now #(deref clock)
     :clock clock
     :save! (fn [_] (r/ok nil))}))

(defn- advance! [c ms] (swap! (:clock c) + ms))
(defn- section [c target] (:ok (ops/subject c {:target target})))

(defn- pass! [c]
  (let [observed (swarm/observe! c)]
    (assoc (ops/reap! c) :observed observed)))

(defn- reaped [pass] (mapv :profile/id (:reaped pass)))

(deftest busy-services-keeps-what-is-above-the-threshold
  (is (= {"worker" 80.0} (sw/busy-services {"worker" 80.0 "db" 1.5} 5 nil)))
  (is (= {} (sw/busy-services {"worker" 5} 5 nil)) "at the threshold is not busy")
  (is (= {"db" 9.0} (sw/busy-services {"worker" 80.0 "db" 9.0} 5 #{"db"}))
      "only the section's own services")
  (is (= {} (sw/busy-services {"worker" nil} 5 nil)) "no reading, not busy"))

(deftest usage-names-the-busy-service-and-its-source
  (is (= {:in-use? true
          :by [{:service "worker" :cpu 80.0 :source :cpu}]
          :sources [:cpu]}
         (sw/usage {:busy {"worker" 80.0}})))
  (is (= {:in-use? false :by [] :sources []} (sw/usage {:busy {}}))))

(deftest a-busy-worker-keeps-an-opted-in-section
  (let [c (ctx)]
    (ops/up-profile! c (section c "etl/worker"))
    (stub/load! (:engine c) "etl" {"worker" 80.0 "db" 0.4})
    (advance! c (* 61 minute))
    (let [pass (pass! c)]
      (is (empty? (reaped pass)) "a worker grinding through a job is use")
      (is (= ["etl/worker"] (get-in pass [:observed :in-use]))))
    (let [[s] (:sections (:ok (swarm/who c)))]
      (is (= [{:service "worker" :cpu 80.0 :source :cpu}] (:used-by s)))
      (is (= [:cpu] (:in-use-via s)) "who names the source that counted")
      (is (= [:cpu :nrepl] (:activity s))))
    (testing "once the job is done the section idles out as before"
      (stub/load! (:engine c) "etl" {"worker" 0.2 "db" 0.4})
      (advance! c (* 61 minute))
      (is (= ["etl/worker"] (reaped (pass! c)))))))

(deftest cpu-does-not-count-for-a-project-that-did-not-opt-in
  (let [c (ctx)]
    (ops/up-profile! c (section c "blog/ghost"))
    (stub/load! (:engine c) "blog" {"ghost" 90.0})
    (advance! c (* 61 minute))
    (is (= ["blog/ghost"] (reaped (pass! c))) "the global default #{:nrepl} holds for blog")))

(deftest the-threshold-is-a-setting
  (let [c (assoc (ctx) :settings (assoc settings :compose/cpu-busy-percent 90))]
    (ops/up-profile! c (section c "etl/worker"))
    (stub/load! (:engine c) "etl" {"worker" 80.0})
    (advance! c (* 61 minute))
    (is (= ["etl/worker"] (reaped (pass! c))) "80% is below a 90% threshold")))

(deftest an-engine-that-cannot-read-load-reaps-as-before
  (let [c (ctx)]
    (ops/up-profile! c (section c "etl/worker"))
    (stub/load! (:engine c) "etl" {"worker" 80.0})
    (stub/fail! (:engine c) :cpu)
    (advance! c (* 61 minute))
    (is (= ["etl/worker"] (reaped (pass! c))) "no reading is no use, and no throw")))
