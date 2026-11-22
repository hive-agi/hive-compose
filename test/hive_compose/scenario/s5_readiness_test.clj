(ns hive-compose.scenario.s5-readiness-test
  "S5 readiness gating (stub engine, real pipeline)."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.scenario.world :as w]
            [hive-compose.stub :as stub]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- ups [c] (mapv second (filter #(= :up-project (first %)) (stub/calls (w/engine c)))))

(deftest s5-a-dependent-starts-only-after-its-provider-is-healthy
  "CLAIM: a provider with a healthcheck that is still :starting makes
   stack-up stop at that instance (`up --wait` times out): the error names
   the instance, the consumer is never brought up, the run is recorded
   :partial and stack-down releases it. Once the provider reports :healthy a
   new run brings the consumer up, after the instance (call order)."
  (let [c (w/ctx)]
    (stub/health! (w/engine c) "infra" "kafka" :starting)
    (let [up (w/up! c "a" "s5slow")]
      (is (= :compose/instance-unhealthy (:error up)) (pr-str up))
      (is (= "infra/kafka" (:instance up)))
      (is (= ["infra"] (ups c)) "only the instance was asked up, never a-s5slow")
      (is (= :partial (get-in @(:state c) [:stacks "s5slow" :status])))
      (is (r/ok? (w/down! c "s5slow")))
      (is (empty? (stub/networks (w/engine c)))))
    (stub/health! (w/engine c) "infra" "kafka" :healthy)
    (let [up (w/up! c "a" "s5ok")]
      (is (r/ok? up) (pr-str up))
      (is (= ["infra" "infra" "a-s5ok"] (ups c)) "consumer after the healthy instance")
      (is (every? #(get-in % [3 :wait?]) (filter #(= :up-project (first %)) (stub/calls (w/engine c))))
          "every up is a --wait"))))

(deftest s5-an-unhealthy-provider-keeps-the-consumer-down
  "CLAIM: an instance whose healthcheck fails (:unhealthy, not just slow)
   refuses the same way: the error names the instance and the consumer is
   never brought up."
  (let [c (w/ctx)
        e (w/engine c)]
    (stub/health! e "infra" "kafka" :unhealthy)
    (let [up (w/up! c "a" "s5flip")]
      (is (r/err? up))
      (is (= "infra/kafka" (:instance up)))
      (is (not-any? #{"a-s5flip"} (ups c))))))
