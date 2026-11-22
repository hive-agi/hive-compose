(ns hive-compose.scenario.s7-parallel-runs-test
  "S7 parallel runs (stub engine, real pipeline)."
  (:require [clojure.set :as set]
            [clojure.test :refer [deftest is testing]]
            [hive-compose.scenario.world :as w]
            [hive-compose.stub :as stub]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- run-containers [c run]
  (set (for [[n {:keys [labels]}] (stub/containers (w/engine c)) :when (= run (get labels "hive.run"))] n)))

(defn- run-networks [c run]
  (set (for [[n {:keys [labels]}] (stub/networks (w/engine c)) :when (= run (get labels "hive.run"))] n)))

(deftest s7-one-stack-under-two-run-ids-at-once
  "CLAIM: stack abc (A, B sharing kafka, C private) up as run p7one and run
   p7two at the same time: no two runs share a container name, a network or
   a compose project of their own (consumers <project>-<run>, private
   instances <provider>-<consumer>-<run>, view networks hive-<run>-<c>); the
   shared kafka is the provider's one container for both, but no view network
   of one run holds a consumer of the other; each run's private kafka is
   reachable only from its own C; taking p7one down leaves p7two whole."
  (let [c (w/ctx)]
    (is (r/ok? (w/up! c "abc" "p7one")))
    (is (r/ok? (w/up! c "abc" "p7two")))
    (let [c1 (run-containers c "p7one") c2 (run-containers c "p7two")
          n1 (run-networks c "p7one") n2 (run-networks c "p7two")
          nets (stub/networks (w/engine c))]
      (testing "no container or network collision"
        (is (= #{"a-p7one-app-1" "b-p7one-app-1" "c-p7one-app-1" "infra-c-p7one-kafka-1"} c1))
        (is (= #{"a-p7two-app-1" "b-p7two-app-1" "c-p7two-app-1" "infra-c-p7two-kafka-1"} c2))
        (is (empty? (set/intersection c1 c2)))
        (is (= #{"hive-p7one-a" "hive-p7one-b" "hive-p7one-c"} n1))
        (is (empty? (set/intersection n1 n2))))
      (testing "isolated networks: a view network holds only its own run's consumer"
        (doseq [[net {:keys [members]}] nets
                :let [run (get-in nets [net :labels "hive.run"])
                      consumers (remove #{"infra-kafka-1"} (keys members))]]
          (is (every? #(re-find (re-pattern (str "-" run "-")) %) consumers) (str net " " (pr-str members)))))
      (testing "each C reaches its own private kafka"
        (is (= ["infra-c-p7one-kafka-1"] (w/resolve-alias c "hive-p7one-c" "kafka")))
        (is (= ["infra-c-p7two-kafka-1"] (w/resolve-alias c "hive-p7two-c" "kafka"))))
      (testing "one shared kafka serves both runs"
        (is (= ["infra-kafka-1"] (w/resolve-alias c "hive-p7one-a" "kafka") (w/resolve-alias c "hive-p7two-a" "kafka")))))
    (testing "down p7one leaves p7two whole"
      (let [before (run-containers c "p7two")]
        (is (r/ok? (w/down! c "p7one")))
        (is (empty? (run-containers c "p7one")))
        (is (= before (run-containers c "p7two")))
        (is (= #{"kafka"} (stub/running (w/engine c) "infra")) "p7two still binds it")
        (is (= ["infra-kafka-1"] (w/resolve-alias c "hive-p7two-b" "kafka")))))
    (is (r/ok? (w/down! c "p7two")))
    (is (empty? (stub/containers (w/engine c))))))
