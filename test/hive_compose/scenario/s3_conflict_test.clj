(ns hive-compose.scenario.s3-conflict-test
  "S3 conflict (stub engine, real pipeline)."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [hive-compose.pipeline.stacks :as stacks]
            [hive-compose.scenario.world :as w]
            [hive-compose.stub :as stub]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(deftest s3-incompatible-range-is-refused-before-anything-starts
  "CLAIM: D needs kafka ^2 while only 3.7.1 is provided, in a stack with A
   and B (^3). The plan is an error naming D and its range ^2 (and the
   provider version available); stack-up is the same error and the engine
   saw NO mutating call: no network, no container, no state entry (docker ps
   unchanged)."
  (let [c (w/ctx {:running {"other" #{"x"}}})
        before (stub/containers (w/engine c))
        p (stacks/plan c {:stack "abd" :run "s3run"})
        up (w/up! c "abd" "s3run")]
    (testing "plan names D and the range"
      (is (r/err? p))
      (is (= :compose/incompatible-version (:error p)))
      (is (= "kafka" (:capability p)))
      (let [es (:errors p)]
        (is (some #(and (some #{"d"} (:consumers %)) (= "^2" (get-in % [:ranges "d"]))) es) (pr-str es))
        (is (some #(str/includes? (:message %) "d") es))
        (is (some #(str/includes? (:message %) "^2") es))))
    (testing "up refuses the same way and starts nothing"
      (is (= :compose/incompatible-version (:error up)))
      (is (empty? (w/mutating-calls c)) (pr-str (w/mutating-calls c)))
      (is (= before (stub/containers (w/engine c))))
      (is (empty? (stub/networks (w/engine c))))
      (is (empty? (:stacks @(:state c))) "no run recorded"))))

(deftest s3-mixed-ranges-in-one-sharing-unit-name-every-sharer
  "CLAIM: when no single provider satisfies every sharer, the error names
   the misfit consumers only (D), not those the provider fits (A, B), so the
   message points at the consumer to fix."
  (let [p (stacks/plan (w/ctx) {:stack "abd" :run "s3run"})]
    (is (= ["d"] (:consumers p)) (pr-str (select-keys p [:consumers :ranges :message])))))
