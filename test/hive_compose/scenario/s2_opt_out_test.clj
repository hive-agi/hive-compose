(ns hive-compose.scenario.s2-opt-out-test
  "S2 opt-out (stub engine, real pipeline)."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.pipeline.stacks :as stacks]
            [hive-compose.scenario.world :as w]
            [hive-compose.stub :as stub]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(deftest s2-share-false-gets-a-private-kafka
  "CLAIM: C needs kafka ^3 with :share? false, beside A and B sharing it. The
   plan shows two instances, infra/kafka :shared and infra/kafka@c :private;
   the private one runs under compose project <provider>-<consumer>-<run>
   (infra-c-s2run), labelled hive.run/hive.stack/hive.instance. On C's view
   network `kafka` resolves to the PRIVATE container only, on A's and B's to
   the shared one only (no container is on both), and taking A and B's
   shared kafka into account C's messages never reach it."
  (let [c (w/ctx)
        run "s2run"
        p (stacks/plan c {:stack "abc" :run run})]
    (testing "plan"
      (is (= {:instances 2 :shared 1 :private 1 :running 0 :partial 0 :to-start 2}
             (get-in p [:ok :diff :summary])))
      (is (= {"infra/kafka" [:shared "infra" ["a" "b"]]
              "infra/kafka@c" [:private "infra-c-s2run" ["c"]]}
             (into {} (map (juxt :instance (juxt :mode :compose-project :consumers)))
                   (get-in p [:ok :diff :instances])))))
    (let [up (w/up! c "abc" run)
          cs (stub/containers (w/engine c))]
      (is (r/ok? up) (pr-str up))
      (testing "two kafka containers, one per compose project"
        (is (= ["infra-c-s2run-kafka-1" "infra-kafka-1"] (w/containers-of c "kafka"))))
      (testing "the private one is labelled for the run"
        (is (= {"hive.run" run "hive.stack" "abc" "hive.instance" "infra/kafka@c"}
               (select-keys (get-in cs ["infra-c-s2run-kafka-1" :labels]) ["hive.run" "hive.stack" "hive.instance"]))))
      (testing "from C, kafka is the private container; from A and B, the shared one"
        (is (= ["infra-c-s2run-kafka-1"] (w/resolve-alias c "hive-s2run-c" "kafka")))
        (is (= ["infra-kafka-1"] (w/resolve-alias c "hive-s2run-a" "kafka")))
        (is (= ["infra-kafka-1"] (w/resolve-alias c "hive-s2run-b" "kafka")))
        (is (= #{"hive-s2run-c"} (get-in cs ["infra-c-s2run-kafka-1" :networks])))
        (is (= #{"hive-s2run-a" "hive-s2run-b"} (get-in cs ["infra-kafka-1" :networks]))
            "the shared kafka is never on C's network"))
      (testing "C's traffic stays on its broker"
        (let [b (w/broker)]
          (w/produce! b c "hive-s2run-c" "s2run.c.audit" :x)
          (is (= [] (w/consume b c "hive-s2run-a" "s2run.c.audit")))
          (is (= [:x] (w/consume b c "hive-s2run-c" "s2run.c.audit"))))))
    (testing "A and B unaffected when C's private instance goes"
      (let [down (w/down! c run)]
        (is (r/ok? down))
        (is (some #(= [:down-project "infra-c-s2run" :all] %) (stub/calls (w/engine c))))))))
