(ns hive-compose.scenario.s4-refcount-release-test
  "S4 refcount release (stub engine, real pipeline)."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.scenario.world :as w]
            [hive-compose.stub :as stub]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(deftest s4-the-last-run-binding-the-shared-kafka-releases-it
  "CLAIM: three runs at once, A alone (rua), B alone (rub), C alone (ruc,
   private kafka). Down A keeps the shared kafka (it reports kept by rub's
   infra/kafka); down B then releases it; down C removes its private compose
   project whole and touches nothing shared; no down argv ever carries -v or
   --volumes; a kafka that ran before any run is never removed."
  (let [c (w/ctx)
        e (w/engine c)]
    (doseq [[stack run] [["a" "rua"] ["b" "rub"] ["c" "ruc"]]]
      (is (r/ok? (w/up! c stack run)) (str stack " up")))
    (is (= ["infra-c-ruc-kafka-1" "infra-kafka-1"] (w/containers-of c "kafka")))
    (testing "down A: B still binds the shared kafka"
      (let [d (w/down! c "rua")]
        (is (r/ok? d))
        (is (= [{:instance "infra/kafka" :project "infra" :kept {"kafka" [["rub" "infra/kafka"]]}}]
               (get-in d [:ok :runs 0 :kept])))
        (is (= [] (get-in d [:ok :runs 0 :released])))
        (is (= #{"kafka"} (stub/running e "infra")))
        (is (= ["infra-kafka-1"] (w/resolve-alias c "hive-rub-b" "kafka")) "B still reaches it")
        (is (not (contains? (stub/networks e) "hive-rua-a")) "A's view network is gone")))
    (testing "down B: the last binder releases it"
      (let [d (w/down! c "rub")]
        (is (r/ok? d))
        (is (= [{:instance "infra/kafka" :project "infra" :services ["kafka"]}] (get-in d [:ok :runs 0 :released])))
        (is (= #{} (stub/running e "infra")))))
    (testing "down C: its private kafka goes with it, whole"
      (let [d (w/down! c "ruc")]
        (is (r/ok? d))
        (is (= [{:instance "infra/kafka@c" :project "infra-c-ruc" :services ["kafka"]}] (get-in d [:ok :runs 0 :released])))
        (is (some #(= [:down-project "infra-c-ruc" :all] %) (stub/calls e)))))
    (is (empty? (stub/containers e)))
    (is (empty? (stub/networks e)))
    (is (empty? (:stacks @(:state c))))
    (testing "volumes are never removed"
      (is (seq (w/down-argvs c)))
      (is (not-any? w/volume-flag? (w/down-argvs c)) (pr-str (w/down-argvs c))))))

(deftest s4-a-kafka-that-ran-before-the-runs-is-never-removed
  "CLAIM: when infra's kafka already runs (a section or a human started it),
   A and B bind it, and after both runs are down it still runs: a run never
   removes what it did not start."
  (let [c (w/ctx {:running {"infra" #{"kafka"}}})]
    (is (r/ok? (w/up! c "a" "rpa")))
    (is (r/ok? (w/up! c "b" "rpb")))
    (is (r/ok? (w/down! c "rpa")))
    (let [d (w/down! c "rpb")]
      (is (r/ok? d))
      (is (= #{"kafka"} (stub/running (w/engine c) "infra")) (pr-str d)))))
