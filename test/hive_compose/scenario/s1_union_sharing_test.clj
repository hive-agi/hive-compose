(ns hive-compose.scenario.s1-union-sharing-test
  "S1 union sharing (stub engine, real pipeline)."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [hive-compose.pipeline.stacks :as stacks]
            [hive-compose.scenario.world :as w]
            [hive-compose.stub :as stub]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(deftest s1-two-consumers-of-kafka-3-share-one-instance
  "CLAIM: projects A and B both need kafka ^3 and infra provides 3.7.1. The
   stack plan marks the one instance infra/kafka :shared before anything
   starts; stack-up runs exactly ONE kafka container; A and B each sit on
   their own view network (hive-<run>-a, hive-<run>-b) where `kafka` resolves
   to that same container, both are given KAFKA_BROKERS=kafka:9092 unchanged;
   their tenancy prefixes differ, so what A produces under its prefix B can
   read from the same broker while B's own topics stay under B's prefix."
  (let [c (w/ctx)
        run "s1run"
        before (stacks/plan c {:stack "ab" :run run})]
    (testing "plan, before anything runs: one shared instance, to start"
      (is (r/ok? before) (pr-str before))
      (is (= {:instances 1 :shared 1 :private 0 :running 0 :partial 0 :to-start 1}
             (get-in before [:ok :diff :summary])))
      (is (= [{:instance "infra/kafka" :mode :shared :state :to-start :consumers ["a" "b"]}]
             (map #(select-keys % [:instance :mode :state :consumers]) (get-in before [:ok :diff :instances]))))
      (is (str/includes? (get-in before [:ok :text]) "1 instances (1 shared, 0 private)")))
    (let [up (w/up! c "ab" run)]
      (is (r/ok? up) (pr-str up))
      (testing "exactly one kafka container, under the provider's own compose project"
        (is (= ["infra-kafka-1"] (w/containers-of c "kafka")))
        (is (= [{:id "infra/kafka" :compose-project "infra" :shared? true}]
               (map #(select-keys % [:id :compose-project :shared?]) (get-in up [:ok :instances])))))
      (testing "each consumer on its own view network, kafka resolving to the one container"
        (is (= ["hive-s1run-a" "hive-s1run-b"] (sort (get-in up [:ok :networks]))))
        (is (= ["infra-kafka-1"] (w/resolve-alias c "hive-s1run-a" "kafka")))
        (is (= ["infra-kafka-1"] (w/resolve-alias c "hive-s1run-b" "kafka")))
        (is (= #{"hive-s1run-a"} (get-in (stub/containers (w/engine c)) ["a-s1run-app-1" :networks]))
            "A attaches to its view network alone")
        (is (= #{"hive-s1run-b"} (get-in (stub/containers (w/engine c)) ["b-s1run-app-1" :networks]))))
      (testing "the plan now reads running"
        (is (= :running (get-in (stacks/plan c {:stack "ab" :run run}) [:ok :diff :instances 0 :state]))))
      (let [wa (w/wiring-of c "ab" run "a")
            wb (w/wiring-of c "ab" run "b")
            b (w/broker)]
        (testing "same endpoint as written, distinct tenancy prefixes"
          (is (= "kafka:9092" (get-in wa [:wiring/env "KAFKA_BROKERS"]) (get-in wb [:wiring/env "KAFKA_BROKERS"])))
          (is (= ["s1run.a." "s1run.b."] [(w/prefix-of wa) (w/prefix-of wb)])))
        (testing "the consumers' containers get the injected vars by name"
          (let [ups (filter #(= :up-project (first %)) (stub/calls (w/engine c)))
                opts (into {} (map (fn [[_ p _ o]] [p o])) ups)]
            (is (= ["HIVE_RUN_ID" "KAFKA_BROKERS" "KAFKA_TOPIC_PREFIX" "OTEL_RESOURCE_ATTRIBUTES"]
                   (get-in opts ["a-s1run" :container-env])))
            (is (= ["hive-s1run-b"] (get-in opts ["b-s1run" :networks])))))
        (testing "A produces under its prefix, B consumes it from the same broker"
          (let [topic (str (w/prefix-of wa) "orders")]
            (is (= "infra-kafka-1" (w/produce! b c "hive-s1run-a" topic {:order 1})))
            (is (= [{:order 1}] (w/consume b c "hive-s1run-b" topic)))
            (w/produce! b c "hive-s1run-b" (str (w/prefix-of wb) "invoices") {:invoice 1})
            (is (= ["s1run.a.orders" "s1run.b.invoices"] (w/topics-on b "infra-kafka-1"))
                "each consumer's topics stay under its own prefix")))))
    (is (r/ok? (w/down! c run)))
    (is (empty? (stub/containers (w/engine c))) "down leaves nothing of the run")
    (is (empty? (stub/networks (w/engine c))))))
