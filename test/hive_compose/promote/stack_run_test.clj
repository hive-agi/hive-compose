(ns hive-compose.promote.stack-run-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.promote.stack-run :as run]
            [hive-compose.schema :as schema]
            [malli.core :as m]))

;; SPDX-License-Identifier: MIT

(deftest run-ids
  (is (= "shop3f9a1c" (run/run-id "shop" "3f9a1c")))
  (is (= "billingapi3f9a1c" (run/run-id "Billing/API" "3f9a1c")))
  (is (= "run3f9a1c" (run/run-id "--" "3f9a1c")))
  (is (= 36 (count (run/run-id (apply str (repeat 50 "a")) "3f9a1c")))))

(defspec generated-run-ids-are-run-ids 200
  (prop/for-all [sid (gen/such-that seq gen/string-ascii)
                 hex (gen/fmap #(apply str %) (gen/vector (gen/elements "0123456789abcdef") 6))]
    (m/validate schema/RunId (run/run-id sid hex))))

(deftest readiness
  (let [h {"a" {:state "running" :health :healthy}
           "b" {:state "running" :health :none}
           "c" {:state "running" :health :starting}
           "d" {:state "exited" :health :none}}]
    (is (run/ready? h ["a" "b"]))
    (is (not (run/ready? h ["a" "c"])))
    (is (= {"c" {:state "running" :health :starting}
            "d" {:state "exited" :health :none}
            "e" {:state "missing" :health :none}}
           (run/not-ready h ["a" "c" "d" "e"])))))

(deftest consumers
  (is (run/container-consumer? "shop"))
  (is (run/container-consumer? "shop/api"))
  (is (not (run/container-consumer? "stack:shop")))
  (is (not (run/container-consumer? "suite:smoke")))
  (is (= "shop-r1x" (run/consumer-compose-project {:compose/projects {"shop" {:project/dir "/w/Shop"}}} "shop" "r1x")))
  (is (= "shop-api-r1x" (run/consumer-compose-project {:compose/projects {"shop" {:project/dir "/w/Shop"}}} "shop/api" "r1x"))
      "two sections of one project never share a compose project"))

(deftest otlp-endpoint-follows-the-bound-alias
  (let [res {:resolution/instances {"tel/otel" {:instance/aliases ["collector"]}}
             :resolution/bindings {"a" {"otel" "tel/otel"} "b" {"kafka" "x/kafka"}}}
        f (run/otlp-envs res "r1x")]
    (is (= "http://collector:4317" (get (first (f "a")) "OTEL_EXPORTER_OTLP_ENDPOINT")))
    (is (= "grpc" (get (first (f "a")) "OTEL_EXPORTER_OTLP_PROTOCOL")))
    (is (nil? (f "b")) "no otel bound, nothing injected")))

(deftest preexistence-marks
  (testing "marked per provider project as a service set"
    (is (= {"infra" #{"kafka" "zookeeper"}}
           (run/mark-preexisting {} {"infra" #{"kafka" "zookeeper"}} {} "r1" {}))))
  (testing "what another live run holds is not the world's"
    (let [other {:stack/instances {"infra/kafka" {:instance/compose-project "infra"
                                                  :instance/services ["kafka"] :instance/needs ["zookeeper"]}}}]
      (is (= {} (run/mark-preexisting {} {"infra" #{"kafka" "zookeeper"}} {"r0" other} "r1" {})))))
  (testing "what an active section holds is not marked either"
    (is (= {"infra" #{"zookeeper"}}
           (run/mark-preexisting {} {"infra" #{"kafka" "zookeeper"}} {} "r1" {"infra" #{"kafka"}}))))
  (testing "marks only grow"
    (is (= {"infra" #{"kafka" "x"}} (run/mark-preexisting {"infra" #{"x"}} {"infra" #{"kafka"}} {} "r1" {})))
    (is (= {"infra" #{"x"}} (run/mark-preexisting {"infra" #{"x"}} {} {} "r1" {})))))

(deftest sparing
  (is (= {:services ["b"] :kept {"a" [[:preexisting]]}}
         (run/sparing-preexisting {:services ["a" "b"]} ["a"])))
  (is (= {:services ["a"]} (run/sparing-preexisting {:services ["a"]} []))))

(deftest idle-runs
  (let [ttls {:ttl-ms 100 :orphan-ttl-ms 10 :claim-ttl-ms 50}]
    (is (= ["old"] (run/idle-runs {"old" {:last-touch 0} "new" {:last-touch 90}} 100 ttls)))
    (is (= [] (run/idle-runs {"c" {:last-touch 0 :claims {"a" {:seen 80}}}} 100 ttls)))
    (is (= ["c"] (run/idle-runs {"c" {:last-touch 0 :claims {"a" {:seen 10}}}} 100 ttls)) "an expired claim")
    (is (= ["o"] (run/idle-runs {"o" {:last-touch 85 :orphaned-at 85}} 100 ttls)))
    (is (= [] (run/idle-runs {"n" {:last-touch 0 :idle-action :none}} 100 ttls)))))

(deftest specs
  (let [settings {:compose/projects {"kafka" {:project/dir "/w/kafka" :project/files ["c.yml"] :project/env {"V" "1"}}
                                     "shop" {:project/dir "/w/shop"}}}
        ip {:instance/id "kafka/kafka@shop" :instance/provider "kafka" :instance/share? false
            :instance/compose-project "kafka-shop-r1x" :instance/services ["kafka"] :instance/needs ["zk"]
            :instance/labels {"hive.run" "r1x"}}]
    (is (= {:dir "/w/kafka" :files ["c.yml"] :project "kafka-shop-r1x" :services ["kafka" "zk"]
            :env {"V" "1"} :wait? true :labels {"hive.run" "r1x"}}
           (run/instance-spec settings ip true)))
    (is (= {:dir "/w/kafka" :files ["c.yml"] :project "kafka" :services ["kafka"] :env {"V" "1"} :wait? false}
           (run/instance-spec settings (assoc ip :instance/share? true :instance/compose-project "kafka") false))
        "shared: as its project runs, no labels")
    (is (= {:dir "/w/shop" :project "shop-r1x" :services ["api"] :env {"A" "1" "HIVE_RUN_ID" "r1x"}
            :container-env ["A" "HIVE_RUN_ID"] :labels {"hive.run" "r1x" "hive.stack" "s" "hive.consumer" "shop"}
            :networks ["hive-r1x-shop"] :wait? true}
           (run/consumer-spec settings "r1x" "s" "shop" ["api"]
                              {:wiring/env {"A" "1" "HIVE_RUN_ID" "r1x"} :wiring/network "hive-r1x-shop"}
                              #{"HIVE_RUN_ID" "A"} true)))))
