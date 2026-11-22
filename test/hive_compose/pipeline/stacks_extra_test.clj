(ns hive-compose.pipeline.stacks-extra-test
  "`:extra` needs on stack-up!/plan (hc-8): a suite consumer `suite:<id>` is
   resolved, wired and given its view network, and is never started; nil or
   empty `:extra` leaves the plan exactly as before."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.pipeline.stacks :as stacks]
            [hive-compose.stub :as stub]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def projects
  {"kafka" {:project/id "kafka" :project/dir "/w/kafka"
            :project/provides {"kafka" {:service "kafka" :version "3.7" :env {"KAFKA_BROKERS" "kafka:9092"}}}}
   "pg" {:project/id "pg" :project/dir "/w/pg"
         :project/provides {"postgres" {:service "db" :version "16" :env {"PGHOST" "db"}}}}
   "shop" {:project/id "shop" :project/dir "/w/shop" :project/needs {"kafka" {:version "^3"}}}})

(def world {"kafka" {"kafka" {}} "pg" {"db" {}} "shop" {"api" {}}})

(defn- ctx []
  {:engine (stub/engine {} {} world)
   :settings {:compose/projects projects :compose/profiles {}
              :compose/stacks {"shop" {:stack/id "shop" :stack/consumers ["shop"]}}}
   :state (atom {:active {} :current nil})
   :now (constantly 0)})

(deftest a-suite-consumer-resolves-through-extra
  (let [c (ctx)
        extra {"suite:smoke" {"postgres" {}}}
        res (stacks/resolve-plan c "shop" "run001" extra)]
    (is (r/ok? res) (pr-str res))
    (testing "bound and wired, with its own view network"
      (is (= {"postgres" "pg/postgres"} (get-in res [:ok :resolution :resolution/bindings "suite:smoke"])))
      (is (= "hive-run001-suite-smoke" (get-in res [:ok :wirings "suite:smoke" :wiring/network])))
      (is (some #{"hive-run001-suite-smoke"} (get-in res [:ok :plan :stack/networks]))))
    (testing "without :extra the suite is unknown to the stack"
      (is (nil? (get-in (stacks/resolve-plan c "shop" "run001") [:ok :resolution :resolution/bindings "suite:smoke"]))))
    (testing "stack-up! brings its instances up and joins them to its network, but never starts the suite"
      (let [up (stacks/stack-up! c {:stack "shop" :run "run002" :extra extra})]
        (is (r/ok? up) (pr-str up))
        (is (= ["shop"] (mapv :consumer (get-in up [:ok :consumers]))))
        (is (contains? (get-in (stub/networks (:engine c)) ["hive-run002-suite-smoke" :members]) "pg-db-1"))
        (is (not-any? #(re-find #"suite" (str (second %))) (filter #(= :up-project (first %)) (stub/calls (:engine c)))))))))

(deftest the-plan-command-takes-extra
  (let [res (stacks/plan (ctx) {:stack "shop" :extra {"suite:smoke" {"postgres" {}}}})]
    (is (r/ok? res) (pr-str res))
    (is (some #(= "pg/postgres" (:instance %)) (get-in res [:ok :diff :instances])))))

(defspec empty-extra-changes-nothing 30
  (prop/for-all [run (gen/fmap #(apply str "r" %) (gen/vector (gen/elements "abc0123456789") 2 10))
                 extra (gen/elements [nil {}])]
    (let [c (ctx)]
      (= (stacks/resolve-plan c "shop" run) (stacks/resolve-plan c "shop" run extra)))))
