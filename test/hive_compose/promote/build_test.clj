(ns hive-compose.promote.build-test
  "The selection law of `compose build` (card 20261123112858-6f2c31bc): what
   a build builds, what it refuses, who keeps the old image, how a failure
   reads."
  (:require [clojure.set :as set]
            [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.promote.build :as build]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def facts
  {"postgres" {:build? false :image "postgres:16"}
   "sisf-payment" {:build? true :image "dc-sisf-payment"}
   "sisf-sync" {:build? true :image "dc-sisf-sync"}})

(deftest the-default-is-every-service-of-the-closure-that-declares-build
  (is (= ["sisf-payment" "sisf-sync"] (:ok (build/select facts nil))))
  (is (= [] (:ok (build/select {"postgres" {:build? false}} nil))) "nothing to build is ok"))

(deftest services-narrow-and-the-two-refusals-name-the-service
  (is (= ["sisf-sync"] (:ok (build/select facts ["sisf-sync"]))))
  (let [res (build/select facts ["nope" "sisf-sync"])]
    (is (= :compose/unknown-names (:error res)))
    (is (= ["nope"] (:services res))))
  (let [res (build/select facts ["postgres"])]
    (is (= :compose/invalid-param (:error res)))
    (is (= ["postgres"] (:value res)))
    (is (re-find #"postgres" (:message res)))))

(deftest the-services-param-reads-text-and-lists
  (is (nil? (build/parse-services nil)))
  (is (nil? (build/parse-services " , ")))
  (is (= ["a" "b"] (build/parse-services "a, b,,a")))
  (is (= ["a" "b"] (build/parse-services ["a" "b"]))))

(def gen-facts
  (gen/map (gen/elements ["a" "b" "c" "d" "e" "f"])
           (gen/fmap (fn [b] {:build? b :image "i"}) gen/boolean)
           {:max-elements 6}))

(defspec selection-law 300
  (prop/for-all [f gen-facts
                 req (gen/one-of [(gen/return nil)
                                  (gen/not-empty (gen/vector (gen/elements ["a" "b" "c" "d" "e" "f" "zz"]) 1 4))])]
    (let [res (build/select f req)
          buildable (set (for [[s m] f :when (:build? m)] s))]
      (cond
        (nil? req) (and (r/ok? res) (= buildable (set (:ok res))) (= (:ok res) (sort (:ok res))))
        (not (every? f req)) (and (= :compose/unknown-names (:error res))
                                  (= (set (remove f req)) (set (:services res))))
        (not (every? buildable req)) (and (= :compose/invalid-param (:error res))
                                          (= (set (remove buildable req)) (set (:value res))))
        :else (and (r/ok? res)
                   (= (set req) (set (:ok res)))
                   (set/subset? (set (:ok res)) buildable))))))

(deftest running-old-image-names-the-active-sections-holding-a-built-service
  (let [active {"flow-payment" {:project "dc" :services ["sisf-payment"] :needs ["postgres" "sisf-payment"]}
                "dc/postgres" {:project "dc" :services ["postgres"]}
                "other/x" {:project "other" :services ["sisf-payment"]}}]
    (is (= [{:section "flow-payment" :services ["sisf-payment"]}]
           (build/running-old-image active "dc" ["sisf-payment"])))
    (is (= [] (build/running-old-image active "dc" ["sisf-sync"])))))

(deftest a-failure-names-the-failing-service-and-the-output-tail
  (let [out (str (apply str (for [i (range 40)] (str "#" i " step\n")))
                 "[sisf-sync builder 3/7] RUN make\n"
                 "target sisf-sync: failed to solve: exit code 2\n")
        f (build/failure "dc" ["sisf-payment" "sisf-sync"] {:error :compose/command-failed :exit 17 :stderr out} 30)]
    (is (= "sisf-sync" (:service f)))
    (is (= 30 (count (:output-tail f))))
    (is (= "target sisf-sync: failed to solve: exit code 2" (peek (:output-tail f))))
    (is (= 17 (:exit f))))
  (testing "one service built: it is the failing one"
    (is (= "a" (:service (build/failure "p" ["a"] {:error :compose/command-failed :exit 1 :stderr "boom"} 5)))))
  (testing "a timeout says so"
    (let [f (build/failure "p" ["a"] {:error :shell/timeout :timeout-ms 1000} 5)]
      (is (:timed-out? f))
      (is (re-find #"build-timeout-ms" (:message f))))))
