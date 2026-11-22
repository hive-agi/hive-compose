(ns hive-compose.promote.flaky-oom-test
  "hc-16b: an OOM kill at a memory cap is classed :oom, never as a flake."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.promote.flaky :as flaky]))

;; SPDX-License-Identifier: MIT

(def ^:private cfg flaky/defaults)

(defn- row [i status & {:keys [oom?]}]
  (cond-> {:run/id (str "r" i) :suite/id "e2e" :project "orders" :case/id "e2e/T"
           :case/status status :at i}
    oom? (assoc :case/oom? true)))

(deftest oom-runs-between-passes-are-not-a-flake
  (let [rows (concat (for [i (range 0 10 2)] (row i :passed))
                     (for [i (range 1 10 2)] (row i :error :oom? true)))
        rep (flaky/case-report cfg rows)]
    (is (= :oom (:class rep)))
    (is (= 5 (:ooms rep)))
    (is (= 0 (:fails rep)))
    (is (= 0 (:flips rep)))
    (is (= "r9" (get-in rep [:last-oom :run])))))

(deftest the-same-rows-without-the-oom-mark-are-a-flake
  (let [rows (concat (for [i (range 0 10 2)] (row i :passed))
                     (for [i (range 1 10 2)] (row i :error)))]
    (is (= :flaky (:class (flaky/case-report cfg rows))))))

(deftest real-fails-keep-their-class-beside-an-oom
  (let [rows (concat (for [i (range 6)] (row i (if (even? i) :passed :failed)))
                     [(row 6 :error :oom? true)])
        rep (flaky/case-report cfg rows)]
    (is (= :flaky (:class rep)))
    (is (= 1 (:ooms rep)))))

(deftest an-old-oom-out-of-the-window-reads-stable
  (let [rows (cons (row 0 :error :oom? true) (for [i (range 1 30)] (row i :passed)))]
    (is (= :stable (:class (flaky/case-report cfg rows))))))

(deftest run-outcomes-carry-the-oom-mark
  (let [run {:run/id "r1" :suite/id "e2e" :project "orders" :exit 137
             :oom {:oom/service "e2e/orders" :oom/line "e2e/orders OOM-killed at 512m"}}]
    (testing "a runner that died with no report records an :error (run) row, marked"
      (is (= [{:run/id "r1" :suite/id "e2e" :project "orders" :at 5
               :case/oom? true :case/id "e2e/(run)" :case/status :error}]
             (flaky/run-outcomes run 5))))
    (is (= :oom (:class (first (flaky/rank cfg (flaky/run-outcomes run 5))))))))

(deftest rank-puts-oom-after-regressions
  (let [rows (concat [(row 0 :error :oom? true)]
                     (for [i (range 3)] (assoc (row i :failed) :case/id "e2e/R")))]
    (is (= [:regression :oom] (mapv :class (flaky/rank cfg rows))))))
