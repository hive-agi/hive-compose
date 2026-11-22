(ns hive-compose.pipeline.flaky-test
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.pipeline.flaky :as sut]
            [hive-compose.stub-history :as stub]))

;; SPDX-License-Identifier: MIT

(defn- ctx
  ([h] (ctx h nil))
  ([h flaky-cfg]
   (let [clock (atom 1000)]
     {:history h
      :now #(swap! clock + 1000)
      :settings (cond-> {} flaky-cfg (assoc :compose/flaky flaky-cfg))})))

(defn- run [i statuses]
  {:run/id (format "run-%03d" i)
   :suite/id "api"
   :project "shop"
   :exit (if (some #{:failed} (vals statuses)) 1 0)
   :report {:report/format :junit
            :report/cases (vec (for [[c s] (sort statuses)]
                                 {:case/id (str "api/T." c) :case/status s :case/time-s 0.1}))}})

(deftest recorded-runs-rank-the-flaky-case-first
  (let [h (stub/history)
        c (ctx h)]
    (doseq [[i s] (map-indexed vector [:passed :failed :passed :failed :passed :failed])]
      (is (= 2 (:appended (:ok (sut/record-run! c (run i {"flap" s "steady" :passed})))))))
    (is (= 12 (count (stub/rows h))))
    (let [res (:ok (sut/flaky c {:suite "api" :project "shop"} 20))]
      (is (= ["api/T.flap" "api/T.steady"] (mapv :case (:cases res))))
      (is (= {:flaky 1 :stable 1} (:classes res)))
      (is (= 2 (:total res)))
      (is (= 20 (get-in res [:config :window]))))
    (testing "limit cuts the ranking, not the totals"
      (let [res (:ok (sut/flaky c {} 1))]
        (is (= 1 (count (:cases res))))
        (is (= 2 (:total res)))))
    (testing "another project has no history yet"
      (is (string? (:note (:ok (sut/flaky c {:project "blog"} 20))))))))

(deftest thresholds-come-from-the-settings
  (let [h (stub/history)
        c (ctx h {:min-runs 10})]
    (doseq [[i s] (map-indexed vector [:passed :failed :passed :failed])]
      (sut/record-run! c (run i {"flap" s})))
    (is (= :stable (:class (first (:cases (:ok (sut/flaky c {} nil)))))) "too few runs for min-runs 10")))

(deftest an-invalid-run-writes-nothing
  (let [h (stub/history)
        res (sut/record-run! (ctx h) {:run/id "BAD ID" :suite/id "api"
                                      :report {:report/cases [{:case/id "api/x" :case/status :passed}]}})]
    (is (= :compose/invalid-run (:error res)))
    (is (= [] (stub/rows h)))))

(deftest port-faults-are-results
  (let [h (stub/history)]
    (stub/fail! h :append!)
    (is (= :stub/fault (:error (sut/record-run! (ctx h) (run 1 {"a" :passed})))))
    (stub/fail! h :history)
    (is (= :stub/fault (:error (sut/flaky (ctx h) {} 5)))))
  (is (= :compose/no-history (:error (sut/flaky {} {} 5)))))
