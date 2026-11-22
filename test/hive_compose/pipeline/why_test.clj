(ns hive-compose.pipeline.why-test
  "`why!` over the stub engine and the stub trace store: result files on disk
   as hc-8 writes them, a run in the stack state, logs per compose project."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [hive-compose.boundary.runs :as runs]
            [hive-compose.pipeline.why :as sut]
            [hive-compose.stub :as stub]
            [hive-compose.stub-trace :as stub-trace]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- temp-dir []
  (str (java.nio.file.Files/createTempDirectory "hive-compose-why"
                                                (make-array java.nio.file.attribute.FileAttribute 0))))

(def t0 1795367920000)

(defn- iso [ms] (str (java.time.Instant/ofEpochMilli ms)))

(def boom-case
  {:case/id "orders-suite/orders.place_boom_order" :case/name "place_boom_order"
   :case/classname "orders" :case/status :failed :case/time-s 0.2
   :case/message "POST /orders returned 500, expected 202" :case/output nil})

(def good-case
  (assoc boom-case :case/id "orders-suite/orders.place_good_order" :case/name "place_good_order"
         :case/status :passed :case/message nil))

(defn- result [run]
  {:run/id run :suite/id "orders-suite" :project "orders" :exit 1 :status :failed
   :started-at t0 :finished-at (+ t0 4000) :duration-ms 4000
   :report {:report/format :junit :report/cases [good-case boom-case]
            :report/totals {:passed 1 :failed 1 :error 0 :skipped 0}}})

(defn- write-result! [dir run]
  (let [f (io/file dir "runs" run "orders-suite" "orders" runs/result-file)]
    (io/make-parents f)
    (spit f (pr-str (result run)))))

(defn- sp [id parent start status msg attrs]
  {:span/id id :span/parent-id parent :span/name id :span/service "orders"
   :span/start-ms start :span/duration-ms 1 :span/status status :span/status-message msg
   :span/attrs attrs})

(defn- boom-trace [run]
  (let [a {"hive.run.id" run "test.id" "orders.place_boom_order"}]
    {:trace/id "t-boom"
     :trace/spans [(sp "POST /orders" nil (+ t0 100) :error nil a)
                   (sp "orders validate" "POST /orders" (+ t0 110) :error "sku boom rejected" a)]}))

(defn- ctx [dir run & {:keys [traces note!]}]
  (let [engine (stub/engine {})]
    (stub/logs! engine (str "orders-" run)
                (str "orders-1  | " (iso (+ t0 105)) " rejecting sku boom\n"
                     "orders-1  | " (iso (+ t0 60000)) " much later\n"))
    {:engine engine
     :trace-query (stub-trace/query (or traces [(boom-trace run)]))
     :settings {:compose/projects {"orders" {:project/id "orders" :project/dir "/w/orders"}}
                :compose/state-file (str dir "/state.edn")}
     :state (atom {:active {} :current nil
                   :stacks {run {:stack/id "shop" :run run
                                 :consumers {"orders" {:compose-project (str "orders-" run)
                                                       :services ["orders"]}}}}})
     :now (constantly t0)
     :note! note!}))

(deftest why-explains-the-boom-order
  (let [dir (temp-dir)
        _ (write-result! dir "r1")
        notes (atom [])
        c (ctx dir "r1" :note! #(swap! notes conj %))
        res (sut/why! c {:test "orders.place_boom_order"})
        b (:ok res)]
    (is (r/ok? res) (pr-str res))
    (is (= "orders validate" (get-in b [:why/failing-span :span/name])) "the child carrying the message")
    (is (= "sku boom rejected" (get-in b [:why/failing-span :span/status-message])))
    (is (= ["POST /orders" "orders validate"] (mapv :span/name (:why/span-path b))))
    (is (= {"orders" [(str "orders-1  | " (iso (+ t0 105)) " rejecting sku boom")]} (:why/logs b))
        "only the lines inside the case window")
    (is (= "r1" (:why/run b)))
    (testing "the query names the run and the test"
      (is (= {"hive.run.id" "r1" "test.id" "orders.place_boom_order"}
             (:attrs (second (first (stub-trace/calls (:trace-query c)))))))
      (is (= [t0 (+ t0 4000)] ((juxt :start-ms :end-ms) (second (first (stub-trace/calls (:trace-query c))))))))
    (testing "logs are asked with timestamps of the run's compose project"
      (is (some #(= [:logs "orders-r1" [] sut/default-tail] %) (stub/calls (:engine c)))))
    (testing "the bundle is written beside the run"
      (let [{:keys [md edn]} (:why/files b)]
        (is (str/ends-with? md "/runs/r1/why/orders-suite_orders.place_boom_order.md"))
        (is (str/includes? (slurp md) "sku boom rejected"))
        (is (= "orders validate" (get-in (edn/read-string (slurp edn)) [:why/failing-span :span/name])))))
    (is (= 1 (count @notes)))
    (is (nil? (:why/gaps b)))))

(deftest why-names-a-test-every-way-and-prefers-the-failure
  (let [dir (temp-dir)]
    (write-result! dir "r1")
    (doseq [t ["orders.place_boom_order" "orders-suite/orders/orders.place_boom_order"
               "orders-suite/orders.place_boom_order" "place_boom_order"]]
      (is (= :failed (get-in (sut/why! (ctx dir "r1") {:test t}) [:ok :why/case :case/status])) t))
    (let [res (sut/why! (ctx dir "r1") {:test "nope"})]
      (is (= :compose/unknown-test (:error res)))
      (is (= ["orders-suite/orders/orders-suite/orders.place_boom_order"] (:known res))))))

(deftest why-without-traces-or-results
  (let [dir (temp-dir)]
    (is (= :compose/no-results (:error (sut/why! (ctx dir "r1") {:test "x"}))))
    (is (= :compose/missing-param (:error (sut/why! (ctx dir "r1") {}))))
    (is (= :compose/invalid-run (:error (sut/why! (ctx dir "r1") {:test "x" :run "Bad-Run"}))))
    (write-result! dir "r1")
    (testing "a trace store that fails leaves a gap, not an error"
      (let [c (ctx dir "r1")
            _ (stub-trace/fail! (:trace-query c) :find-traces)
            b (:ok (sut/why! c {:test "orders.place_boom_order"}))]
        (is (nil? (:why/failing-span b)))
        (is (= :stub/fault (get-in b [:why/gaps :traces 0 :error])))
        (is (seq (:why/logs b)) "logs still come")))
    (testing "the fallback query on the cell's test id finds the runner's spans"
      (let [t {:trace/id "t-cell"
               :trace/spans [(sp "suite" nil (+ t0 10) :error "case failed"
                                 {"hive.run.id" "r1" "test.id" "orders-suite/orders"})]}
            b (:ok (sut/why! (ctx dir "r1" :traces [t]) {:test "orders.place_boom_order"}))]
        (is (= "t-cell" (get-in b [:why/failing-span :trace/id])))
        (is (= {"hive.run.id" "r1" "test.id" "orders-suite/orders"} (get-in b [:why/trace-query :attrs])))))
    (testing "a note that throws is a gap"
      (let [b (:ok (sut/why! (ctx dir "r1" :note! (fn [_] (throw (ex-info "down" {}))))
                             {:test "orders.place_boom_order"}))]
        (is (= :compose/note-failed (get-in b [:why/gaps :note :error])))))))
