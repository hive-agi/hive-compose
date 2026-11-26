(ns hive-compose.pipeline.why-test
  "`why!` over the stub engine and the stub trace store: result files on disk
   as hc-8 writes them, a run in the stack state, logs per compose project."
  (:require [clojure.edn :as edn]
            [hive-compose.adapter.cli]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [hive-compose.boundary.runs :as runs]
            [hive-compose.pipeline.why :as sut]
            [hive-compose.port :as port]
            [hive-compose.promote.suite :as suite]
            [hive-compose.stub :as stub]
            [hive-compose.stub-runner :as stub-runner]
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

(defn- write-result!
  "As hc-8 writes it: through the shared codec."
  [dir run]
  (let [w (runs/write-result! dir (result run))]
    (assert (r/ok? w) (pr-str w))
    (:ok w)))

(deftest the-result-codec-round-trips-and-refuses-what-is-no-cell-result
  (let [dir (temp-dir)
        path (write-result! dir "run7")]
    (is (= (runs/result-path dir "run7" "orders-suite" "orders") path))
    (is (str/ends-with? path "/runs/run7/orders-suite/orders/result.edn"))
    (is (= (result "run7") (:ok (runs/read-result path))) "what is written reads back the same")
    (is (= ["run7"] (map :run (runs/result-files dir nil)))))
  (let [dir (temp-dir)]
    (is (= :compose/invalid-result (:error (runs/write-result! dir (assoc (result "run7") :project "../up")))))
    (is (= :compose/invalid-result (:error (runs/write-result! dir (dissoc (result "run7") :status)))))
    (is (empty? (runs/result-files dir nil)) "nothing written"))
  (testing "a file that is no cell result is skipped and reported"
    (let [dir (temp-dir)
          f (io/file dir "runs" "run8" "s" "p" runs/result-file)]
      (io/make-parents f)
      (spit f "{:hello 1}")
      (let [{:keys [results skipped]} (runs/read-results dir nil)]
        (is (empty? results))
        (is (= [:compose/invalid-result] (map :error skipped)))))))

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
        _ (write-result! dir "run1")
        notes (atom [])
        c (ctx dir "run1" :note! #(swap! notes conj %))
        res (sut/why! c {:test "orders.place_boom_order"})
        b (:ok res)]
    (is (r/ok? res) (pr-str res))
    (is (= "orders validate" (get-in b [:why/failing-span :span/name])) "the child carrying the message")
    (is (= "sku boom rejected" (get-in b [:why/failing-span :span/status-message])))
    (is (= ["POST /orders" "orders validate"] (mapv :span/name (:why/span-path b))))
    (is (= {"orders" [(str "orders-1  | " (iso (+ t0 105)) " rejecting sku boom")]} (:why/logs b))
        "only the lines inside the case window")
    (is (= "run1" (:why/run b)))
    (testing "the query names the run and the test"
      (is (= {"hive.run.id" "run1" "test.id" "orders.place_boom_order"}
             (:attrs (second (first (stub-trace/calls (:trace-query c)))))))
      (is (= [t0 (+ t0 4000)] ((juxt :start-ms :end-ms) (second (first (stub-trace/calls (:trace-query c))))))))
    (testing "logs are asked with timestamps of the run's compose project"
      (is (some #(= [:logs "orders-run1" [] sut/default-tail] %) (stub/calls (:engine c)))))
    (testing "the bundle is written beside the run"
      (let [{:keys [md edn]} (:why/files b)]
        (is (str/ends-with? md "/runs/run1/why/orders-suite_orders.place_boom_order.md"))
        (is (str/includes? (slurp md) "sku boom rejected"))
        (is (= "orders validate" (get-in (edn/read-string (slurp edn)) [:why/failing-span :span/name])))))
    (is (= 1 (count @notes)))
    (is (nil? (:why/gaps b)))))

(deftest logs-are-asked-for-the-case-window-not-a-tail
  (let [p (sut/logs-profile {:project "orders-run1" :dir "/w/orders" :env {"A" "1"}} [t0 (+ t0 4000)] 2000)
        argv (hive-compose.adapter.cli/logs-argv p [] 2000)]
    (is (= (iso (- t0 2000)) (:profile/since p)))
    (is (= (iso (+ t0 6000)) (:profile/until p)))
    (is (= {"A" "1"} (:profile/env p)) "the project's env, so compose can interpolate its files")
    (is (= ["--timestamps" "--since" (iso (- t0 2000)) "--until" (iso (+ t0 6000))]
           (drop (inc (.indexOf ^java.util.List argv "2000")) argv))))
  (testing "no dir is left out, never nil in argv"
    (is (not (contains? (sut/logs-profile {:project "x"} [nil nil] 0) :profile/dir))))
  (testing "output at the engine's clip is reported as a gap"
    (let [dir (temp-dir)
          _ (write-result! dir "run1")
          c (ctx dir "run1")
          _ (stub/logs! (:engine c) "orders-run1" (apply str (repeat sut/clip-chars "x")))
          b (:ok (sut/why! c {:test "orders.place_boom_order"}))]
      (is (= ["orders-run1"] (get-in b [:why/gaps :logs-clipped]))))))

(deftest the-run-is-the-directory-not-the-file-content
  (let [dir (temp-dir)
        f (io/file dir "runs" "run2" "orders-suite" "orders" runs/result-file)]
    (io/make-parents f)
    (spit f (pr-str (assoc (result "run2") :run/id "../../escape")))
    (testing "a file whose run id is no RunId is refused"
      (is (= [:compose/invalid-result] (map :error (:skipped (runs/read-results dir nil)))))
      (is (= :compose/unknown-test (:error (sut/why! (ctx dir "run2") {:test "orders.place_boom_order"})))))
    (spit f (pr-str (assoc (result "run2") :run/id "other")))
    (testing "a file naming another run than its directory is skipped"
      (is (= [:compose/result-run-mismatch] (map :error (:skipped (runs/read-results dir nil))))))
    (spit f (pr-str (result "run2")))
    (let [b (:ok (sut/why! (ctx dir "run2") {:test "orders.place_boom_order"}))]
      (is (= "run2" (:why/run b)))
      (is (str/includes? (get-in b [:why/files :md]) "/runs/run2/why/")))))

(deftest why-names-a-test-every-way-and-prefers-the-failure
  (let [dir (temp-dir)]
    (write-result! dir "run1")
    (doseq [t ["orders.place_boom_order" "orders-suite/orders/orders.place_boom_order"
               "orders-suite/orders.place_boom_order" "place_boom_order"]]
      (is (= :failed (get-in (sut/why! (ctx dir "run1") {:test t}) [:ok :why/case :case/status])) t))
    (let [res (sut/why! (ctx dir "run1") {:test "nope"})]
      (is (= :compose/unknown-test (:error res)))
      (is (= ["orders-suite/orders/orders-suite/orders.place_boom_order"] (:known res))))))

(deftest why-names-the-oom-killed-runner-and-its-cap
  (let [dir (temp-dir)
        suites {"hog-suite" {:suite/id "hog-suite" :suite/image "hive-compose-e2e/hog:dev"
                             :suite/resources {:mem "256m"}}}
        ids (atom 0)
        spec (:ok (suite/runner-spec {:suites suites :run "run1" :state-dir dir
                                      :id-source (fn [n] (format (str "%0" (* 2 n) "x") (swap! ids inc)))}
                                     {:suite/id "hog-suite" :project "w"}))
        runner (stub-runner/capped-suite-runner {"hog-suite/w" {:peak-mem (* 512 1024 1024)}})
        answer (port/-run! runner spec)
        cell (suite/cell-result "run1" (:cell spec)
                                (suite/runner-outcome (:ok answer) (r/err :report/none {:dir (:reports-dir spec)})))]
    (testing "the capped cell is OOM-killed and its result carries the finding"
      (is (= {:mem "256m"} (:resources spec)) "the suite's cap reaches the runner spec")
      (is (= 137 (:exit cell)))
      (is (= :error (:status cell)))
      (is (= "hog-suite/w OOM-killed at 256m" (get-in cell [:oom :oom/line])))
      (is (= "hog-suite/w OOM-killed at 256m"
             (get-in (suite/matrix-summary [cell]) [:failures 0 :oom/line]))))
    (is (r/ok? (runs/write-result! dir cell)) "the codec keeps :oom")
    (is (= (:oom cell) (:oom (first (:results (runs/read-results dir "run1"))))))
    (let [res (sut/why! (ctx dir "run1" :traces []) {:test "hog-suite/w"})
          b (:ok res)]
      (is (r/ok? res) (pr-str res))
      (is (= "hog-suite/w OOM-killed at 256m" (get-in b [:why/cell :oom/line])))
      (is (= (* 256 1024 1024) (get-in b [:why/cell :oom/cap])))
      (is (str/includes? (:why/summary b) "hog-suite/w OOM-killed at 256m") (:why/summary b))
      (is (str/includes? (:why/markdown b) "hog-suite/w OOM-killed at 256m"))
      (is (str/includes? (slurp (get-in b [:why/files :md])) "OOM-killed at 256m")))))

(deftest why-without-traces-or-results
  (let [dir (temp-dir)]
    (is (= :compose/no-results (:error (sut/why! (ctx dir "run1") {:test "x"}))))
    (is (= :compose/missing-param (:error (sut/why! (ctx dir "run1") {}))))
    (is (= :compose/invalid-run (:error (sut/why! (ctx dir "run1") {:test "x" :run "Bad-Run"}))))
    (write-result! dir "run1")
    (testing "a trace store that fails leaves a gap, not an error"
      (let [c (ctx dir "run1")
            _ (stub-trace/fail! (:trace-query c) :find-traces)
            b (:ok (sut/why! c {:test "orders.place_boom_order"}))]
        (is (nil? (:why/failing-span b)))
        (is (= :stub/fault (get-in b [:why/gaps :traces 0 :error])))
        (is (seq (:why/logs b)) "logs still come")))
    (testing "the fallback query on the cell's test id finds the runner's spans"
      (let [t {:trace/id "t-cell"
               :trace/spans [(sp "suite" nil (+ t0 10) :error "case failed"
                                 {"hive.run.id" "run1" "test.id" "orders-suite/orders"})]}
            b (:ok (sut/why! (ctx dir "run1" :traces [t]) {:test "orders.place_boom_order"}))]
        (is (= "t-cell" (get-in b [:why/failing-span :trace/id])))
        (is (= {"hive.run.id" "run1" "test.id" "orders-suite/orders"} (get-in b [:why/trace-query :attrs])))))
    (testing "a note that throws is a gap"
      (let [b (:ok (sut/why! (ctx dir "run1" :note! (fn [_] (throw (ex-info "down" {}))))
                             {:test "orders.place_boom_order"}))]
        (is (= :compose/note-failed (get-in b [:why/gaps :note :error])))))))
