(ns hive-compose.pipeline.suites-test
  "The suite x project matrix run on the stub engine and the stub suite
   runner: one union stack, a runner per cell, JUnit read back, results
   recorded, down on success, kept on failure. test.check: the summary and
   the stack's fate follow the cells' outcomes, whatever they are."
  (:require [clojure.edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.boundary.runs :as runs]
            [hive-compose.pipeline.stacks :as stacks]
            [hive-compose.pipeline.suites :as suites]
            [hive-compose.stub :as stub]
            [hive-compose.stub-runner :as sr]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def projects
  {"pg" {:project/id "pg" :project/dir "/w/pg"
         :project/provides {"postgres" {:service "db" :version "16" :env {"PGHOST" "db"}}}}
   "shop" {:project/id "shop" :project/dir "/w/shop" :project/needs {"postgres" {}}}
   "billing" {:project/id "billing" :project/dir "/w/billing"}})

(def world {"pg" {"db" {}} "shop" {"api" {}} "billing" {"billing" {}}})

(def suite-defs
  {"smoke" {:suite/id "smoke" :suite/image "ghcr.io/x/smoke:1" :suite/projects ["shop" "billing"]
            :suite/env {"TOKEN" {:pass "ci/token"}}}
   "load" {:suite/id "load" :suite/command ["k6" "run" "x.js"] :suite/projects ["shop"]
           :suite/needs {"postgres" {}}}})

(defn- tmp-dir []
  (str (doto (java.nio.file.Files/createTempDirectory "hc8-suites" (make-array java.nio.file.attribute.FileAttribute 0))
         (-> .toFile .deleteOnExit))))

(defn- ctx
  ([] (ctx {}))
  ([scripts]
   (let [recorded (atom []) hex (atom 0)]
     {:engine (stub/engine {} {} world)
      :suite-runner (sr/suite-runner scripts)
      :settings {:compose/projects projects :compose/profiles {} :compose/suites suite-defs
                 :compose/default-ttl-minutes 60}
      :state (atom {:active {} :current nil})
      :now (constantly 5000)
      :state-dir (tmp-dir)
      :rand-hex #(format "%06x" (swap! hex inc))
      :id-source (fn [n] (apply str (repeat (* 2 n) "a")))
      :recorded recorded
      :record-run! (fn [_ run] (swap! recorded conj run) (r/ok {:run (:run/id run) :appended 1}))})))

(defn- ups [c] (filter #(= :up-project (first %)) (stub/calls (:engine c))))

(deftest every-cell-passes-and-the-stack-goes-down
  (let [c (ctx)
        res (suites/test! c {})]
    (is (r/ok? res) (pr-str res))
    (let [{:keys [run status exit summary cells stack-kept?]} (:ok res)]
      (is (= "test000001" run))
      (is (= [:passed 0] [status exit]))
      (is (= 3 (:cells summary)) "smoke x {billing shop}, load x shop")
      (is (= [["load" "shop"] ["smoke" "billing"] ["smoke" "shop"]] (mapv (juxt :suite/id :project) cells)))
      (is (= 3 (get-in summary [:cases :passed])) "the JUnit fixtures were read back")
      (is (false? stack-kept?))
      (is (empty? (:stacks @(:state c))) "taken down")
      (is (= 3 (count @(:recorded c))) "every cell recorded in the flake history")
      (is (every? :report @(:recorded c))))
    (testing "one union stack: pg once, both projects under test as consumers, no suite started"
      (is (= #{"pg" "shop-test000001" "billing-test000001"} (set (map second (ups c)))))
      (is (= 1 (count (filter #(= "pg" (second %)) (ups c))))))))

(deftest runners-get-their-network-reports-and-correlation
  (let [c (ctx)
        _ (suites/test! c {:suites "smoke,load" :projects "shop"})
        runs (sr/suite-runs (:suite-runner c))
        by (into {} (map (juxt :test-id identity)) runs)
        smoke (get by "smoke/shop")
        load (get by "load/shop")]
    (is (= 2 (count runs)))
    (testing "a container joins the view network of the project under test"
      (is (= :container (::sr/kind smoke)))
      (is (= "hive-test000001-shop" (:network smoke)))
      (is (nil? (:extra-networks smoke)) "a suite with no needs has no network of its own")
      (is (= suites/default-cell-timeout-ms (:timeout-ms smoke)) "a deadline is always set")
      (is (= {"hive.run" "test000001" "hive.consumer" "suite:smoke"} (:labels smoke)))
      (is (= {:pass "ci/token"} (get-in smoke [:env "TOKEN"])) "secret refs reach the runner unread"))
    (testing "a host command gets the wiring of its own needs and the correlation env"
      (is (= :command (::sr/kind load)))
      (is (= "db" (get-in load [:env "PGHOST"])))
      (is (= "load/shop/" (get-in load [:env "HIVE_TEST_ID_PREFIX"])))
      (is (= "test000001" (get-in load [:env "HIVE_RUN_ID"])))
      (is (re-matches #"00-a{32}-a{16}-01" (get-in load [:env "TRACEPARENT"]))))))

(deftest a-failing-cell-keeps-the-stack-up
  (let [c (ctx {"smoke/shop" {:exit 1 :passed 2 :failed 1}})
        res (suites/test! c {})
        {:keys [run status exit summary stack-kept? hint]} (:ok res)]
    (is (r/ok? res))
    (is (= [:failed 1] [status exit]))
    (is (= [{:suite/id "smoke" :project "shop" :status :failed :exit 1 :cases ["smoke/c.bad0"]}]
           (:failures summary)))
    (is (true? stack-kept?))
    (is (contains? (:stacks @(:state c)) run) "kept for inspection")
    (is (re-find #"stack-down run=test000001" hint))))

(deftest a-runner-that-dies-is-an-error-cell
  (let [c (ctx {"load" {:error :compose/secret-unresolved}})
        res (suites/test! c {:suites ["load"]})
        cell (first (get-in res [:ok :cells]))]
    (is (= :error (get-in res [:ok :status])))
    (is (= :error (:status cell)))
    (is (= :compose/secret-unresolved (get-in cell [:runner-error :error])))
    (is (= :report/none (get-in cell [:report-error :error])))))

(deftest no-report-and-exit-zero-passes
  (let [res (suites/test! (ctx {"load" {:exit 0}}) {:suites "load"})]
    (is (= :passed (get-in res [:ok :status])))))

(deftest keep-up-keeps-a-passing-stack
  (let [c (ctx)
        res (suites/test! c {:keep-up? true})]
    (is (= :passed (get-in res [:ok :status])))
    (is (true? (get-in res [:ok :stack-kept?])))
    (is (= 1 (count (:stacks @(:state c)))))))

(deftest dry-run-starts-nothing
  (let [c (ctx)
        res (suites/test! (dissoc c :suite-runner) {:dry-run? true :run "dry001"})]
    (is (r/ok? res) (pr-str res))
    (is (= "dry001" (get-in res [:ok :run])))
    (is (= 3 (count (get-in res [:ok :runners]))))
    (is (= "pass:ci/token" (get-in (first (filter #(= "smoke/shop" (:test-id %)) (get-in res [:ok :runners])))
                                   [:env "TOKEN"])) "references rendered, never read")
    (is (some #(= "pg/postgres" (:instance %)) (get-in res [:ok :plan :instances])))
    (is (empty? (ups c)))
    (is (empty? (sr/suite-runs (:suite-runner c))))))

(deftest refusals-start-nothing
  (let [c (ctx)]
    (is (= :compose/unknown-names (:error (suites/test! c {:suites "nope"}))))
    (is (= :compose/empty-matrix (:error (suites/test! c {:suites "load" :projects "billing"}))))
    (is (= :compose/invalid-run (:error (suites/test! c {:run "Bad-Run"}))))
    (is (= :compose/no-suite-runner (:error (suites/test! (dissoc c :suite-runner) {}))))
    (is (empty? (ups c)))))

(deftest a-stack-that-does-not-come-up-is-kept-and-named
  (let [c (ctx)]
    (stub/health! (:engine c) "pg" "db" :unhealthy)
    (let [res (suites/test! c {})]
      (is (r/err? res))
      (is (= "test000001" (:run res)))
      (is (empty? (sr/suite-runs (:suite-runner c))) "no runner ran")
      (is (contains? (:stacks @(:state c)) "test000001")))))

(def api-suite
  "A container suite that talks to what it needs: it runs on its own view network."
  {:suite/id "api" :suite/image "ghcr.io/x/api:1" :suite/projects ["billing" "shop"]
   :suite/needs {"postgres" {}} :suite/env {"BILLING_URL" "http://billing:8080"}})

(deftest a-container-suite-joins-the-project-network-and-its-own
  (let [c (assoc-in (ctx) [:settings :compose/suites "api"] api-suite)
        res (suites/test! c {:suites "api" :projects "billing" :keep-up? true})
        spec (first (sr/suite-runs (:suite-runner c)))]
    (is (= :passed (get-in res [:ok :status])) (pr-str res))
    (is (= "hive-test000001-billing" (:network spec)) "created on the project under test's view network")
    (is (= ["hive-test000001-suite-api"] (:extra-networks spec)) "and joins the suite consumer's own")
    (is (= "http://billing:8080" (get-in spec [:env "BILLING_URL"])) "service URLs from :suite/env")
    (is (= "db" (get-in spec [:env "PGHOST"])) "its needs from the suite consumer's wiring")
    (is (contains? (get-in (stub/networks (:engine c)) ["hive-test000001-suite-api" :members]) "pg-db-1")
        "what it needs joined its network")
    (is (r/ok? (hive-compose.pipeline.stacks/stack-down! c {:run "test000001"})))
    (is (empty? (stub/networks (:engine c))) "its networks go with the run")))

(deftest an-existing-run-is-reused-and-never-taken-down
  (let [c (-> (ctx)
              (assoc-in [:settings :compose/suites "api"] api-suite)
              (assoc-in [:settings :compose/stacks "shop"] {:stack/id "shop" :stack/consumers ["shop"]}))
        up (hive-compose.pipeline.stacks/stack-up! c {:stack "shop" :run "shop001"})
        _ (is (r/ok? up) (pr-str up))
        before (set (map second (ups c)))
        res (suites/test! c {:suites "api" :projects "shop" :run "shop001"})]
    (is (r/ok? res) (pr-str res))
    (is (= [:passed true true] [(get-in res [:ok :status]) (get-in res [:ok :reused?]) (get-in res [:ok :stack-kept?])]))
    (is (= before (set (map second (ups c)))) "nothing new came up: the run's pg serves the suite")
    (is (= ["hive-shop001-shop" ["hive-shop001-suite-api"]]
           ((juxt :network :extra-networks) (first (sr/suite-runs (:suite-runner c))))))
    (is (= :compose/project-not-in-run (:error (suites/test! c {:suites "api" :projects "billing" :run "shop001"}))))
    (is (contains? (:stacks @(:state c)) "shop001") "the reused run stays")
    (is (not (contains? (stub/networks (:engine c)) "hive-shop001-suite-api")) "its suite network is gone")
    (is (not-any? #(= [:down-project "pg"] (take 2 %)) (stub/calls (:engine c))))
    (testing "a reused run that lacks what the suites need is refused, nothing ran"
      (let [c2 (-> (ctx)
                   (assoc-in [:settings :compose/suites "api"] api-suite)
                   (assoc-in [:settings :compose/stacks "bill"] {:stack/id "bill" :stack/consumers ["billing"]}))
            _ (hive-compose.pipeline.stacks/stack-up! c2 {:stack "bill" :run "bill001"})
            res2 (suites/test! c2 {:suites "api" :projects "billing" :run "bill001"})]
        (is (= :compose/suite-needs-not-in-run (:error res2)) (pr-str res2))
        (is (empty? (sr/suite-runs (:suite-runner c2))))))))

(deftest data-phases-run-before-between-and-after-suites
  (let [calls (atom [])
        c (assoc (ctx) :data-phase! (fn [_ run phase] (swap! calls conj [run phase]) {:ok? true}))
        res (suites/test! c {})]
    (is (= :passed (get-in res [:ok :status])))
    (is (= [["test000001" :before-suite] ["test000001" :between-suites] ["test000001" :after]] @calls)
        "two suites: one between")
    (testing "a failing phase fails the matrix and keeps the stack"
      (let [c (assoc (ctx) :data-phase! (fn [_ _ phase] (if (= phase :after) (r/err :data/reset-failed {}) :ok)))
            res (suites/test! c {})]
        (is (= :error (get-in res [:ok :status])))
        (is (= :after (get-in res [:ok :data-errors 0 :phase])))
        (is (true? (get-in res [:ok :stack-kept?])))))
    (testing "a failing :before-suite runs no cell"
      (let [c (assoc (ctx) :data-phase! (fn [_ _ _] (r/err :data/seed-failed {})))
            res (suites/test! c {})]
        (is (= :error (get-in res [:ok :status])))
        (is (empty? (sr/suite-runs (:suite-runner c))))))))

(deftest a-rerun-never-reads-the-previous-attempts-reports
  (let [c (ctx {"load" {:exit 1 :passed 0 :failed 2}})
        first-run (suites/test! c {:suites "load" :run "ci001"})]
    (is (= :failed (get-in first-run [:ok :status])))
    (sr/script! (:suite-runner c) {"load" {:exit 0}})
    (let [again (suites/test! c {:suites "load" :run "ci001"})
          cell (first (get-in again [:ok :cells]))]
      (is (r/ok? again) (pr-str again))
      (is (true? (get-in again [:ok :reused?])) "the kept run is reused")
      (is (nil? (:report cell)) "the old junit.xml was cleared, not read")
      (is (= :report/none (get-in cell [:report-error :error])))
      (is (= :passed (:status cell)) "exit 0 with no report is the verdict"))))

(deftest a-declared-host-reports-dir-is-emptied-then-collected
  (let [dir (tmp-dir)
        old (io/file dir "old.xml")]
    (spit old (sr/junit "load" 0 3))
    (testing "a leftover of an earlier run is deleted before the cell, never read"
      (let [c (-> (ctx {"load" {:exit 0}})
                  (assoc-in [:settings :compose/suites "load" :suite/reports-dir] dir))
            res (suites/test! c {:suites "load"})]
        (is (= :passed (get-in res [:ok :status])) (pr-str res))
        (is (not (.exists old)))
        (is (= 0 (get-in res [:ok :summary :cases :failed])))))
    (testing "what the runner writes there is moved into the cell's own dir and read"
      (let [c (-> (ctx {"load" {:exit 1 :passed 1 :failed 1 :collect true}})
                  (assoc-in [:settings :compose/suites "load" :suite/reports-dir] dir))
            res (suites/test! c {:suites "load"})
            cell (first (get-in res [:ok :cells]))]
        (is (= 1 (get-in res [:ok :summary :cases :failed])))
        (is (empty? (filter #(.isFile %) (file-seq (io/file dir)))) "collected, so the next cell starts clean")
        (is (.isFile (io/file (:reports-dir cell) "junit.xml")))))))

(deftest an-undeletable-leftover-stops-the-cell-loudly
  (let [dir (tmp-dir)
        sub (io/file dir "ro")
        old (io/file sub "old.xml")]
    (.mkdirs sub)
    (spit old (sr/junit "load" 0 3))
    (.setWritable sub false)
    (try
      (let [c (-> (ctx {"load" {:exit 0}})
                  (assoc-in [:settings :compose/suites "load" :suite/reports-dir] dir))
            res (suites/test! c {:suites "load"})
            cell (first (get-in res [:ok :cells]))]
        (when (.exists old) ;; root may delete anyway
          (is (= :error (:status cell)))
          (is (= :report/unclearable (get-in cell [:runner-error :error])))
          (is (empty? (sr/suite-runs (:suite-runner c))) "the runner never ran on top of it")))
      (finally (.setWritable sub true)))))

(deftest the-reaper-never-takes-a-running-matrix
  (let [clock (atom 0)
        reaped (atom [])
        c (-> (ctx {"load/shop" {:exit 1 :passed 0 :failed 1}})
              (assoc :now #(deref clock))
              (assoc-in [:settings :compose/claim-ttl-minutes] 1)
              (assoc-in [:settings :compose/orphan-ttl-minutes] 1)
              (assoc-in [:settings :compose/default-ttl-minutes] 1))
        ;; the data phase and the cell each take 20 minutes of clock, far past
        ;; every TTL, and run the reaper while they do
        slow (fn [cx] (swap! clock + (* 20 60000)) (swap! reaped into (:reaped (stacks/reap! cx))))
        c (assoc c :data-phase! (fn [cx _ _] (slow cx) :ok)
                   :record-run! (fn [cx _] (slow cx) (r/ok {})))
        res (suites/test! c {:suites "load"})
        run (get-in res [:ok :run])]
    (is (r/ok? res) (pr-str res))
    (is (empty? @reaped) "nothing reaped while the matrix ran")
    (is (= :failed (get-in res [:ok :status])))
    (is (contains? (:stacks @(:state c)) run) "kept for inspection")
    (is (empty? (get-in @(:state c) [:stacks run :claims])) "test!'s claim released at the end")
    (is (= @clock (get-in @(:state c) [:stacks run :last-touch])) "touched when the matrix ended")
    (testing "after a full idle TTL the kept run goes"
      (swap! clock + (* 2 60000))
      (is (= [run] (mapv :run (:reaped (stacks/reap! c))))))))

(deftest a-throw-anywhere-keeps-the-run-named-and-releases-its-claim
  (testing "a cell that throws is an :error cell; the others still run"
    (let [c (ctx {"load" {:throws true}})
          res (suites/test! c {})
          cells (get-in res [:ok :cells])]
      (is (r/ok? res) (pr-str res))
      (is (= :error (:status (first (filter #(= "load" (:suite/id %)) cells)))))
      (is (= :compose/runner-threw (get-in (first cells) [:runner-error :error])))
      (is (= 2 (count (filter #(= :passed (:status %)) cells))))
      (is (empty? (get-in @(:state c) [:stacks (get-in res [:ok :run]) :claims])))))
  (testing "a save that throws mid-matrix answers an error naming the run"
    (let [n (atom 0)
          c (assoc (ctx) :save! (fn [_] (when (> (swap! n inc) 3) (throw (ex-info "disk full" {}))) (r/ok nil)))
          res (suites/test! c {})]
      (is (r/err? res))
      (is (= "test000001" (or (:run res) (get-in res [:ok :run]))))
      (is (empty? (get-in @(:state c) [:stacks "test000001" :claims]))))))

(deftest only-an-up-run-is-reused
  (let [c (ctx)]
    (stub/health! (:engine c) "pg" "db" :unhealthy)
    (is (r/err? (suites/test! c {:suites "load" :run "part001"})))
    (is (= :partial (get-in @(:state c) [:stacks "part001" :status])))
    (let [res (suites/test! c {:suites "load" :run "part001"})]
      (is (= :compose/run-not-up (:error res)))
      (is (empty? (sr/suite-runs (:suite-runner c)))))))

(deftest a-run-another-matrix-works-on-is-refused
  (let [c (ctx)]
    (is (r/ok? (suites/test! c {:suites "load" :run "busy001" :keep-up? true})))
    (stacks/claim! c "busy001" "test:busy001" {})
    (is (= :compose/run-busy (:error (suites/test! c {:suites "load" :run "busy001"}))))))

(deftest a-foreign-claim-keeps-a-passing-stack-up
  (let [c (assoc (ctx) :record-run! (fn [cx run] (stacks/claim! cx (:run/id run) "ling-7" {}) (r/ok {})))
        res (suites/test! c {:suites "load"})]
    (is (= :passed (get-in res [:ok :status])))
    (is (true? (get-in res [:ok :stack-kept?])))
    (is (= ["ling-7"] (get-in res [:ok :claimed-by])))))

(deftest the-summary-and-each-result-are-written-to-the-run-dir
  (let [c (ctx {"smoke/shop" {:exit 1 :passed 1 :failed 1}})
        res (suites/test! c {})
        run (get-in res [:ok :run])
        read (runs/read-results (:state-dir c) run)]
    (is (.isFile (io/file (get-in res [:ok :summary-file]))))
    (is (= :failed (:status (clojure.edn/read-string (slurp (get-in res [:ok :summary-file]))))))
    (is (= 3 (count (:results read))) "why reads every cell's result.edn")
    (is (empty? (:skipped read)))))

(deftest the-answer-carries-only-failed-cases-clipped
  (let [big (apply str (repeat 5000 "x"))
        raw (str "<testsuite name=\"load\">"
                 (apply str (repeat 50 "<testcase classname=\"c\" name=\"ok\"/>"))
                 "<testcase classname=\"c\" name=\"bad\"><failure message=\"m\"/><system-out>" big "</system-out></testcase>"
                 "</testsuite>")
        c (ctx {"load" {:exit 1 :raw raw}})
        res (suites/test! c {:suites "load"})
        rep (:report (first (get-in res [:ok :cells])))]
    (is (= 50 (get-in rep [:report/totals :passed])))
    (is (= 1 (count (:report/failed-cases rep))))
    (is (nil? (:report/cases rep)))
    (is (< (count (:case/output (first (:report/failed-cases rep)))) 2100))
    (is (= 5000 (count (:case/output (first (filter #(= "bad" (:case/name %))
                                                    (get-in (first @(:recorded c)) [:report :report/cases]))))))
        "the history keeps the full text")))

(deftest a-runner-without-scrub-has-its-texts-redacted
  (let [secret "s3cr3t-tok"
        raw (str "<testsuite name=\"load\"><testcase classname=\"c\" name=\"t\"><failure message=\"" secret "\"/>"
                 "<system-out>" secret "</system-out></testcase></testsuite>")
        c (-> (ctx {"load" {:exit 1 :raw raw}})
              (assoc-in [:settings :compose/suites "load" :suite/env] {"TOKEN" {:pass "ci/token"}}))
        res (suites/test! c {:suites "load"})]
    (is (= :failed (get-in res [:ok :status])))
    (is (not (str/includes? (pr-str res) secret)))
    (is (not (str/includes? (pr-str @(:recorded c)) secret)))
    (is (true? (get-in res [:ok :cells 0 :report :report/redacted?])))))

(deftest secrets-in-reports-never-reach-the-answer-or-the-history
  (let [secret "s3cr3t-tok"
        raw (str "<testsuite name=\"load\"><testcase classname=\"c\" name=\"test_auth[" secret "]\">"
                 "<failure message=\"bad token " secret "\"/>"
                 "<system-out>TOKEN=" secret "</system-out></testcase></testsuite>")
        c (ctx {"load" {:exit 1 :raw raw :scrub {secret "pass:ci/token"}}})
        res (suites/test! c {:suites "load"})]
    (is (= :failed (get-in res [:ok :status])))
    (is (not (str/includes? (slurp (get-in res [:ok :summary-file])) secret)) "summary.edn scrubbed")
    (is (not (str/includes? (pr-str res) secret)) "answer scrubbed")
    (is (not (str/includes? (pr-str @(:recorded c)) secret)) "flake history scrubbed")
    (is (str/includes? (pr-str res) "pass:ci/token"))))

(def outcome-gen
  (gen/one-of [(gen/fmap (fn [[p f]] {:exit (if (pos? f) 1 0) :passed p :failed f})
                         (gen/tuple (gen/choose 0 3) (gen/choose 0 2)))
               (gen/return {:exit 0})
               (gen/return {:exit 2})
               (gen/return {:error :compose/runner-timeout})]))

(defspec summary-and-stack-fate-follow-the-cells 25
  (prop/for-all [outs (gen/vector outcome-gen 3)]
    (let [ids ["load/shop" "smoke/billing" "smoke/shop"]
          c (ctx (zipmap ids outs))
          res (suites/test! c {})
          {:keys [status exit cells stack-kept?]} (:ok res)
          all-pass? (every? #(= :passed (:status %)) cells)]
      (and (r/ok? res)
           (= 3 (count cells) (count @(:recorded c)))
           (= all-pass? (= :passed status) (zero? exit))
           (= (not all-pass?) stack-kept? (boolean (seq (:stacks @(:state c)))))
           (every? #(= (:test-id %) (str (:suite/id %) "/" (:project %))) cells)))))
