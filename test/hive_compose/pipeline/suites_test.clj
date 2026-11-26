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
      (is (= :container (:runner smoke)))
      (is (= "hive-test000001-shop" (:network smoke)))
      (is (nil? (:extra-networks smoke)) "a suite with no needs has no network of its own")
      (is (= suites/default-cell-timeout-ms (:timeout-ms smoke)) "a deadline is always set")
      (is (= {"hive.run" "test000001" "hive.consumer" "suite:smoke"} (:labels smoke)))
      (is (= {:pass "ci/token"} (get-in smoke [:env "TOKEN"])) "secret refs reach the runner unread"))
    (testing "a host command gets the wiring of its own needs and the correlation env"
      (is (= :command (:runner load)))
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
    (is (= [{:suite/id "smoke" :project "shop" :status :failed :exit 1 :reason :cases/failed :cases ["smoke/case-3"]}]
           (:failures summary))
        "smoke resolves a secret and the stub answers no :scrub: case ids are redacted to their position")
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

(deftest no-report-and-exit-zero-passes-only-when-no-report-is-declared
  (testing "a suite that declares nothing about reports: exit 0 is the verdict"
    (let [res (suites/test! (ctx {"load" {:exit 0}}) {:suites "load"})]
      (is (= [:passed 0] [(get-in res [:ok :status]) (get-in res [:ok :exit])]))))
  (testing "M1: a suite that declares its report format and leaves none could not run: :error, exit 2"
    (let [c (assoc-in (ctx {"load" {:exit 0}}) [:settings :compose/suites "load" :suite/report-format] :junit)
          res (suites/test! c {:suites "load"})
          cell (first (get-in res [:ok :cells]))]
      (is (= [:error 2] [(get-in res [:ok :status]) (get-in res [:ok :exit])]))
      (is (= :report/none (:reason cell)))
      (is (true? (get-in res [:ok :stack-kept?])) "kept for inspection")))
  (testing "a report with zero cases is :report/empty"
    (let [c (assoc-in (ctx {"load" {:exit 0 :raw "<testsuite name=\"load\"/>"}})
                      [:settings :compose/suites "load" :suite/report-format] :junit)
          cell (first (get-in (suites/test! c {:suites "load"}) [:ok :cells]))]
      (is (= [:error :report/empty] [(:status cell) (:reason cell)]))))
  (testing ":suite/report-required? false opts out"
    (let [c (-> (ctx {"load" {:exit 0}})
                (assoc-in [:settings :compose/suites "load" :suite/report-format] :junit)
                (assoc-in [:settings :compose/suites "load" :suite/report-required?] false))]
      (is (= :passed (get-in (suites/test! c {:suites "load"}) [:ok :status]))))))

(deftest the-exit-code-tells-failed-from-could-not-run
  (is (= 1 (get-in (suites/test! (ctx {"load" {:exit 1 :passed 0 :failed 1}}) {:suites "load"}) [:ok :exit])))
  (is (= 2 (get-in (suites/test! (ctx {"load" {:error :compose/secret-unresolved}}) {:suites "load"}) [:ok :exit])))
  (testing "2 wins over 1"
    (let [res (suites/test! (ctx {"smoke/shop" {:exit 1 :passed 0 :failed 1} "load" {:error :compose/runner-failed}}) {})]
      (is (= [:error 2] [(get-in res [:ok :status]) (get-in res [:ok :exit])])))))

(deftest skip-fails-fails-a-cell-with-a-skipped-case
  (let [raw (str "<testsuite name=\"load\"><testcase classname=\"c\" name=\"ok\"/>"
                 "<testcase classname=\"c\" name=\"later\"><skipped/></testcase></testsuite>")]
    (testing "default: a skipped case does not fail the cell"
      (is (= :passed (get-in (suites/test! (ctx {"load" {:exit 0 :raw raw}}) {:suites "load"}) [:ok :status]))))
    (testing "skip_fails on the call"
      (let [res (suites/test! (ctx {"load" {:exit 0 :raw raw}}) {:suites "load" :skip-fails? true})
            cell (first (get-in res [:ok :cells]))]
        (is (= [:failed 1] [(get-in res [:ok :status]) (get-in res [:ok :exit])]))
        (is (= :skipped-as-failure (:reason cell)))
        (is (= #{:passed :skipped} (set (map :case/status (get-in cell [:report :report/cases]))))
            "the case stays :skipped in the report")))
    (testing ":suite/skip-fails? on the suite; the call may turn it off"
      (let [c (assoc-in (ctx {"load" {:exit 0 :raw raw}}) [:settings :compose/suites "load" :suite/skip-fails?] true)]
        (is (= :failed (get-in (suites/test! c {:suites "load"}) [:ok :status])))
        (is (= :passed (get-in (suites/test! c {:suites "load" :skip-fails? false}) [:ok :status])))))))

(deftest variants-are-cells-of-their-own-on-one-stack
  (let [c (-> (ctx)
              (assoc-in [:settings :compose/suites "load" :suite/env] {"MODE" "x" "SISF_SYNC_REVERSE" "unset"})
              (assoc-in [:settings :compose/suites "load" :suite/variants]
                        {"reverse-off" {"SISF_SYNC_REVERSE" "false"} "reverse-on" {"SISF_SYNC_REVERSE" "true"}}))
        res (suites/test! c {:suites "load"})
        runs (sr/suite-runs (:suite-runner c))
        cells (get-in res [:ok :cells])]
    (is (= :passed (get-in res [:ok :status])) (pr-str res))
    (is (= ["load@reverse-off/shop" "load@reverse-on/shop"] (mapv :test-id runs)) "run one after the other")
    (is (= [["false" "x" "reverse-off"] ["true" "x" "reverse-on"]]
           (mapv (juxt #(get-in % [:env "SISF_SYNC_REVERSE"]) #(get-in % [:env "MODE"]) #(get-in % [:env "HIVE_VARIANT"])) runs))
        "the variant env goes over the suite's own, on the runner")
    (is (= ["load@reverse-off" "load@reverse-on"] (mapv :suite/id cells)) "the history and why key on <suite>@<variant>")
    (is (= ["reverse-off" "reverse-on"] (mapv :variant cells)))
    (is (= 1 (count (filter #(= "pg" (second %)) (ups c)))) "one stack for both")
    (testing "variants=a narrows; an unknown variant is named"
      (let [c2 (assoc c :suite-runner (sr/suite-runner {}))]
        (is (= ["load@reverse-on/shop"]
               (mapv :test-id (do (suites/test! c2 {:suites "load" :variants "reverse-on"})
                                  (sr/suite-runs (:suite-runner c2))))))
        (is (= {"load" ["nope"]} (:variants (suites/test! c2 {:suites "load" :variants "nope"}))))))
    (testing "R2-3: a selected suite with variants but not the named one refuses the call, nothing runs"
      (let [c3 (-> c
                   (assoc :suite-runner (sr/suite-runner {}))
                   (assoc-in [:settings :compose/suites "smoke" :suite/variants] {"only-off" {}}))
            res (suites/test! c3 {:suites "load,smoke" :variants "reverse-on"})]
        (is (= {"smoke" ["reverse-on"]} (:variants res)) (pr-str res))
        (is (empty? (sr/suite-runs (:suite-runner c3))))))
    (testing "a suite without variants under a variants filter runs once and says so"
      (let [c4 (assoc c :suite-runner (sr/suite-runner {}))
            res (suites/test! c4 {:suites "load,smoke" :projects "shop" :variants "reverse-on"})
            smoke (first (filter #(= "smoke" (:suite/id %)) (get-in res [:ok :cells])))]
        (is (= ["load@reverse-on/shop" "smoke/shop"] (sort (map :test-id (sr/suite-runs (:suite-runner c4))))))
        (is (string? (:variant-note smoke)))))))

(deftest r2-1-r2-2-no-false-green-from-the-report
  (testing "R2-1: a truncated JUnit with exit 0 could not run (exit 2)"
    (let [res (suites/test! (ctx {"load" {:exit 0 :raw "<testsuite name=\"load\"><testcase classname=\"c\" name=\"a\">"}})
                            {:suites "load"})]
      (is (= [:error 2] [(get-in res [:ok :status]) (get-in res [:ok :exit])]))
      (is (= :report/malformed (get-in res [:ok :cells 0 :reason])))))
  (testing "R2-2: a container suite that writes no report could not run (exit 2)"
    (let [c (assoc-in (ctx {"smoke/shop" {:exit 0}}) [:settings :compose/suites "smoke" :suite/env] {})
          res (suites/test! c {:suites "smoke" :projects "shop"})]
      (is (= [:error 2 :report/none] [(get-in res [:ok :status]) (get-in res [:ok :exit]) (get-in res [:ok :cells 0 :reason])]))
      (is (= :passed (get-in (suites/test! (assoc-in c [:settings :compose/suites "smoke" :suite/report-required?] false)
                                           {:suites "smoke" :projects "shop"})
                             [:ok :status]))
          "only an explicit false opts out"))))

(deftest the-answer-lists-every-case-and-why-a-cell-could-not-run
  (let [res (suites/test! (ctx {"smoke/shop" {:exit 1 :passed 2 :failed 1} "load" {:error :compose/secret-unresolved}}) {})
        by (into {} (map (juxt :test-id identity)) (get-in res [:ok :cells]))
        smoke (get by "smoke/shop")
        load (get by "load/shop")]
    (is (= 3 (count (get-in smoke [:report :report/cases]))) "passed cases too, not only failed ones")
    (is (every? #(and (:case/id %) (:case/status %) (contains? % :case/time-s)) (get-in smoke [:report :report/cases])))
    (is (= :error (:status load)))
    (is (= :compose/secret-unresolved (:reason load)))
    (testing "an OOM-killed cell names its service"
      (let [c (-> (ctx {"load" {:peak-mem (* 512 1024 1024)}})
                  (assoc-in [:settings :compose/suites "load" :suite/resources] {:mem "256m"}))
            cell (first (get-in (suites/test! c {:suites "load"}) [:ok :cells]))]
        (is (= :error (:status cell)) (pr-str cell))
        (is (= "load/shop" (:service cell)))))))

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

(deftest a-run-whose-every-report-file-was-skipped-says-which-and-why
  (let [c (ctx {"smoke/shop" {:exit 0 :raw "<failsafe-summary result=\"255\"><completed>4</completed></failsafe-summary>"}})
        res (suites/test! c {:suites "smoke" :projects "shop"})
        cell (first (get-in res [:ok :cells]))
        file (str (io/file (:reports-dir cell) "junit.xml"))]
    (is (r/ok? res) (pr-str res))
    (testing "the caller of `test` gets the report error, not no report at all"
      (is (= :report/none (get-in cell [:report-error :error])))
      (is (nil? (:report cell))))
    (testing "with each skipped file and the keyword reason it was skipped"
      (is (= [{:file file :reason :junit/unknown-root}] (get-in cell [:report-error :skipped-files])))
      (is (string? (get-in cell [:report-error :hint]))))
    (testing "a required report that skipped every file cannot run: the cell errors, the matrix too"
      (is (= :error (:status cell)))
      (is (= :report/none (:reason cell)) "never a silent pass")
      (is (= 2 (get-in res [:ok :exit]))))))

(defn- with-declared-dir
  "`c` whose load suite runs in `root` and declares `reports` under it."
  [c root reports]
  (-> c
      (assoc-in [:settings :compose/suites "load" :suite/dir] root)
      (assoc-in [:settings :compose/suites "load" :suite/reports-dir] reports)))

(deftest a-declared-host-reports-dir-is-stashed-then-collected
  (let [root (tmp-dir)
        dir (io/file root "build" "reports")
        old (io/file dir "old.xml")
        pom (io/file root "pom.xml")]
    (.mkdirs dir)
    (spit old (sr/junit "load" 0 3))
    (spit pom "<project/>")
    (testing "a leftover of an earlier run is MOVED to the cell's stale-reports before the cell, never read"
      (let [c (with-declared-dir (ctx {"load" {:exit 0}}) root "build/reports")
            res (suites/test! c {:suites "load"})
            cell (first (get-in res [:ok :cells]))
            stale (io/file (.getParent (io/file (:reports-dir cell))) "stale-reports" "declared" "old.xml")]
        (is (= [:error :report/none] [(get-in res [:ok :status]) (:reason cell)])
            "M1: the suite declares a reports dir and this attempt wrote none: could not run, never a silent pass")
        (is (not (.exists old)))
        (is (.isFile stale) "moved, never deleted")
        (is (.isFile pom) "nothing outside the declared dir is touched")
        (is (= 0 (get-in res [:ok :summary :cases :failed])))))
    (testing "what the runner writes there is moved into the cell's own dir and read"
      (let [c (with-declared-dir (ctx {"load" {:exit 1 :passed 1 :failed 1 :collect true}}) root "build/reports")
            res (suites/test! c {:suites "load"})
            cell (first (get-in res [:ok :cells]))]
        (is (= 1 (get-in res [:ok :summary :cases :failed])))
        (is (empty? (filter #(.isFile %) (file-seq dir))) "collected, so the next cell starts clean")
        (is (.isFile (io/file (:reports-dir cell) "junit.xml")))
        (is (.isFile pom))))))

(deftest an-unsafe-declared-reports-dir-is-refused
  (let [root (tmp-dir)
        pom (io/file root "pom.xml")]
    (spit pom "<project/>")
    (java.nio.file.Files/createSymbolicLink (.toPath (io/file root "self")) (.toPath (io/file root))
                                            (make-array java.nio.file.attribute.FileAttribute 0))
    (doseq [[why reports] [[:is-root "."] [:is-root "./"] [:link-escapes "self"]]]
      (testing (str why " " reports)
        (let [c (with-declared-dir (ctx) root reports)
              res (suites/test! c {:suites "load"})]
          (is (= :compose/unsafe-reports-dir (:error res)) (pr-str res))
          (is (= why (:reason res)))
          (is (empty? (ups c)) "nothing started")
          (is (.isFile pom)))))))

(deftest the-reports-dir-must-lie-strictly-inside-the-suite-dir
  (let [root (tmp-dir)]
    (is (= :is-root (:reason (hive-compose.boundary.report/safe-reports-dir root root))))
    (is (= :is-root (:reason (hive-compose.boundary.report/safe-reports-dir root (str root "/x/..")))))
    (is (= :ancestor (:reason (hive-compose.boundary.report/safe-reports-dir root (.getParent (io/file root))))))
    (is (= :outside (:reason (hive-compose.boundary.report/safe-reports-dir root (tmp-dir)))))
    (is (= :outside (:reason (hive-compose.boundary.report/safe-reports-dir root (str root "/../elsewhere")))))
    (is (r/ok? (hive-compose.boundary.report/safe-reports-dir root (str root "/target/reports"))))
    (testing "a link inside the suite dir that points at it, or out of it, is refused"
      (let [up (io/file root "up")
            out (io/file root "out")]
        (java.nio.file.Files/createSymbolicLink (.toPath up) (.toPath (io/file root)) (make-array java.nio.file.attribute.FileAttribute 0))
        (java.nio.file.Files/createSymbolicLink (.toPath out) (.toPath (io/file (tmp-dir))) (make-array java.nio.file.attribute.FileAttribute 0))
        (is (= :link-escapes (:reason (hive-compose.boundary.report/safe-reports-dir root (str up)))))
        (is (= :link-escapes (:reason (hive-compose.boundary.report/safe-reports-dir root (str out "/reports")))))))))

(deftest report-walks-never-follow-links
  (let [root (tmp-dir)
        elsewhere (tmp-dir)
        victim (io/file elsewhere "keep.xml")]
    (spit victim (sr/junit "x" 1 0))
    (java.nio.file.Files/createSymbolicLink (.toPath (io/file root "linked")) (.toPath (io/file elsewhere))
                                            (make-array java.nio.file.attribute.FileAttribute 0))
    (is (empty? (hive-compose.boundary.report/report-files root)))
    (is (r/ok? (hive-compose.boundary.report/stash-reports! root (str (tmp-dir) "/stash"))))
    (is (.isFile victim) "a linked dir is never entered")))

(deftest an-unmovable-leftover-stops-the-cell-loudly
  (let [root (tmp-dir)
        dir (io/file root "reports")
        sub (io/file dir "ro")
        old (io/file sub "old.xml")]
    (.mkdirs sub)
    (spit old (sr/junit "load" 0 3))
    (.setWritable sub false)
    (try
      (let [c (with-declared-dir (ctx {"load" {:exit 0}}) root "reports")
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
    (is (= :compose/run-busy (:error (suites/test! c {:suites "load" :run "busy001"}))))
    (testing "a dead test claim (a JVM that died mid-matrix) does not block reuse"
      (let [clock (atom 5000)
            c (assoc c :now #(deref clock))]
        (swap! clock + (* 31 60000))
        (is (= :passed (get-in (suites/test! c {:suites "load" :run "busy001"}) [:ok :status])))))))

(deftest two-reuses-of-one-run-never-both-run
  (let [c (-> (ctx)
              (assoc-in [:settings :compose/suites "api"] api-suite)
              (assoc-in [:settings :compose/stacks "shop"] {:stack/id "shop" :stack/consumers ["shop"]}))
        _ (stacks/stack-up! c {:stack "shop" :run "shop002"})
        inner (atom nil)
        ;; while the first matrix runs its cell, a second asks for the same run
        c (assoc c :record-run! (fn [cx _] (reset! inner (suites/test! cx {:suites "api" :projects "shop" :run "shop002"}))
                                  (r/ok {})))
        res (suites/test! c {:suites "api" :projects "shop" :run "shop002"})]
    (is (= :passed (get-in res [:ok :status])) (pr-str res))
    (is (= :compose/run-busy (:error @inner)) "the second is refused before it attaches anything")
    (is (= 1 (count (sr/suite-runs (:suite-runner c)))) "only the first ran a cell")
    (is (not (contains? (stub/networks (:engine c)) "hive-shop002-suite-api")) "the first still detached its network")))

(deftest a-run-taken-down-mid-matrix-is-never-resurrected
  (let [c (-> (ctx)
              (assoc-in [:settings :compose/suites "api"] api-suite)
              (assoc-in [:settings :compose/stacks "shop"] {:stack/id "shop" :stack/consumers ["shop"]}))
        _ (stacks/stack-up! c {:stack "shop" :run "shop003"})
        c (assoc c :record-run! (fn [cx _] (stacks/stack-down! cx {:run "shop003"}) (r/ok {})))
        res (suites/test! c {:suites "api" :projects "shop" :run "shop003"})]
    (is (r/ok? res) (pr-str res))
    (is (not (contains? (:stacks @(:state c)) "shop003")) "no ghost entry written back")))

(deftest a-throw-on-a-reused-run-still-detaches-its-networks
  (let [c (-> (ctx)
              (assoc-in [:settings :compose/suites "api"] api-suite)
              (assoc-in [:settings :compose/stacks "shop"] {:stack/id "shop" :stack/consumers ["shop"]}))
        _ (stacks/stack-up! c {:stack "shop" :run "shop004"})
        n (atom 0)
        ;; saves 1..2 are the claim and the attach; the third, a lease, throws
        c (assoc c :save! (fn [_] (when (= 3 (swap! n inc)) (throw (ex-info "disk full" {})))))
        res (suites/test! c {:suites "api" :projects "shop" :run "shop004"})]
    (is (= :compose/test-threw (:error res)) (pr-str res))
    (is (not (contains? (stub/networks (:engine c)) "hive-shop004-suite-api")) "detached in the finally")
    (is (not-any? #{"hive-shop004-suite-api"} (get-in @(:state c) [:stacks "shop004" :networks])))
    (is (empty? (get-in @(:state c) [:stacks "shop004" :claims])) "claim released")))

(deftest an-interrupt-stops-the-matrix
  (let [c (assoc (ctx) :record-run! (fn [_ _] (.interrupt (Thread/currentThread)) (r/ok {})))
        res (try (suites/test! c {}) (finally (Thread/interrupted)))
        cells (get-in res [:ok :cells])]
    (is (r/ok? res) (pr-str res))
    (is (= 1 (count (sr/suite-runs (:suite-runner c)))) "no cell starts after the interrupt")
    (is (= [:compose/interrupted :compose/interrupted] (keep #(get-in % [:runner-error :error]) cells)))))

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
    (is (= 51 (count (:report/cases rep))) "every case by id, status and time")
    (is (every? #(= #{:case/id :case/status :case/time-s} (set (keys %))) (:report/cases rep)) "no texts in the case list")
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

(deftest every-free-text-is-scrubbed-or-redacted
  (let [secret "s3cr3t-tok"
        junit (str "<testsuite name=\"load\"><testcase classname=\"c." secret "\" name=\"t\">"
                   "<failure message=\"m\"/></testcase></testsuite>")
        tap (str "TAP version 13\n1..2\nnot ok 1 - x\nBail out! token " secret "\n")
        broken (str "<testsuite name=\"load\"><testcase name=\"" secret)]
    (testing "with the runner's :scrub: classname, a TAP bail-out, a parser error message"
      (doseq [raw [junit tap broken]]
        (let [c (ctx {"load" {:exit 1 :raw raw :scrub {secret "pass:ci/token"}}})
              res (suites/test! c {:suites "load"})]
          (is (r/ok? res) (pr-str res))
          (is (not (str/includes? (pr-str res) secret)) (str "answer: " raw))
          (is (not (str/includes? (slurp (get-in res [:ok :summary-file])) secret)))
          (is (not (str/includes? (pr-str @(:recorded c)) secret))))))
    (testing "with no :scrub but refs in the env: ids, names, classnames and the output tail go"
      (doseq [raw [junit tap broken]]
        (let [c (-> (ctx {"load" {:exit 1 :raw raw :output [(str "TOKEN=" secret)]}})
                    (assoc-in [:settings :compose/suites "load" :suite/env] {"TOKEN" {:pass "ci/token"}}))
              res (suites/test! c {:suites "load"})]
          (is (not (str/includes? (pr-str res) secret)) (str "answer: " raw))
          (is (not (str/includes? (pr-str @(:recorded c)) secret))))))
    (testing "a runner error's message is scrubbed too"
      (let [c (ctx {"load" {:error :compose/runner-failed :throws (str " " secret) :scrub {secret "pass:ci/token"}}})
            res (suites/test! c {:suites "load"})]
        (is (not (str/includes? (pr-str res) secret)))
        (is (str/includes? (pr-str res) "pass:ci/token"))))))

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
           (= exit ({:passed 0 :failed 1 :error 2} status))
           (= (not all-pass?) stack-kept? (boolean (seq (:stacks @(:state c)))))
           (every? #(= (:test-id %) (str (:suite/id %) "/" (:project %))) cells)))))
