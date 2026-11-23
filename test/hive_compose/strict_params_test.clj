(ns hive-compose.strict-params-test
  "Card 20261123144701-428fd846: a call whose param names the command does not
   take is refused before anything runs (MCP dispatch and the control API, one
   check), `test` with no selection is refused unless all=true, its answer
   starts with the plan, and a matrix that ran no cell takes its own stack
   down. Stub engine, stub suite runner, no redefs."
  (:require [clojure.data.json :as json]
            [clojure.test :refer [deftest is testing]]
            [hive-addon.protocol :as addon]
            [hive-compose.addon :as sut]
            [hive-compose.api :as api]
            [hive-compose.pipeline.stacks :as stacks]
            [hive-compose.pipeline.suites :as suites]
            [hive-compose.promote.sync]
            [hive-compose.stub :as stub]
            [hive-compose.stub-runner :as sr]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- temp-dir []
  (str (java.nio.file.Files/createTempDirectory "hive-compose-strict"
                                                (make-array java.nio.file.attribute.FileAttribute 0))))

(def ^:private projects
  {"pg" {:project/id "pg" :project/dir "/w/pg" :project/provides {"postgres" {:service "db"}}}
   "funsync" {:project/id "funsync" :project/dir "/w/funsync" :project/needs {"postgres" {}}}
   "funpay" {:project/id "funpay" :project/dir "/w/funpay" :project/needs {"postgres" {}}}})

(def ^:private world {"pg" {"db" {}} "funsync" {"sync" {}} "funpay" {"pay" {}}})

(def ^:private suite-defs
  [{:suite/id "sync" :suite/command "make e2e" :suite/projects ["funsync"]}
   {:suite/id "pay" :suite/command "make e2e" :suite/projects ["funpay"]}])

(defn- make [dir engine runner]
  (sut/make-addon {:compose/profiles [{:profile/id "api" :profile/dir "/w/shop"}]
                   :compose/profiles-file (str dir "/absent.edn")
                   :compose/state-file (str dir "/state.edn")
                   :compose/engine engine
                   :compose/suite-runner runner
                   :compose/swarm? false
                   :compose/initial-delay-ms 3600000
                   :compose/projects projects
                   :compose/suites suite-defs}))

(defn- call [a params]
  (let [{:keys [content isError]} ((:handler (first (addon/tools a))) params)]
    {:error? (boolean isError)
     :body (json/read-str (:text (first content)) :key-fn keyword)}))

(defmacro ^:private with-addon [[a engine runner] & body]
  `(let [dir# (temp-dir)
         ~engine (stub/engine {} {} world)
         ~runner (sr/suite-runner {})
         ~a (make dir# ~engine ~runner)]
     (addon/initialize! ~a {})
     (try ~@body (finally (addon/shutdown! ~a)))))

(deftest the-incident-call-is-refused-and-starts-nothing
  (with-addon [a engine runner]
    (let [res (call a {:command "test" :suite "sync" :project "funsync"})]
      (is (:error? res))
      (is (= "invalid-param" (get-in res [:body :error])))
      (is (= ["project" "suite"] (get-in res [:body :unknown])))
      (is (= {:project "projects" :suite "suites"} (get-in res [:body :did-you-mean])))
      (is (= (vec (sort (get sut/command-params "test"))) (get-in res [:body :valid])))
      (is (empty? (stub/calls engine)) "the stub engine saw ZERO calls")
      (is (empty? (sr/suite-runs runner))))))

(deftest one-unknown-key-is-refused-on-every-command
  ;; generated over the command table: no command ignores a key it does not take
  (with-addon [a engine runner]
    (doseq [c sut/commands
            bogus ["bogus_key" "suite" "tragets"]
            :when (not (contains? (get sut/command-params c) bogus))]
      (let [res (call a {"command" c bogus "x"})]
        (is (:error? res) (str c " " bogus))
        (is (= "invalid-param" (get-in res [:body :error])) (str c " " bogus))
        (is (= [bogus] (get-in res [:body :unknown])) (str c " " bogus))))
    (is (empty? (stub/calls engine)) "no command reached the engine")
    (is (empty? (sr/suite-runs runner)))))

(deftest the-api-runs-the-same-check
  (let [declared (get-in (sut/tool nil) [:inputSchema :properties])]
    (doseq [c sut/commands]
      (let [res (api/check-params declared (get sut/command-params c) {"zzz_unknown" 1} c)]
        (is (= :compose/invalid-param (:error res)) c)
        (is (= ["zzz_unknown"] (:unknown res)) c)))
    (is (= {"project" "projects" "suite" "suites"}
           (:did-you-mean (api/check-params declared (get sut/command-params "test")
                                            {"suite" "sync" "project" "funsync"} "test"))))
    (is (= 400 (api/http-status (api/unknown-params "test" #{"suites"} {"suite" "x"}))))))

(deftest host-keys-are-accepted-everywhere
  (let [host {"agent_id" "ling-1" "directory" "/tmp" "async" false "async-timeout-ms" 10
              "_meta" {} "_caller_id" "c" "_caller_cwd" "/tmp" "_caller_credential" "k"}]
    (is (every? api/host-param? (keys host)))
    (doseq [c sut/commands]
      (is (r/ok? (api/unknown-params c (get sut/command-params c) host)) c)
      (is (r/ok? (api/unknown-params c (get sut/command-params c)
                                     (into {} (map (fn [[k v]] [(keyword k) v])) host)))
          (str c " with keyword keys"))))
  (with-addon [a _engine _runner]
    (is (not (:error? (call a {"command" "status" "agent_id" "ling-1" "_caller_id" "c"
                               "_caller_cwd" "/tmp" "directory" "/tmp" "_meta" {}}))))))

(deftest nearest-suggests-by-prefix-or-edit-distance
  (is (= "suites" (api/nearest "suite" ["projects" "run" "suites"])))
  (is (= "projects" (api/nearest "project" ["projects" "run" "suites"])))
  (is (= "run" (api/nearest "runn" ["projects" "run" "suites"])))
  (is (nil? (api/nearest "zzzzzz" ["projects" "run" "suites"]))))

(deftest test-without-a-selection-is-refused-unless-all
  (with-addon [a engine runner]
    (let [res (call a {"command" "test"})]
      (is (:error? res))
      (is (= "missing-param" (get-in res [:body :error])))
      (is (= {:projects ["funpay" "funsync"] :suites ["pay" "sync"] :cells 2}
             (get-in res [:body :would-run])))
      (is (re-find #"all=true" (get-in res [:body :hint])))
      (is (empty? (filter #(#{:up-project :network-create} (first %)) (stub/calls engine))) "nothing started"))
    (testing "dry_run without a selection plans and starts nothing"
      (let [res (call a {"command" "test" "dry_run" true "run" "dry001"})]
        (is (not (:error? res)) (pr-str res))
        (is (= ["funpay" "funsync"] (get-in res [:body :plan :projects])))
        (is (= 2 (get-in res [:body :plan :cells])))
        (is (empty? (filter #(= :up-project (first %)) (stub/calls engine))))))
    (testing "all=true runs the whole matrix"
      (let [res (call a {"command" "test" "all" true "run" "all001"})]
        (is (not (:error? res)) (pr-str res))
        (is (= 2 (count (sr/suite-runs runner))))
        (is (= {:projects ["funpay" "funsync"] :suites ["pay" "sync"] :cells 2 :services 3}
               (get-in res [:body :plan])))))))

(defn- ctx []
  (let [hex (atom 0)]
    {:engine (stub/engine {} {} world)
     :suite-runner (sr/suite-runner {})
     :settings {:compose/projects projects :compose/profiles {}
                :compose/suites (into {} (map (juxt :suite/id identity)) suite-defs)
                :compose/default-ttl-minutes 60}
     :state (atom {:active {} :current nil})
     :now (constantly 5000)
     :state-dir (temp-dir)
     :rand-hex #(format "%06x" (swap! hex inc))
     :id-source (fn [n] (apply str (repeat (* 2 n) "a")))}))

(defn- ups [c] (filter #(= :up-project (first %)) (stub/calls (:engine c))))

(deftest the-plan-comes-first-and-counts-what-starts
  (let [c (ctx)
        dry (suites/test! c {:suites "sync" :dry-run? true})]
    (is (= {:projects ["funsync"] :suites ["sync"] :cells 1 :services 2}
           (select-keys (get-in dry [:ok :plan]) [:projects :suites :cells :services]))
        "pg/db and funsync's sync")
    (is (empty? (ups c))))
  (let [seen (atom nil)
        c (ctx)
        c (assoc c :progress! (fn [cx line] (reset! seen [line (count (ups cx))])))
        res (suites/test! c {:suites "sync,pay"})]
    (is (= {:projects ["funpay" "funsync"] :suites ["pay" "sync"] :cells 2 :services 3} (get-in res [:ok :plan])))
    (is (= :plan (first (keys (:ok res)))) "the plan is the first key of the answer")
    (is (= [(get-in res [:ok :plan]) 0] @seen) "the plan line goes out before anything starts")
    (is (= 3 (count (ups c))) "pg, funsync, funpay: as counted")))

(deftest a-matrix-that-ran-no-cell-takes-its-stack-down
  (testing "a :before-suite error: no cell ran, the run it started goes down"
    (let [c (assoc (ctx) :data-phase! (fn [_ _ _] (r/err :data/seed-failed {})))
          res (suites/test! c {:suites "sync"})]
      (is (= [:error 2] [(get-in res [:ok :status]) (get-in res [:ok :exit])]))
      (is (true? (get-in res [:ok :no-cell-ran?])))
      (is (false? (get-in res [:ok :stack-kept?])))
      (is (empty? (:stacks @(:state c))))
      (is (some #(= [:down-project "pg"] (take 2 %)) (stub/calls (:engine c))))))
  (testing "the incident: the state lost the run before the first phase; it is put back and taken down"
    (let [c (assoc (ctx) :data-phase! (fn [cx run _]
                                        (swap! (:state cx) update :stacks dissoc run)
                                        (r/err :compose/unknown-run {:run run})))
          res (suites/test! c {:suites "sync"})]
      (is (true? (get-in res [:ok :run-restored?])))
      (is (false? (get-in res [:ok :stack-kept?])) (pr-str res))
      (is (empty? (:stacks @(:state c))))
      (is (= #{} (stub/running (:engine c) "pg")))))
  (testing "keep_up keeps it"
    (let [c (assoc (ctx) :data-phase! (fn [_ _ _] (r/err :data/seed-failed {})))
          res (suites/test! c {:suites "sync" :keep-up? true})]
      (is (true? (get-in res [:ok :stack-kept?])))
      (is (= 1 (count (:stacks @(:state c)))))))
  (testing "another agent's live claim keeps it"
    (let [c (assoc (ctx) :data-phase! (fn [cx run _] (stacks/claim! cx run "ling-7" {}) (r/err :data/seed-failed {})))
          res (suites/test! c {:suites "sync"})]
      (is (true? (get-in res [:ok :stack-kept?])))
      (is (= ["ling-7"] (get-in res [:ok :claimed-by])))))
  (testing "a reused run is never taken down by this rule"
    (let [c (assoc-in (ctx) [:settings :compose/stacks "s"] {:stack/id "s" :stack/consumers ["funsync"]})
          _ (stacks/stack-up! c {:stack "s" :run "reuse001"})
          c (assoc c :data-phase! (fn [_ _ _] (r/err :data/seed-failed {})))
          res (suites/test! c {:suites "sync" :run "reuse001"})]
      (is (true? (get-in res [:ok :stack-kept?])) (pr-str res))
      (is (contains? (:stacks @(:state c)) "reuse001")))))

(deftest a-state-file-without-stacks-never-drops-a-run
  ;; a writer that predates :stacks saves a file with no :stacks key: that is
  ;; no opinion on the runs, never a removal of one this side left unchanged
  (let [run {:stack/id "s" :run "r1" :last-touch 1 :claims {"test:r1:x" {:since 1 :seen 1}}}
        base {:active {} :current nil :stacks {"r1" run}}
        mine base
        theirs {:active {} :current nil}]
    (is (= {"r1" run} (:stacks (hive-compose.promote.sync/merge-state base mine theirs)))))
  (testing "an explicit removal still lands"
    (let [run {:stack/id "s" :run "r1" :last-touch 1}
          base {:active {} :current nil :stacks {"r1" run}}]
      (is (= {} (:stacks (hive-compose.promote.sync/merge-state base base {:active {} :current nil :stacks {}})))))))
