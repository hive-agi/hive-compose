(ns hive-compose.pipeline.suites
  "The suite x project matrix run (hc-8), over the context `hive-compose.pipeline.ops`
   takes, plus `:suite-runner` (a `hive-compose.port/ISuiteRunner`).

     test!  expand the matrix (promote.suite), bring up ONE stack for it, the
            union of the projects under test and the suites' own needs
            (hc-6 stack-up! with :extra), run every cell's runner through
            ISuiteRunner under its resource caps (hc-16b), read its reports
            (boundary.report), fold the cell result, record it in the flake
            history (hc-13, when present), then the matrix summary. A matrix
            that passed is taken down unless :keep-up?; a failed one stays up
            for inspection (the reaper TTL still applies, and `stack-down
            run=<run>` releases it).

   Optional context keys: `:id-source` (fn [n-bytes]) -> 2n hex chars for
   trace ids, `:rand-hex` (0-arity, 6 hex chars) for run ids, `:state-dir`
   (default: the state file's directory), `:otlp-env` (default
   `promote.otel/otlp-env` when the stack binds an otel instance, else none),
   `:record-run!` (fn [ctx cell-result]) -> Result, the flake history
   (hc-13), none when absent, `:data-phase!` (fn [ctx run phase]) (hc-9
   `pipeline.data/run-phase!`), and `:default-resources`, the caps of a suite
   that declares none (`:compose/default-resources`).
   Each cell's full result goes to `<state>/runs/<run>/<suite>/<project>/result.edn`
   and the matrix answer to `<state>/runs/<run>/summary.edn` (boundary.runs).

   Every entry point answers a Result and never throws. An interrupt (a
   cancelled call) stops the matrix: the cells not yet started answer
   :compose/interrupted, and the flag is restored for the caller."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [hive-compose.boundary.report :as report]
            [hive-compose.boundary.runs :as runs]
            [hive-compose.pipeline.stacks :as stacks]
            [hive-compose.port :as port]
            [hive-compose.promote.env :as env]
            [hive-compose.promote.otel :as otel]
            [hive-compose.promote.stack-run :as stack-run]
            [hive-compose.promote.suite :as suite]
            [hive-compose.schema :as schema]
            [hive-dsl.result :as r]
            [malli.core :as m])
  (:import (java.security SecureRandom)))

;; SPDX-License-Identifier: MIT

(def run-prefix
  "Generated test run ids are `test<6 hex>`."
  "test")

(def claim-agent-prefix
  "test! holds the run it works on under its own claim `test:<run>:<nonce>`
   (one nonce per matrix, so two matrices on one run never renew or drop each
   other's claim) from the instant stack-up! records the run (or the reuse
   claim) to the end, its lease (`:seen`) renewed before every cell and data
   phase to cover that step's deadline, so the reaper never takes a matrix
   down mid-run. `claim-exclusive!` refuses a reuse while any live claim of
   this prefix holds the run."
  "test:")

(defn- hex [n]
  (let [b (byte-array n)]
    (.nextBytes (SecureRandom.) b)
    (apply str (map #(format "%02x" (bit-and % 0xff)) b))))

(defn claim-agent
  "The claim id of one matrix on `run`: `test:<run>:<nonce>`."
  [run nonce]
  (str claim-agent-prefix run ":" nonce))

(defn- agent-of
  "The claim id this matrix holds its run under (set by test!)."
  [ctx]
  (::agent ctx))

(defn- default-id-source [n] (hex n))

(defn- state-dir [ctx]
  (or (:state-dir ctx)
      (some-> (get-in ctx [:settings :compose/state-file]) io/file .getParentFile str)
      (str (System/getProperty "java.io.tmpdir") "/hive-compose")))

(defn- interrupted? [] (.isInterrupted (Thread/currentThread)))

(defn- caught!
  "Restore the interrupt flag a caught InterruptedException cleared; answers `t`."
  [t]
  (when (instance? InterruptedException t) (.interrupt (Thread/currentThread)))
  t)

(defn- pick-run
  "Result<{:run RunId :reuse? bool}>: `run` when given (a recorded run is
   reused, which `claim-reuse!` checks atomically before anything attaches),
   else a fresh `test<hex>`."
  [ctx run]
  (let [rid (or run (stack-run/run-id run-prefix ((or (:rand-hex ctx) #(hex 3)))))
        entry (get-in @(:state ctx) [:stacks rid])]
    (if-not (m/validate schema/RunId rid)
      (r/err :compose/invalid-run {:run rid :problems (schema/explain schema/RunId rid)})
      (r/ok {:run rid :reuse? (some? entry)}))))

(defn- suites-of [ctx] (get-in ctx [:settings :compose/suites] {}))
(defn- projects-of [ctx] (get-in ctx [:settings :compose/projects] {}))

(defn- with-matrix-stack
  "`ctx` whose settings hold the matrix stack value under its id, so hc-6
   resolves it as any configured stack."
  [ctx stack]
  (assoc-in ctx [:settings :compose/stacks (:stack/id stack)] stack))

(defn- binds-otel? [resolution]
  (some #(= otel/service (:instance/capability %)) (vals (:resolution/instances resolution))))

(defn- spec-ctx
  "The `runner-spec` context of a run: suites, ids, state dir, OTLP, the
   default resource caps."
  [ctx run resolution]
  {:suites (suites-of ctx)
   :run run
   :state-dir (state-dir ctx)
   :id-source (or (:id-source ctx) default-id-source)
   :default-resources (or (:default-resources ctx) (get-in ctx [:settings :compose/default-resources]))
   :otlp-env (if (contains? ctx :otlp-env)
               (:otlp-env ctx)
               (when (binds-otel? resolution) otel/otlp-env))})

(def default-cell-timeout-ms
  "A cell's deadline when its suite sets no :suite/timeout-ms: what the claim
   lease covers and the runner bounds the run by."
  (* 30 60 1000))

(defn- checked-collect-from
  "Result<spec>: a host suite's declared reports dir (`:collect-from`)
   canonical and strictly inside its `:suite/dir` (boundary.report
   `safe-reports-dir`), else :compose/unsafe-reports-dir."
  [ctx spec]
  (if-let [from (:collect-from spec)]
    (let [root (get-in (suites-of ctx) [(get-in spec [:cell :suite/id]) :suite/dir])]
      (r/map-ok (report/safe-reports-dir root from) #(assoc spec :collect-from %)))
    (r/ok spec)))

(defn- cell-spec
  "Result<RunnerSpec> of `cell`: promote.suite's (a container on the project
   under test's view network, plus `:extra-networks` [the suite consumer's]
   when the suite has needs; a command's declared reports dir as
   `:collect-from`, checked safe), with the suite consumer's wiring env, a
   deadline always set, and the cell's `:verdict-policy`
   (promote.suite/verdict-policy of its suite and the call's `skip-fails?`)."
  [ctx sctx wirings skip-fails? cell]
  (let [w (get wirings (suite/suite-consumer (:suite/id cell)))]
    (r/let-ok [spec (suite/runner-spec (assoc sctx :wiring-env (:wiring/env w)) cell)
               spec (checked-collect-from ctx spec)]
      (r/ok (-> spec
                (update :timeout-ms (fnil identity default-cell-timeout-ms))
                (assoc :verdict-policy (suite/verdict-policy (get (suites-of ctx) (:suite/id cell)) skip-fails?)))))))

(defn- all-ok [f xs]
  (reduce (fn [acc x] (let [y (f x)] (if (r/err? y) (reduced y) (r/map-ok acc #(conj % (:ok y))))))
          (r/ok []) xs))

(defn- run-stack
  "The Stack value an existing run `entry` was planned from: the configured
   stack when there is one, else its container consumers (a matrix stack)."
  [ctx entry]
  (or (get-in ctx [:settings :compose/stacks (:stack/id entry)])
      {:stack/id (:stack/id entry)
       :stack/consumers (vec (sort (filter stack-run/container-consumer?
                                           (keys (get-in entry [:plan :stack/consumers])))))}))

(defn- plan-matrix
  "Result<{:run :reuse? :stack :cells :extra :resolution :wirings :plan :specs :ctx}>:
   all that is known before anything starts. With `run` naming a live stack
   run, the matrix runs on that run's stack (`:reuse?`), which must run every
   project under test."
  [ctx {:keys [suites projects variants run skip-fails?]}]
  (r/let-ok [cells (suite/expand-matrix (suites-of ctx) (projects-of ctx)
                                        {:suites suites :projects projects :variants variants})
             {rid :run reuse? :reuse?} (pick-run ctx run)
             stack (if reuse?
                     (let [s (run-stack ctx (get-in @(:state ctx) [:stacks rid]))
                           missing (vec (sort (remove (set (:stack/consumers s)) (distinct (map :project cells)))))]
                       (if (seq missing)
                         (r/err :compose/project-not-in-run
                                {:run rid :projects missing
                                 :hint "the reused run does not run these projects; omit run for a stack of their own"})
                         (r/ok s)))
                     (r/ok (suite/matrix-stack rid cells)))]
    (let [extra (into (sorted-map)
                      (filter (fn [[c _]] (re-find #"^suite:" c)))
                      (suite/union-needs (suites-of ctx) (projects-of ctx) cells))
          ctx' (with-matrix-stack ctx stack)]
      (r/let-ok [{:keys [resolution wirings plan]} (stacks/resolve-plan ctx' (:stack/id stack) rid extra)
                 specs (all-ok #(cell-spec ctx (spec-ctx ctx rid resolution) wirings skip-fails? %) cells)]
        (r/ok {:run rid :reuse? reuse? :stack stack :cells cells :extra extra :resolution resolution
               :wirings wirings :plan plan :specs specs :ctx ctx'})))))

(defn- shown-spec
  "A RunnerSpec as shown: secret references rendered."
  [spec]
  (update spec :env env/rendered))

(defn- persist! [{:keys [state save!]}]
  (when save! (save! (select-keys @state [:active :current :stacks :preexisting]))))

(defn- suite-consumer? [c] (boolean (re-find #"^suite:" (str c))))

;; ---------------------------------------------------------------------------
;; texts: scrub, redact, clip

(def ^:private scrubbed-case-keys
  "Every text a case carries: a parametrised test id or name (pytest
   `test_auth[<token>]`) is as likely a carrier as its output."
  [:case/id :case/name :case/classname :case/message :case/output])

(defn- scrub-err
  "Error map `e` with its free text (:message :stderr) passed through `scrub`
   when there is one, else dropped (`redact?`) or kept."
  [e scrub redact?]
  (reduce (fn [e k]
            (cond
              (not (string? (get e k))) e
              scrub (update e k scrub)
              redact? (assoc e k "[redacted]")
              :else e))
          e [:message :stderr]))

(defn- scrub-report
  "`report` (a Result<Report>) with every text of every case and the
   bail-out reason passed through `scrub`, the runner's own (closed over the
   values it resolved). An error report has its message scrubbed: the XML
   parser's message quotes report content."
  [report scrub]
  (cond
    (r/ok? report)
    (-> report
        (update-in [:ok :report/cases]
                   (fn [cs] (mapv (fn [c] (reduce (fn [c k] (cond-> c (string? (get c k)) (update k scrub)))
                                                  c scrubbed-case-keys))
                                  cs)))
        (update :ok #(cond-> % (string? (:report/bail-out %)) (update :report/bail-out scrub))))
    :else (scrub-err report scrub false)))

(defn- redact-report
  "`report` with every case's free text dropped and its id, name and
   classname replaced by `<suite>/case-<n>`, the bail-out reason and an
   error's message dropped: a runner that resolved secrets but answered no
   :scrub cannot be trusted with any text it left."
  [report suite-id]
  (if (r/ok? report)
    (-> report
        (update-in [:ok :report/cases]
                   (fn [cs] (vec (map-indexed (fn [i c]
                                                (let [n (str suite-id "/case-" (inc i))]
                                                  (assoc c :case/id n :case/name n :case/classname nil
                                                         :case/message nil :case/output nil)))
                                              cs))))
        (update :ok #(cond-> (dissoc % :report/bail-out) (:report/bail-out %) (assoc :report/bail-out "[redacted]")))
        (assoc-in [:ok :report/redacted?] true))
    (scrub-err report nil true)))

(def answer-text-chars
  "How much of a case's message or output the answer carries; the full text
   stays in the cell's reports dir and in its result.edn."
  2000)

(def answer-failed-cases
  "How many failed cases a cell lists in the answer; the count of the rest
   is given, and every case stays in result.edn."
  50)

(def answer-line-chars
  "How long a line of a runner's output tail may be in the answer."
  500)

(defn- clip
  ([s] (clip s answer-text-chars))
  ([s n] (if (and (string? s) (> (count s) n)) (str (subs s 0 n) "...") s)))

;; ---------------------------------------------------------------------------
;; the reused run: claim, attach, detach

(defn- claim-reuse!
  "Result: test!'s claim on reused run `run`, written in ONE swap BEFORE any
   suite network attaches, iff the run is recorded, :up and no other LIVE
   test claim holds it (a dead one, from a JVM that died mid-matrix, is taken
   over)."
  [ctx run ms]
  (stacks/claim-exclusive! ctx run (agent-of ctx)
                           {:note "compose test" :prefix claim-agent-prefix
                            :seen (+ ((:now ctx)) (long ms))}))

(defn- lease!
  "Result: test!'s claim on `run` renewed so it stays live for `ms` from now
   (its `:seen` set ahead, as stack-run/claim-live? reads it), in one write
   under the stack lock. An error when the run is gone (taken down
   meanwhile): nothing is written then."
  [ctx run ms]
  (r/map-ok (stacks/claim! ctx run (agent-of ctx) {:note "compose test" :seen (+ ((:now ctx)) (long ms))})
            (constantly run)))

;; ---------------------------------------------------------------------------
;; one cell

(defn- run-runner [runner spec]
  (try
    (port/-run! runner spec)
    (catch Throwable t
      (caught! t)
      (r/err :compose/runner-threw {:exit nil :message (str t)}))))

(defn- record!
  "{:recorded ..} or {:record-error ..} through the injected `:record-run!`
   (fn [ctx cell-result]) -> Result (hc-13 `pipeline.flaky/record-run!`),
   nil when none is wired. The whole cell result goes, but for its output."
  [ctx result scrub]
  (when-let [f (:record-run! ctx)]
    (let [res (try (f ctx (dissoc result :output))
                   (catch Throwable t (caught! t) (r/err :compose/record-threw {:message (str t)})))]
      (if (r/ok? res)
        {:recorded (:ok res)}
        {:record-error (scrub-err (select-keys res [:error :message :hint]) scrub false)}))))

(defn- prepare-dirs!
  "Result: the cell's own reports dir cleared, and its declared host dir
   (`:collect-from`) too: every report file is MOVED into
   `<run-dir>/stale-reports` (never deleted), loudly: a report file that
   cannot be moved stops the attempt rather than be read as this one's."
  [spec]
  (let [stale (str (.getParent (io/file (:reports-dir spec))) "/stale-reports")]
    (r/let-ok [_ (report/stash-reports! (:reports-dir spec) (str stale "/own"))
               _ (if-let [from (:collect-from spec)] (report/stash-reports! from (str stale "/declared")) (r/ok nil))]
      (r/ok spec))))

(def ^:private kept-error-keys [:error :message :reason :var :hint :exit :leftovers :run :dir :root :stderr])

(defn- cell-error
  "The :error cell result of `spec` that could not run: `err` names why."
  [spec err t0 t1 scrub]
  (-> (suite/cell-result (:run spec) (:cell spec)
                         {:exit nil :report (r/err :report/none {}) :started-at t0 :finished-at t1
                          :policy (:verdict-policy spec)})
      (assoc :test-id (:test-id spec) :trace-id (:trace-id spec) :reports-dir (:reports-dir spec)
             :status :error
             :reason (or (:error err) :compose/runner-failed)
             :runner-error (scrub-err (select-keys err kept-error-keys) scrub (seq (env/refs (:env spec)))))))

(defn- run-cell!
  "The cell result of `spec` (promote.suite cell-result, :oom carried) plus
   what the run left behind: :test-id :trace-id :output, :cap and
   :timed-out?, and :runner-error when the runner could not run it to the
   end. Before the runner: the claim lease is renewed over the cell's
   deadline (a run gone meanwhile is an :error cell) and the reports dirs are
   cleared; after: a declared host dir is collected into the cell's own,
   then read. Case texts are scrubbed with the runner's :scrub (redacted when
   the spec resolves secrets and the runner gives none) and clipped in the
   answer. Never throws: a throw is an :error cell. The full result is
   written to `<run-dir>/result.edn` (boundary.runs, read by `why`)."
  [ctx spec]
  (let [t0 ((:now ctx))
        scrub (atom nil)]
    (try
      (let [ready (r/let-ok [_ (lease! ctx (:run spec) (+ (:timeout-ms spec default-cell-timeout-ms)
                                                         (* 10 60 1000)))]
                    (prepare-dirs! spec))]
        (if (r/err? ready)
          (cell-error spec ready t0 ((:now ctx)) nil)
          (let [out (run-runner (:suite-runner ctx) spec)
                ok (when (r/ok? out) (:ok out))
                sc (or (:scrub ok) (:scrub out))
                _ (reset! scrub sc)
                redact? (and (nil? sc) (seq (env/refs (:env spec))))
                collected (if-let [from (:collect-from spec)]
                            (report/collect-reports! from (:reports-dir spec))
                            (r/ok nil))
                raw (if (r/err? collected) collected (report/read-reports (:reports-dir spec) (:report-format spec :auto)))
                rep (cond
                      sc (scrub-report raw sc)
                      redact? (redact-report raw (get-in spec [:cell :suite/id]))
                      :else raw)
                result (suite/cell-result (:run spec) (:cell spec)
                                          {:exit (:exit ok)
                                           :report rep
                                           :oom (:oom ok)
                                           :started-at (or (:started-at ok) t0)
                                           :finished-at (or (:finished-at ok) ((:now ctx)))
                                           :timed-out? (boolean (:timed-out? ok))
                                           :policy (:verdict-policy spec)})
                tail (when-not redact? (:output ok))
                result (cond-> (assoc result :test-id (:test-id spec) :trace-id (:trace-id spec)
                                      :reports-dir (:reports-dir spec))
                         (seq tail) (assoc :output (vec tail))
                         (:cap ok) (assoc :cap (:cap ok))
                         (:timed-out? ok) (assoc :timed-out? true)
                         (r/err? out) (assoc :runner-error (scrub-err (select-keys out kept-error-keys) sc redact?))
                         ;; could not run: the runner's own category is the reason
                         (and (r/err? out) (= :error (:status result))) (assoc :reason (:error out)))
                written (runs/write-result! (state-dir ctx) (dissoc result :output :runner-error))]
            (cond-> (merge result (record! ctx result sc))
              (r/err? written) (assoc :result-error (select-keys written [:error :problems :message]))))))
      (catch Throwable t
        (caught! t)
        (cell-error spec {:error :compose/cell-threw :message (str t)} t0 ((:now ctx)) @scrub)))))

(defn- interrupted-cell
  "The :error cell result of `spec` not started because the call was interrupted."
  [ctx spec]
  (let [t ((:now ctx))]
    (cell-error spec {:error :compose/interrupted :hint "the call was cancelled; this cell never started"} t t nil)))

;; ---------------------------------------------------------------------------
;; test!

(defn- unclaim!
  "Drop this matrix's claim on `run` and touch it (stacks/end-claim!, under
   the stack lock): a run kept for inspection gets a full idle TTL from the
   end of the matrix, not from its stack-up. A run that is gone (taken down)
   is left alone."
  [ctx run]
  (stacks/end-claim! ctx run (agent-of ctx) {:down? false}))

(def answer-cases
  "How many cases a cell lists by id, status and time in the answer
   (`:report/cases`); the rest are counted in `:report/cases-more`, and every
   case stays in result.edn."
  2000)

(defn- service-of
  "The service a could-not-run cell names, when one is known: the OOM-killed
   service, else the instance or compose project the runner error names."
  [result]
  (or (get-in result [:oom :oom/service])
      (get-in result [:runner-error :instance])
      (get-in result [:runner-error :compose-project])))

(defn- answer-cell
  "`result` as the answer carries it: the report cut to its totals, every
   case as {:case/id :case/status :case/time-s} (up to `answer-cases`;
   `:report/cases-more` counts the rest), and up to `answer-failed-cases`
   failed/errored cases with their texts (`:report/failed-more` counts the
   rest), texts and names clipped; the output tail with its lines clipped. A
   cell that could not run (:error) carries `:reason` and, when known,
   `:service`. The full report stays in the reports dir and result.edn."
  [result]
  (let [svc (when (= :error (:status result)) (service-of result))
        result (cond-> result
                 (:output result) (update :output (fn [ls] (mapv #(clip % answer-line-chars) ls)))
                 svc (assoc :service svc)
                 (and (= :error (:status result)) (nil? (:reason result)))
                 (assoc :reason (or (get-in result [:runner-error :error]) (get-in result [:report-error :error]))))]
    (if-let [rep (:report result)]
      (let [cases (:report/cases rep)
            failed (filter #(#{:failed :error} (:case/status %)) cases)
            shown (take answer-failed-cases failed)
            more (- (count failed) (count shown))
            listed (take answer-cases cases)
            cases-more (- (count cases) (count listed))]
        (assoc result :report
               (cond-> {:report/format (:report/format rep)
                        :report/totals (:report/totals rep)
                        :report/cases (vec (for [c listed]
                                             {:case/id (clip (:case/id c) answer-line-chars)
                                              :case/status (:case/status c)
                                              ;; nil when the report gives no time
                                              :case/time-s (:case/time-s c)}))
                        :report/failed-cases (vec (for [c shown]
                                                    (-> c
                                                        (update :case/message clip)
                                                        (update :case/output clip)
                                                        (update :case/name clip answer-line-chars)
                                                        (update :case/id clip answer-line-chars))))}
                 (pos? cases-more) (assoc :report/cases-more cases-more)
                 (pos? more) (assoc :report/failed-more more)
                 (:report/bail-out rep) (assoc :report/bail-out (clip (:report/bail-out rep)))
                 (:report/redacted? rep) (assoc :report/redacted? true))))
      result)))

(defn- finish!
  "The answer for `results` and `phases`, also written to
   `<state>/runs/<run>/summary.edn`. The matrix ends its claim in ONE step
   under the stack lock (stacks/end-claim!): drop its own claim, re-read the
   claims left, and take a run this call created down when every cell
   passed, every data phase answered ok, `keep-up?` is not set and no other
   claim holds it; a reused run is never taken down. `:exit` is
   promote.suite/matrix-exit: 0 passed, 1 failed, 2 could not run."
  [ctx {:keys [run stack-id reuse? keep-up? skip-fails? variants?]} results phases]
  (let [summary (suite/matrix-summary results)
        phase-errors (vec (for [p phases :when (r/err? (:result p))]
                            {:phase (:phase p) :after-suite (:after-suite p)
                             :error (select-keys (:result p) [:error :message :hint])}))
        status (if (seq phase-errors) :error (:status summary))
        down-wanted? (and (= :passed status) (not keep-up?) (not reuse?))
        {others :others down :down} (stacks/end-claim! ctx run (agent-of ctx) {:down? down-wanted?})
        down? (and down-wanted? (empty? others))
        kept? (not (and down? (some? down) (r/ok? down)))
        answer (cond-> {:run run :stack stack-id :reused? (boolean reuse?)
                        :status status :exit (suite/matrix-exit status)
                        :summary (dissoc summary :status :exit)
                        :cells (mapv #(cond-> (answer-cell %)
                                        ;; a variants filter leaves a suite without variants alone
                                        (and variants? (nil? (:variant %)))
                                        (assoc :variant-note "the suite declares no variants: it ran once, unfiltered"))
                                     results)
                        :stack-kept? kept?}
                 (some? skip-fails?) (assoc :skip-fails? skip-fails?)
                 (seq phases) (assoc :data-phases (mapv #(-> % (dissoc :result) (assoc :ok? (r/ok? (:result %)))) phases))
                 (seq phase-errors) (assoc :data-errors phase-errors)
                 (and down (r/ok? down)) (assoc :down (:ok down))
                 (and down (r/err? down)) (assoc :down-error (select-keys down [:error :errors :hint]))
                 (seq others) (assoc :claimed-by others)
                 (and kept? (contains? (:stacks @(:state ctx)) run))
                 (assoc :hint (str "the stack stays up"
                                   (cond reuse? " (a reused run is never taken down)"
                                         (seq others) (str " (claimed by " (str/join ", " others) ")")
                                         (not= :passed status) (str " for inspection (" (name status) ")")
                                         :else "")
                                   "; stack-down run=" run " releases it, the reaper after its TTL")))
        written (runs/write-summary! (state-dir ctx) run answer)]
    (r/ok (if (r/ok? written)
            (assoc answer :summary-file (:ok written))
            (assoc answer :summary-error (select-keys written [:error :message]))))))

(defn- phase!
  "{:phase :after-suite? :result Result} of the injected data hook (hc-9)
   `(:data-phase! ctx)` (fn [ctx run phase]), a no-op when absent."
  [ctx run phase after-suite]
  (let [f (:data-phase! ctx)
        res (if f
              (try (let [x (f ctx run phase)] (if (r/err? x) x (r/ok x)))
                   (catch Throwable t (caught! t) (r/err :compose/data-phase-threw {:message (str t)})))
              (r/ok nil))]
    (cond-> {:phase phase :result res} after-suite (assoc :after-suite after-suite))))

(def phase-lease-ms
  "The claim lease a data phase runs under."
  (* 30 60 1000))

(defn- run-cells!
  "[results phases]: every spec's cell run, suite by suite (specs are sorted
   by suite), with the data hook :before-suite before the first suite,
   :between-suites between two suites and :after at the end, when it is
   given; the claim lease is renewed before each, and a phase whose run is
   gone (its lease refused) does not run. A :before-suite error runs no cell;
   a run gone meanwhile answers every remaining cell :error; an interrupt
   answers every cell not yet started :compose/interrupted."
  [ctx run specs]
  ;; one group per suite, and per variant of a suite with variants: a data
  ;; phase :between-suites runs between two variants as between two suites
  (let [groups (partition-by #(suite/cell-key (:cell %)) specs)
        hook? (some? (:data-phase! ctx))
        phase (fn [p after]
                (let [l (lease! ctx run phase-lease-ms)]
                  (if (r/err? l)
                    (cond-> {:phase p :result l} after (assoc :after-suite after))
                    (phase! ctx run p after))))
        before (when hook? (phase :before-suite nil))]
    (if (and before (r/err? (:result before)))
      [[] [before]]
      (loop [gs groups results [] phases (if before [before] [])]
        (if-let [g (first gs)]
          (let [rs (mapv #(if (interrupted?) (interrupted-cell ctx %) (run-cell! ctx %)) g)
                sid (suite/cell-key (:cell (first g)))
                between (when (and hook? (next gs) (not (interrupted?))) (phase :between-suites sid))]
            (recur (rest gs) (into results rs) (cond-> phases between (conj between))))
          [results (cond-> phases (and hook? (not (interrupted?))) (conj (phase :after nil)))])))))

(defn- on-run!
  "Run the cells of `specs` on live run `run`, already claimed (stack-up!, or
   `claim-reuse!`), so the reaper cannot take it while the data phases and
   cells run. The claim, and every suite network in atom `created` (the
   attach of a reused run), are released in a finally, whatever happens."
  [ctx info specs created]
  (let [run (:run info)
        detached (atom nil)
        detach! #(when (seq @created)
                   (let [nets @created]
                     (reset! created [])
                     (reset! detached (stacks/detach-extra! ctx run nets))))]
    (try
      ;; claimed by stack-up! (in the swap that recorded the run) or
      ;; claim-reuse!; the lease is renewed per step, inside the try, so a
      ;; throw in a lease still reaches the finally
      (lease! ctx run phase-lease-ms)
      (let [[results phases] (run-cells! ctx run specs)]
        (detach!)
        (r/map-ok (finish! ctx info results phases)
                  #(cond-> % @detached (assoc :detach-error (select-keys @detached [:error :stderr])))))
      (finally
        (try (detach!) (catch Throwable t (caught! t) nil))
        (try (unclaim! ctx run) (catch Throwable t (caught! t) nil))))))

(defn- on-reused-run!
  "Claim reused run `run` (one swap, before anything attaches), attach the
   suite networks of `plan` (stacks/attach-extra!, under the stack lock), then
   `on-run!`. A failing attach detaches what it created and drops the claim."
  [ctx info plan specs]
  (let [run (:run info)
        created (atom [])]
    (r/let-ok [_ (claim-reuse! ctx run phase-lease-ms)]
      (let [attached (try (stacks/attach-extra! ctx run plan suite-consumer? created)
                          (catch Throwable t (caught! t) (r/err :compose/attach-threw {:message (str t)})))]
        (if (r/err? attached)
          (do (try (stacks/detach-extra! ctx run @created) (catch Throwable t (caught! t) nil))
              (unclaim! ctx run)
              attached)
          (on-run! ctx info specs created))))))

(defn test!
  "Result<{:run :stack :status :exit :summary :cells [cell-result] :stack-kept?
   :reused? :summary-file}>: run the suite x project (x variant) matrix
   `opts` {:suites names :projects names :variants names :run RunId?
   :keep-up? :wait? (default true) :dry-run? :skip-fails?}; names as
   promote.suite/parse-names takes them (\"X,Y\", a coll, nil for all).
   `:skip-fails?` (nil: each suite's own :suite/skip-fails?) makes a skipped
   case fail its cell. `:exit` is 0 every cell passed, 1 a test failed, 2
   something could not run (2 wins). A suite's :suite/variants run as cells
   of their own (`<suite>@<variant>`), one after the other on the same stack,
   each with its env over the suite's, applied to the runner only.

   Without :run (or with a :run no stack holds), a stack of its own comes up
   (hc-6 stack-up!, which claims the run id atomically) for the union of what
   the cells need. With the :run of a live, :up stack run that no other
   matrix works on, the suites run on that run: claimed first, then each
   suite consumer gets its view network, joined by the run's instances it
   needs, removed afterwards; the run itself is never taken down. With
   :dry-run? nothing starts: the answer is the matrix, the stack plan and
   every runner spec (secret references rendered).

   Never throws: an unexpected throw answers :compose/test-threw naming the
   run and the stack-down that releases it."
  [ctx {:keys [keep-up? dry-run? wait?] :or {wait? true} :as opts}]
  (let [known (atom nil)]
    (try
      (if (and (not dry-run?) (not (satisfies? port/ISuiteRunner (:suite-runner ctx))))
        (r/err :compose/no-suite-runner {:hint "the context has no ISuiteRunner (:suite-runner)"})
        (r/let-ok [{:keys [run reuse? stack cells extra specs plan] ctx' :ctx} (plan-matrix ctx opts)]
          (let [stack-id (:stack/id stack)
                ;; this matrix's own claim id, test:<run>:<nonce>
                agent (claim-agent run ((or (:claim-nonce ctx) #(hex 4))))
                ctx (assoc ctx ::agent agent)
                ctx' (assoc ctx' ::agent agent)
                info {:run run :stack-id stack-id :reuse? reuse? :keep-up? keep-up?
                      :skip-fails? (:skip-fails? opts)
                      :variants? (some? (suite/parse-names (:variants opts)))}]
            (reset! known {:run run :stack stack-id :agent agent})
            (cond
              dry-run?
              (r/map-ok (stacks/plan ctx' {:stack stack-id :run run :extra extra})
                        (fn [p] {:run run :stack stack-id :dry-run? true :reused? reuse? :cells cells
                                 :plan (:diff p) :text (:text p)
                                 :runners (mapv shown-spec specs)}))

              reuse?
              (on-reused-run! ctx' info plan specs)

              :else
              (let [up (stacks/stack-up! ctx' {:stack stack-id :run run :wait? wait? :extra extra
                                                ;; claimed in the swap that records the run
                                                :claim {agent {:note "compose test"
                                                               :seen (+ ((:now ctx)) (long phase-lease-ms))}}})]
                (if (r/err? up)
                  (cond-> (do (unclaim! ctx run) ;; kept for inspection, not held
                              (assoc up :run run :stack stack-id))
                    (contains? (:stacks @(:state ctx)) run)
                    (assoc :hint (str "the matrix stack did not come up; it is kept for inspection. stack-down run="
                                      run " releases it")))
                  (on-run! ctx' info specs (atom []))))))))
      (catch Throwable t
        (caught! t)
        (let [{:keys [run stack agent]} @known]
          ;; a throw inside stack-up! leaves the run recorded with this
          ;; matrix's claim: drop it (the state swap lands before any save)
          (when (and run agent)
            (try (stacks/end-claim! ctx run agent {:down? false}) (catch Throwable t2 (caught! t2) nil)))
          (r/err :compose/test-threw
                 (cond-> {:message (str t)}
                   run (assoc :run run :stack stack)
                   (and run (contains? (:stacks @(:state ctx)) run))
                   (assoc :hint (str "the run is kept; stack-down run=" run " releases it")))))))))
