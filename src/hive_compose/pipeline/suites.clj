(ns hive-compose.pipeline.suites
  "The suite x project matrix run (hc-8), over the context `hive-compose.pipeline.ops`
   takes, plus `:suite-runner` (a `hive-compose.port/ISuiteRunner`).

     test!  expand the matrix (promote.suite), bring up ONE stack for it, the
            union of the projects under test and the suites' own needs
            (hc-6 stack-up! with :extra), run every cell's runner through
            ISuiteRunner, read its reports (boundary.report), fold the cell
            result, record it in the flake history (hc-13, when present),
            then the matrix summary. A matrix that passed is taken down unless
            :keep-up?; a failed one stays up for inspection (the reaper TTL
            still applies, and `stack-down run=<run>` releases it).

   Optional context keys: `:id-source` (fn [n-bytes]) -> 2n hex chars for
   trace ids, `:rand-hex` (0-arity, 6 hex chars) for run ids, `:state-dir`
   (default: the state file's directory), `:otlp-env` (default
   `promote.otel/otlp-env` when the stack binds an otel instance, else none),
   `:record-run!` (fn [ctx cell-result]) -> Result, the flake history
   (hc-13), none when absent, `:data-phase!` (fn [ctx run phase]) (hc-9).
   Each cell's full result goes to `<state>/runs/<run>/<suite>/<project>/result.edn`
   and the matrix answer to `<state>/runs/<run>/summary.edn` (boundary.runs).

   Every entry point answers a Result and never throws."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [hive-compose.boundary.report :as report]
            [hive-compose.boundary.runs :as runs]
            [hive-compose.pipeline.stacks :as stacks]
            [hive-compose.port :as port]
            [hive-compose.promote.env :as env]
            [hive-compose.promote.otel :as otel]
            [hive-compose.promote.stack :as stack]
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
  "test! holds the run it works on under the claim `test:<run>` from stack-up
   (or attach) to the end, its lease (`:seen`) renewed before every cell and
   data phase to cover that step's deadline, so the reaper never takes a
   matrix down mid-run."
  "test:")

(defn- claim-agent [run] (str claim-agent-prefix run))

(defn- hex [n]
  (let [b (byte-array n)]
    (.nextBytes (SecureRandom.) b)
    (apply str (map #(format "%02x" (bit-and % 0xff)) b))))

(defn- default-id-source [n] (hex n))

(defn- state-dir [ctx]
  (or (:state-dir ctx)
      (some-> (get-in ctx [:settings :compose/state-file]) io/file .getParentFile str)
      (str (System/getProperty "java.io.tmpdir") "/hive-compose")))

(defn- pick-run
  "Result<{:run RunId :reuse? bool}>: `run` when given (a live stack run is
   reused; one that is not :up is refused), else a fresh `test<hex>`. A run
   another matrix is working on (a live `test:` claim) is refused too."
  [ctx run]
  (let [rid (or run (stack-run/run-id run-prefix ((or (:rand-hex ctx) #(hex 3)))))
        entry (get-in @(:state ctx) [:stacks rid])]
    (cond
      (not (m/validate schema/RunId rid))
      (r/err :compose/invalid-run {:run rid :problems (schema/explain schema/RunId rid)})

      (and entry (not= :up (:status entry)))
      (r/err :compose/run-not-up {:run rid :status (:status entry)
                                  :hint (str "the run is " (some-> (:status entry) name)
                                             "; stack-down run=" rid " first, or omit run for a stack of its own")})

      (some #(str/starts-with? % claim-agent-prefix) (keys (:claims entry)))
      (r/err :compose/run-busy {:run rid :hint "another compose test works on this run; wait for it or omit run"})

      :else (r/ok {:run rid :reuse? (some? entry)}))))

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
  "The `runner-spec` context of a run: suites, ids, state dir, OTLP."
  [ctx run resolution]
  {:suites (suites-of ctx)
   :run run
   :state-dir (state-dir ctx)
   :id-source (or (:id-source ctx) default-id-source)
   :otlp-env (if (contains? ctx :otlp-env)
               (:otlp-env ctx)
               (when (binds-otel? resolution) otel/otlp-env))})

(def default-cell-timeout-ms
  "A cell's deadline when its suite sets no :suite/timeout-ms: what the claim
   lease covers and the runner bounds the run by."
  (* 30 60 1000))

(defn- cell-spec
  "Result<RunnerSpec> of `cell`: promote.suite's (a container on the project
   under test's view network, plus `:extra-networks` [the suite consumer's]
   when the suite has needs; a command's declared reports dir as
   `:collect-from`), with the suite consumer's wiring env and a deadline
   always set."
  [sctx wirings cell]
  (let [w (get wirings (suite/suite-consumer (:suite/id cell)))]
    (r/map-ok (suite/runner-spec (assoc sctx :wiring-env (:wiring/env w)) cell)
              #(update % :timeout-ms (fnil identity default-cell-timeout-ms)))))

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
  [ctx {:keys [suites projects run]}]
  (r/let-ok [cells (suite/expand-matrix (suites-of ctx) (projects-of ctx) {:suites suites :projects projects})
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
                 specs (all-ok #(cell-spec (spec-ctx ctx rid resolution) wirings %) cells)]
        (r/ok {:run rid :reuse? reuse? :stack stack :cells cells :extra extra :resolution resolution
               :wirings wirings :plan plan :specs specs :ctx ctx'})))))

(defn- shown-spec
  "A RunnerSpec as shown: secret references rendered."
  [spec]
  (update spec :env env/rendered))

(defn- persist! [{:keys [state save!]}]
  (when save! (save! (select-keys @state [:active :current :stacks :preexisting]))))

(defn- attach-suites!
  "Result<[network]>: in reused run `run`, each suite consumer of `plan` (the
   run's StackPlan re-resolved with the suites' needs) gets its view network,
   joined by the instances it binds, which must already run in the run. Each
   network is recorded on the run before anything joins it, so its
   stack-down removes it even when a connect fails; only networks this call
   created are answered (and detached afterwards)."
  [ctx run plan]
  (let [engine (:engine ctx)
        entry (get-in @(:state ctx) [:stacks run])
        live (get-in entry [:plan :stack/instances])
        suites (filter (fn [[c _]] (re-find #"^suite:" c)) (:stack/consumers plan))
        missing (vec (sort (for [[_ cp] suites {:keys [instance]} (:consumer/attach cp)
                                 :when (not (contains? live instance))]
                             instance)))]
    (if (seq missing)
      (r/err :compose/suite-needs-not-in-run
             {:run run :instances missing
              :hint "the reused run does not run what the suites need; omit run for a stack of their own"})
      (all-ok (fn [[_ cp]]
                (let [net (:consumer/network cp)]
                  (r/let-ok [made (port/-network-create! engine net (stack/labels run (:stack/id entry)))]
                    (swap! (:state ctx) update-in [:stacks run :networks] #(vec (distinct (conj (vec %) net))))
                    (persist! ctx)
                    (r/let-ok [_ (all-ok (fn [{:keys [instance aliases]}]
                                           (let [ip (get live instance)]
                                             (port/-network-connect! engine net
                                                                     {:project (:instance/compose-project ip)
                                                                      :service (:instance/service ip)}
                                                                     aliases)))
                                         (:consumer/attach cp))]
                      (r/ok (when (:created? made) net))))))
              suites))))

(defn- detach-suites!
  "Remove the suite networks `nets` (those attach-suites! created; nils
   ignored) from reused run `run`. Answers the first error, nil when clean."
  [ctx run nets]
  (let [nets (vec (remove nil? nets))
        res (mapv #(port/-network-rm! (:engine ctx) %) nets)]
    (when (seq nets)
      (swap! (:state ctx) update-in [:stacks run :networks] #(vec (remove (set nets) %)))
      (persist! ctx))
    (first (filter r/err? res))))

;; ---------------------------------------------------------------------------
;; one cell

(defn- run-runner [runner spec]
  (try
    (if (= :container (:runner spec))
      (port/-run-container! runner spec)
      (port/-run-command! runner spec))
    (catch Throwable t
      (r/err :compose/runner-threw {:message (str t)}))))

(defn- record!
  "{:recorded ..} or {:record-error ..} through the injected `:record-run!`
   (fn [ctx cell-result]) -> Result (hc-13 `pipeline.flaky/record-run!`),
   nil when none is wired. The whole cell result goes, but for its log tail."
  [ctx result]
  (when-let [f (:record-run! ctx)]
    (let [res (try (f ctx (dissoc result :log-tail))
                   (catch Throwable t (r/err :compose/record-threw {:message (str t)})))]
      (if (r/ok? res)
        {:recorded (:ok res)}
        {:record-error (select-keys res [:error :message :hint])}))))

(def ^:private scrubbed-case-keys
  "Every text a case carries: a parametrised test id or name (pytest
   `test_auth[<token>]`) is as likely a carrier as its output."
  [:case/id :case/name :case/classname :case/message :case/output])

(defn- scrub-report
  "`report` (a Result<Report>) with every text of every case and the
   bail-out reason passed through `scrub`, the runner's own (closed over the
   values it resolved)."
  [report scrub]
  (if (and scrub (r/ok? report))
    (-> report
        (update-in [:ok :report/cases]
                   (fn [cs] (mapv (fn [c] (reduce (fn [c k] (cond-> c (string? (get c k)) (update k scrub)))
                                                  c scrubbed-case-keys))
                                  cs)))
        (update :ok #(cond-> % (string? (:report/bail-out %)) (update :report/bail-out scrub))))
    report))

(defn- lease!
  "Result: test!'s claim on `run` renewed so it stays live for `ms` from now
   (its `:seen` set ahead, as stack-run/claim-live? reads it). An error when
   the run is gone (taken down meanwhile)."
  [ctx run ms]
  (r/let-ok [_ (stacks/claim! ctx run (claim-agent run) {:note "compose test"})]
    (swap! (:state ctx) update-in [:stacks run :claims (claim-agent run)]
           #(when % (assoc % :seen (+ ((:now ctx)) (long ms)))))
    (persist! ctx)
    (r/ok run)))

(def answer-text-chars
  "How much of a case's message or output the answer carries; the full text
   stays in the cell's reports dir and in its result.edn."
  2000)

(defn- clip [s] (if (and (string? s) (> (count s) answer-text-chars)) (str (subs s 0 answer-text-chars) "...") s))

(defn- redact-report
  "`report` with every case's free text (message, output) and the bail-out
   reason dropped: a runner that resolved secrets but answered no :scrub
   cannot be trusted with them."
  [report]
  (if (r/ok? report)
    (-> report
        (update-in [:ok :report/cases] (fn [cs] (mapv #(assoc % :case/message nil :case/output nil) cs)))
        (update :ok #(cond-> (dissoc % :report/bail-out) (:report/bail-out %) (assoc :report/bail-out "[redacted]")))
        (assoc-in [:ok :report/redacted?] true))
    report))

(defn- prepare-dirs!
  "Result: the cell's own reports dir cleared, and its declared host dir
   (`:collect-from`) too, loudly: a report file that cannot be deleted stops
   the attempt rather than be read as this one's."
  [spec]
  (r/let-ok [_ (report/clear-reports! (:reports-dir spec))
             _ (if-let [from (:collect-from spec)] (report/clear-reports! from) (r/ok nil))]
    (r/ok spec)))

(defn- cell-error
  "The :error cell result of `spec` that could not run: `err` names why."
  [spec err t0 t1]
  (-> (suite/cell-result (:run spec) (:cell spec)
                         {:exit nil :report (r/err :report/none {}) :started-at t0 :finished-at t1})
      (assoc :test-id (:test-id spec) :trace-id (:trace-id spec) :reports-dir (:reports-dir spec)
             :runner-error (select-keys err [:error :message :reason :var :hint :exit :leftovers :run]))))

(defn- run-cell!
  "The cell result of `spec` (promote.suite cell-result) plus what the run
   left behind: :test-id :trace-id :log-tail, and :runner-error when the
   runner could not run it to the end. Before the runner: the claim lease is
   renewed over the cell's deadline and the reports dirs are cleared; after:
   a declared host dir is collected into the cell's own, then read. Case
   texts are scrubbed with the runner's :scrub (redacted when the spec
   resolves secrets and the runner gives none) and clipped in the answer.
   Never throws: a throw is an :error cell. The full result is written to
   `<run-dir>/result.edn` (boundary.runs, read by `why`)."
  [ctx spec]
  (let [t0 ((:now ctx))]
    (try
      (let [ready (r/let-ok [_ (lease! ctx (:run spec) (+ (:timeout-ms spec default-cell-timeout-ms)
                                                         (* 10 60 1000)))]
                    (prepare-dirs! spec))]
        (if (r/err? ready)
          (cell-error spec ready t0 ((:now ctx)))
          (let [out (run-runner (:suite-runner ctx) spec)
                ok (when (r/ok? out) (:ok out))
                scrub (or (:scrub ok) (:scrub out))
                collected (if-let [from (:collect-from spec)]
                            (report/collect-reports! from (:reports-dir spec))
                            (r/ok nil))
                raw (if (r/err? collected) collected (report/read-reports (:reports-dir spec) (:report-format spec :auto)))
                rep (cond
                      scrub (scrub-report raw scrub)
                      (seq (env/refs (:env spec))) (redact-report raw)
                      :else raw)
                result (suite/cell-result (:run spec) (:cell spec)
                                          {:exit (:exit ok)
                                           :report rep
                                           :started-at (or (:started-ms ok) (:started-ms out) t0)
                                           :finished-at (or (:ended-ms ok) (:ended-ms out) ((:now ctx)))})
                tail (or (:log-tail ok) (:log-tail out))
                result (cond-> (assoc result :test-id (:test-id spec) :trace-id (:trace-id spec)
                                      :reports-dir (:reports-dir spec))
                         (seq tail) (assoc :log-tail (vec tail))
                         (r/err? out) (assoc :runner-error (select-keys out [:error :message :reason :var :hint :exit])))
                written (runs/write-result! (state-dir ctx) (dissoc result :log-tail :runner-error))]
            (cond-> (merge result (record! ctx result))
              (r/err? written) (assoc :result-error (select-keys written [:error :problems :message]))))))
      (catch Throwable t
        (cell-error spec {:error :compose/cell-threw :message (str t)} t0 ((:now ctx)))))))

;; ---------------------------------------------------------------------------
;; test!

(defn- unclaim!
  "Drop test!'s claim on `run` and touch it: a run kept for inspection gets a
   full idle TTL from the end of the matrix, not from its stack-up. A run
   that is gone (taken down) is left alone."
  [ctx run]
  (when (contains? (:stacks @(:state ctx)) run)
    (swap! (:state ctx) update-in [:stacks run]
           #(-> % (update :claims dissoc (claim-agent run)) (assoc :last-touch ((:now ctx)))))
    (persist! ctx)))

(defn- answer-cell
  "`result` as the answer carries it: the report cut to its totals and its
   failed/errored cases, their texts clipped to `answer-text-chars`; the
   full report stays in the reports dir and result.edn."
  [result]
  (if-let [rep (:report result)]
    (assoc result :report
           (cond-> {:report/format (:report/format rep)
                    :report/totals (:report/totals rep)
                    :report/failed-cases (vec (for [c (:report/cases rep)
                                                    :when (#{:failed :error} (:case/status c))]
                                                (-> c (update :case/message clip) (update :case/output clip))))}
             (:report/bail-out rep) (assoc :report/bail-out (clip (:report/bail-out rep)))
             (:report/redacted? rep) (assoc :report/redacted? true)))
    result))

(defn- foreign-claims
  "Claims on `run` other than test!'s own: someone else works on it."
  [ctx run]
  (vec (sort (remove #(= (claim-agent run) %) (keys (get-in @(:state ctx) [:stacks run :claims]))))))

(defn- finish!
  "The answer for `results` and `phases`, also written to
   `<state>/runs/<run>/summary.edn`. A run this call created is taken down
   when every cell passed, every data phase answered ok, `keep-up?` is not
   set and nobody else claims it; a reused run is never taken down."
  [ctx {:keys [run stack-id reuse? keep-up?]} results phases]
  (let [summary (suite/matrix-summary results)
        phase-errors (vec (for [p phases :when (r/err? (:result p))]
                            {:phase (:phase p) :after-suite (:after-suite p)
                             :error (select-keys (:result p) [:error :message :hint])}))
        status (if (seq phase-errors) :error (:status summary))
        _ (unclaim! ctx run)
        others (foreign-claims ctx run)
        down? (and (= :passed status) (not keep-up?) (not reuse?) (empty? others))
        down (when down? (stacks/stack-down! ctx {:run run}))
        kept? (not (and down? (r/ok? down)))
        answer (cond-> {:run run :stack stack-id :reused? (boolean reuse?)
                        :status status :exit (if (= :passed status) 0 1)
                        :summary (dissoc summary :status :exit)
                        :cells (mapv answer-cell results)
                        :stack-kept? kept?}
                 (seq phases) (assoc :data-phases (mapv #(-> % (dissoc :result) (assoc :ok? (r/ok? (:result %)))) phases))
                 (seq phase-errors) (assoc :data-errors phase-errors)
                 (and down (r/ok? down)) (assoc :down (:ok down))
                 (and down (r/err? down)) (assoc :down-error (select-keys down [:error :errors :hint]))
                 (seq others) (assoc :claimed-by others)
                 (and kept? (contains? (:stacks @(:state ctx)) run))
                 (assoc :hint (str "the stack stays up"
                                   (cond reuse? " (a reused run is never taken down)"
                                         (seq others) (str " (claimed by " (str/join ", " others) ")")
                                         (not= :passed status) " for inspection (failed)"
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
                   (catch Throwable t (r/err :compose/data-phase-threw {:message (str t)})))
              (r/ok nil))]
    (cond-> {:phase phase :result res} after-suite (assoc :after-suite after-suite))))

(def phase-lease-ms
  "The claim lease a data phase runs under."
  (* 30 60 1000))

(defn- run-cells!
  "[results phases]: every spec's cell run, suite by suite (specs are sorted
   by suite), with the data hook :before-suite before the first suite,
   :between-suites between two suites and :after at the end, when it is
   given; the claim lease is renewed before each. A :before-suite error
   runs no cell; a run gone meanwhile answers every remaining cell :error."
  [ctx run specs]
  (let [groups (partition-by #(get-in % [:cell :suite/id]) specs)
        hook? (some? (:data-phase! ctx))
        phase (fn [p after] (lease! ctx run phase-lease-ms) (phase! ctx run p after))
        before (when hook? (phase :before-suite nil))]
    (if (and before (r/err? (:result before)))
      [[] [before]]
      (loop [gs groups results [] phases (if before [before] [])]
        (if-let [g (first gs)]
          (let [rs (mapv #(run-cell! ctx %) g)
                sid (get-in (first g) [:cell :suite/id])
                between (when (and hook? (next gs)) (phase :between-suites sid))]
            (recur (rest gs) (into results rs) (cond-> phases between (conj between))))
          [results (cond-> phases hook? (conj (phase :after nil)))])))))

(defn- on-run!
  "Run the cells of `specs` on live run `run`: claimed (lease) first, so the
   reaper cannot take it while the data phases and cells run; the claim and
   any suite networks attached for it are released in a finally, whatever
   happens."
  [ctx info specs nets]
  (let [run (:run info)]
    (try
      (lease! ctx run phase-lease-ms)
      (let [[results phases] (run-cells! ctx run specs)
            detach (when (:reuse? info) (detach-suites! ctx run nets))]
        (r/map-ok (finish! ctx info results phases)
                  #(cond-> % detach (assoc :detach-error (select-keys detach [:error :stderr])))))
      (finally
        (try (unclaim! ctx run) (catch Throwable _best-effort nil))))))

(defn test!
  "Result<{:run :stack :status :exit :summary :cells [cell-result] :stack-kept?
   :reused? :summary-file}>: run the suite x project matrix `opts` {:suites
   names :projects names :run RunId? :keep-up? :wait? (default true)
   :dry-run?}; names as promote.suite/parse-names takes them (\"X,Y\", a
   coll, nil for all).

   Without :run (or with a :run no stack holds), a stack of its own comes up
   (hc-6 stack-up!, which claims the run id atomically) for the union of what
   the cells need. With the :run of a live, :up stack run, the suites run on
   that run: each suite consumer gets its view network, joined by the run's
   instances it needs, removed afterwards; the run itself is never taken
   down. With :dry-run? nothing starts: the answer is the matrix, the stack
   plan and every runner spec (secret references rendered).

   Never throws: an unexpected throw answers :compose/test-threw naming the
   run and the stack-down that releases it."
  [ctx {:keys [keep-up? dry-run? wait?] :or {wait? true} :as opts}]
  (let [known (atom nil)]
    (try
      (if (and (not dry-run?) (not (satisfies? port/ISuiteRunner (:suite-runner ctx))))
        (r/err :compose/no-suite-runner {:hint "the context has no ISuiteRunner (:suite-runner)"})
        (r/let-ok [{:keys [run reuse? stack cells extra specs plan] ctx' :ctx} (plan-matrix ctx opts)]
          (let [stack-id (:stack/id stack)
                info {:run run :stack-id stack-id :reuse? reuse? :keep-up? keep-up?}]
            (reset! known {:run run :stack stack-id})
            (cond
              dry-run?
              (r/map-ok (stacks/plan ctx' {:stack stack-id :run run :extra extra})
                        (fn [p] {:run run :stack stack-id :dry-run? true :reused? reuse? :cells cells
                                 :plan (:diff p) :text (:text p)
                                 :runners (mapv shown-spec specs)}))

              reuse?
              (r/let-ok [nets (attach-suites! ctx' run plan)]
                (on-run! ctx' info specs nets))

              :else
              (let [up (stacks/stack-up! ctx' {:stack stack-id :run run :wait? wait? :extra extra})]
                (if (r/err? up)
                  (cond-> (assoc up :run run :stack stack-id)
                    (contains? (:stacks @(:state ctx)) run)
                    (assoc :hint (str "the matrix stack did not come up; it is kept for inspection. stack-down run="
                                      run " releases it")))
                  (on-run! ctx' info specs nil)))))))
      (catch Throwable t
        (let [{:keys [run stack]} @known]
          (r/err :compose/test-threw
                 (cond-> {:message (str t)}
                   run (assoc :run run :stack stack)
                   (and run (contains? (:stacks @(:state ctx)) run))
                   (assoc :hint (str "the run is kept; stack-down run=" run " releases it")))))))))
