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
   `:record-run!` (fn [ctx cell-result]) -> Result, default
   `hive-compose.pipeline.flaky/record-run!` when that namespace is on the
   classpath and the context carries a `:history`.

   Every entry point answers a Result and never throws."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [hive-compose.boundary.report :as report]
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
  "Result<{:run RunId :reuse? bool}>: `run` when given (an existing stack run
   is reused), else a fresh `test<hex>`."
  [ctx run]
  (let [rid (or run (stack-run/run-id run-prefix ((or (:rand-hex ctx) #(hex 3)))))]
    (cond
      (not (m/validate schema/RunId rid))
      (r/err :compose/invalid-run {:run rid :problems (schema/explain schema/RunId rid)})
      :else (r/ok {:run rid :reuse? (contains? (:stacks @(:state ctx)) rid)}))))

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

(defn- cell-spec
  "Result<RunnerSpec> of `cell`: promote.suite's, with the suite consumer's
   wiring env. A container is created on the view network of the project
   under test (`:network`, so it reaches that project's services by name,
   its URLs from `:suite/env`), and with `:networks` also joins the suite
   consumer's own view network, where the instances of `:suite/needs` (otel ..)
   answer under their aliases."
  [sctx wirings cell]
  (let [w (get wirings (suite/suite-consumer (:suite/id cell)))]
    (r/map-ok (suite/runner-spec (assoc sctx :wiring-env (:wiring/env w)) cell)
              (fn [spec]
                (cond-> spec
                  (and w (= :container (:runner spec))) (assoc :networks [(:wiring/network w)]))))))

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
  (when save! (save! (select-keys @state [:active :current :stacks]))))

(defn- attach-suites!
  "Result<[network]>: in reused run `run`, each suite consumer of `plan` (the
   run's StackPlan re-resolved with the suites' needs) gets its view network,
   joined by the instances it binds, which must already run in the run. The
   networks are recorded on the run, so its stack-down removes them."
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
      (r/let-ok [nets (all-ok (fn [[_ cp]]
                                (r/let-ok [_ (port/-network-create! engine (:consumer/network cp)
                                                                    (stack/labels run (:stack/id entry)))
                                           _ (all-ok (fn [{:keys [instance aliases]}]
                                                       (let [ip (get live instance)]
                                                         (port/-network-connect! engine (:consumer/network cp)
                                                                                 {:project (:instance/compose-project ip)
                                                                                  :service (:instance/service ip)}
                                                                                 aliases)))
                                                     (:consumer/attach cp))]
                                  (r/ok (:consumer/network cp))))
                              suites)]
        (swap! (:state ctx) update-in [:stacks run :networks] #(vec (distinct (concat % nets))))
        (persist! ctx)
        (r/ok nets)))))

(defn- detach-suites!
  "Remove the suite networks `nets` added to reused run `run`."
  [ctx run nets]
  (let [res (mapv #(port/-network-rm! (:engine ctx) %) nets)]
    (swap! (:state ctx) update-in [:stacks run :networks] #(vec (remove (set nets) %)))
    (persist! ctx)
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

(def ^:private flaky-record
  (delay (try (requiring-resolve 'hive-compose.pipeline.flaky/record-run!)
              (catch Throwable _no-flake-history-on-classpath nil))))

(defn- record!
  "{:recorded ..} or {:record-error ..} or nil when there is no history."
  [ctx result]
  (when-let [f (or (:record-run! ctx) (when (:history ctx) @flaky-record))]
    (let [res (try (f ctx (select-keys result [:run/id :suite/id :project :report :exit]))
                   (catch Throwable t (r/err :compose/record-threw {:message (str t)})))]
      (if (r/ok? res)
        {:recorded (:ok res)}
        {:record-error (select-keys res [:error :message :hint])}))))

(def claim-agent-prefix
  "test! holds the run it works on under the claim `test:<run>`, renewed per
   cell, so the reaper never takes a matrix down mid-run."
  "test:")

(defn- claim-agent [run] (str claim-agent-prefix run))

(defn- owned-dir?
  "True when `dir` lies under hive-compose's state directory: its reports
   are ours to clear before an attempt."
  [ctx dir]
  (let [root (str (.getCanonicalPath (io/file (state-dir ctx))) "/")]
    (str/starts-with? (str (.getCanonicalPath (io/file dir)) "/") root)))

(defn- scrub-report
  "`report` (a Result<Report>) with every case's message and output passed
   through `scrub`, the runner's own (closed over the values it resolved)."
  [report scrub]
  (if (and scrub (r/ok? report))
    (update-in report [:ok :report/cases]
               (fn [cs] (mapv (fn [c] (cond-> c
                                        (string? (:case/message c)) (update :case/message scrub)
                                        (string? (:case/output c)) (update :case/output scrub)))
                              cs)))
    report))

(defn- run-cell!
  "The cell result of `spec` (promote.suite cell-result) plus what the run
   left behind: :test-id :trace-id :log-tail, and :runner-error when the
   runner could not run it to the end. The reports dir is cleared first when
   hive-compose owns it; a declared one is read only for files written since
   the attempt started. Case texts are scrubbed of resolved secrets."
  [ctx spec]
  (stacks/claim! ctx (:run spec) (claim-agent (:run spec)) {:note "compose test"})
  (let [t0 ((:now ctx))
        cleared (when (owned-dir? ctx (:reports-dir spec)) (report/clear-reports! (:reports-dir spec)))
        out (if (and cleared (r/err? cleared)) cleared (run-runner (:suite-runner ctx) spec))
        ok (when (r/ok? out) (:ok out))
        scrub (or (:scrub ok) (:scrub out))
        started (or (:started-ms ok) (:started-ms out) t0)
        rep (scrub-report (report/read-reports (:reports-dir spec) (:report-format spec :auto)
                                               ;; our own clock, not the runner's: files older than this attempt
                                               (when-not cleared t0))
                          scrub)
        result (suite/cell-result (:run spec) (:cell spec)
                                  {:exit (:exit ok)
                                   :report rep
                                   :started-at started
                                   :finished-at (or (:ended-ms ok) (:ended-ms out) ((:now ctx)))})
        tail (or (:log-tail ok) (:log-tail out))
        result (cond-> (assoc result :test-id (:test-id spec) :trace-id (:trace-id spec)
                              :reports-dir (:reports-dir spec))
                 (seq tail) (assoc :log-tail (vec tail))
                 (r/err? out) (assoc :runner-error (select-keys out [:error :message :reason :var :hint :exit])))]
    (merge result (record! ctx result))))

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

(defn- finish!
  "The summary of `results` and `phases`. A run this call created is taken
   down when every cell passed, every data phase answered ok and `keep-up?`
   is not set; a reused run is never taken down."
  [ctx {:keys [run stack-id reuse? keep-up?]} results phases]
  (let [summary (suite/matrix-summary results)
        phase-errors (vec (for [p phases :when (r/err? (:result p))]
                            {:phase (:phase p) :after-suite (:after-suite p)
                             :error (select-keys (:result p) [:error :message :hint])}))
        status (if (seq phase-errors) :error (:status summary))
        down? (and (= :passed status) (not keep-up?) (not reuse?))
        _ (unclaim! ctx run)
        down (when down? (stacks/stack-down! ctx {:run run}))]
    (r/ok (cond-> {:run run :stack stack-id :reused? (boolean reuse?)
                   :status status :exit (if (= :passed status) 0 1)
                   :summary (dissoc summary :status :exit)
                   :cells (vec results)
                   :stack-kept? (not (and down? (r/ok? down)))}
            (seq phases) (assoc :data-phases (mapv #(-> % (dissoc :result) (assoc :ok? (r/ok? (:result %)))) phases))
            (seq phase-errors) (assoc :data-errors phase-errors)
            (and down (r/ok? down)) (assoc :down (:ok down))
            (and down (r/err? down)) (assoc :down-error (select-keys down [:error :errors :hint]))
            (and (not down?) (not reuse?))
            (assoc :hint (str "the stack stays up"
                              (when (not= :passed status) " for inspection (failed)")
                              "; stack-down run=" run " releases it, the reaper after its TTL"))))))

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

(defn- run-cells!
  "[results phases]: every spec's cell run, suite by suite (specs are sorted
   by suite), with the data hook :before-suite before the first suite,
   :between-suites between two suites and :after at the end, when it is
   given. A :before-suite error runs no cell."
  [ctx run specs]
  (let [groups (partition-by #(get-in % [:cell :suite/id]) specs)
        hook? (some? (:data-phase! ctx))
        before (when hook? (phase! ctx run :before-suite nil))]
    (if (and before (r/err? (:result before)))
      [[] [before]]
      (loop [gs groups results [] phases (if before [before] [])]
        (if-let [g (first gs)]
          (let [rs (mapv #(run-cell! ctx %) g)
                sid (get-in (first g) [:cell :suite/id])
                between (when (and hook? (next gs)) (phase! ctx run :between-suites sid))]
            (recur (rest gs) (into results rs) (cond-> phases between (conj between))))
          [results (cond-> phases hook? (conj (phase! ctx run :after nil)))])))))

(defn test!
  "Result<{:run :stack :status :exit :summary :cells [cell-result] :stack-kept? :reused?}>:
   run the suite x project matrix `opts` {:suites names :projects names
   :run RunId? :keep-up? :wait? (default true) :dry-run?}; names as
   promote.suite/parse-names takes them (\"X,Y\", a coll, nil for all).

   Without :run (or with a :run no stack holds), a stack of its own comes up
   for the union of what the cells need. With the :run of a live stack run,
   the suites run on that run: each suite consumer gets its view network,
   joined by the run's instances it needs, removed afterwards; the run
   itself is never taken down. With :dry-run? nothing starts: the answer is
   the matrix, the stack plan and every runner spec (secret references
   rendered)."
  [ctx {:keys [keep-up? dry-run? wait?] :or {wait? true} :as opts}]
  (try
    (if (and (not dry-run?) (not (satisfies? port/ISuiteRunner (:suite-runner ctx))))
      (r/err :compose/no-suite-runner {:hint "the context has no ISuiteRunner (:suite-runner)"})
      (r/let-ok [{:keys [run reuse? stack cells extra specs plan] ctx' :ctx} (plan-matrix ctx opts)]
        (let [stack-id (:stack/id stack)
              info {:run run :stack-id stack-id :reuse? reuse? :keep-up? keep-up?}]
          (cond
            dry-run?
            (r/map-ok (stacks/plan ctx' {:stack stack-id :run run :extra extra})
                      (fn [p] {:run run :stack stack-id :dry-run? true :reused? reuse? :cells cells
                               :plan (:diff p) :text (:text p)
                               :runners (mapv shown-spec specs)}))

            reuse?
            (r/let-ok [nets (attach-suites! ctx' run plan)]
              (let [[results phases] (run-cells! ctx' run specs)
                    detach (detach-suites! ctx' run nets)]
                (r/map-ok (finish! ctx' info results phases)
                          #(cond-> % detach (assoc :detach-error (select-keys detach [:error :stderr]))))))

            :else
            (let [up (stacks/stack-up! ctx' {:stack stack-id :run run :wait? wait? :extra extra})]
              (if (r/err? up)
                (assoc up :run run :stack stack-id
                       :hint (str "the matrix stack did not come up; it is kept for inspection. stack-down run="
                                  run " releases it"))
                (let [[results phases] (run-cells! ctx' run specs)]
                  (finish! ctx' info results phases))))))))
    (catch Throwable t
      (r/err :compose/test-threw {:message (str t)}))))
