(ns hive-compose.promote.suite
  "Pure: the suite x project test matrix (hc-8).

   A Suite (`hive-compose.schema/Suite`, under `:compose/suites`) is a test
   runner in any language. `compose test suites=X,Y projects=A,B` runs each
   selected suite against each selected project it applies to. Each such
   pair is a cell.

   - `expand-matrix`  suites and the two filters to the cells
                      [{:suite/id :project}], unknown names as Result errors.
   - `union-needs`    the {consumer Needs} the cells need together, for the
                      capability resolver (hc-3): one stack for the matrix.
   - `runner-spec`    what to start for one cell in one run: a container or a
                      host command, with the correlation env (HIVE_RUN_ID,
                      HIVE_SUITE_ID, HIVE_TEST_ID_PREFIX, TRACEPARENT,
                      BAGGAGE), the OTLP env and the reports directory.
   - `cell-result`    exit code + report Result + timing to the run result
                      the flake history (hc-13 `record-run!`) takes.
   - `matrix-summary` the cell results folded.

   Services integrate only through protocols: env vars, OTLP, JUnit XML or
   TAP. No IO and no randomness here: trace ids come from an injected id
   source, and the OTLP env from an injected fn (`hive-compose.promote.otel/otlp-env`
   as data). Every entry point that can fail answers a hive-dsl Result."
  (:require [clojure.string :as str]
            [hive-compose.promote.otel :as otel]
            [hive-compose.schema :as schema]
            [hive-dsl.result :as r]
            [malli.core :as m]))

;; SPDX-License-Identifier: MIT

;; ---------------------------------------------------------------------------
;; Names
;; ---------------------------------------------------------------------------

(defn suite-consumer
  "The consumer id a suite's own `:suite/needs` are bound to: `suite:<id>`
   (see `hive-compose.schema/ConsumerId`)."
  [suite-id]
  (str "suite:" suite-id))

(defn test-id
  "The test id of a cell, `<suite>/<project>`. Baggage `test.id` and the OTLP
   resource attribute `hive.test.id` carry it."
  [{:suite/keys [id] :keys [project]}]
  (str id "/" project))

(defn test-id-prefix
  "HIVE_TEST_ID_PREFIX of a cell, `<suite>/<project>/`: a runner names the
   spans of its test `t` `<prefix><t>`."
  [cell]
  (str (test-id cell) "/"))

(defn parse-names
  "A filter as a sorted vector of distinct names, or nil for no filter. Takes
   `\"X,Y\"` (blank parts dropped), a collection of strings, keywords or
   symbols, or nil. A filter that names nothing (\"\" or \" , \") is no filter."
  [v]
  (let [parts (cond
                (nil? v) nil
                (string? v) (str/split v #",")
                (ident? v) [(name v)]
                (coll? v) (map #(if (ident? %) (name %) (str %)) v)
                :else [(str v)])]
    (->> parts (map str/trim) (remove str/blank?) distinct sort vec not-empty)))

;; ---------------------------------------------------------------------------
;; (a) Matrix
;; ---------------------------------------------------------------------------

(defn- unknown [known names]
  (vec (sort (distinct (remove #(contains? known %) names)))))

(defn- unknown-message [bad-suites bad-projects bad-suite-projects]
  (str/join "; "
            (cond-> []
              (seq bad-suites) (conj (str "unknown suites: " (str/join ", " bad-suites)))
              (seq bad-projects) (conj (str "unknown projects: " (str/join ", " bad-projects)))
              (seq bad-suite-projects)
              (conj (str "suites naming unknown projects: "
                         (str/join ", " (for [[s ps] bad-suite-projects]
                                          (str s " -> " (str/join "," ps)))))))))

(defn expand-matrix
  "Result<[{:suite/id :project}]>: the cells of `suites` ({id Suite}) over
   `projects` ({id Project}), sorted by suite, then by project.

   `filters` is {:suites :projects}, each in a form `parse-names` takes, nil
   for all. A suite that lists `:suite/projects` runs against those, any
   other suite against every project. The projects filter narrows that
   further.

   Errors: :compose/unknown-names when a filter or a selected suite's
   `:suite/projects` names a suite or project that does not exist. Every such
   name is listed under :suites, :projects or :suite-projects. The error is
   :compose/empty-matrix when the filters leave no cell."
  [suites projects filters]
  (let [suite-filter (parse-names (:suites filters))
        project-filter (parse-names (:projects filters))
        suite-ids (or suite-filter (vec (sort (keys suites))))
        bad-suites (unknown suites suite-filter)
        bad-projects (unknown projects project-filter)
        bad-suite-projects (into (sorted-map)
                                 (keep (fn [id]
                                         (let [bad (unknown projects (get-in suites [id :suite/projects]))]
                                           (when (seq bad) [id bad]))))
                                 (remove (set bad-suites) suite-ids))]
    (if (or (seq bad-suites) (seq bad-projects) (seq bad-suite-projects))
      (r/err :compose/unknown-names
             (cond-> {:message (unknown-message bad-suites bad-projects bad-suite-projects)}
               (seq bad-suites) (assoc :suites bad-suites)
               (seq bad-projects) (assoc :projects bad-projects)
               (seq bad-suite-projects) (assoc :suite-projects bad-suite-projects)))
      (let [cells (vec (for [sid suite-ids
                             :let [own (get-in suites [sid :suite/projects])
                                   ps (cond->> (if own (set own) (set (keys projects)))
                                        project-filter (filter (set project-filter)))]
                             p (sort ps)]
                         {:suite/id sid :project p}))]
        (if (seq cells)
          (r/ok cells)
          (r/err :compose/empty-matrix
                 {:suites suite-ids :projects project-filter
                  :message (str "no selected suite applies to the selected projects"
                                (when project-filter (str " (" (str/join ", " project-filter) ")")))}))))))

;; ---------------------------------------------------------------------------
;; (b) Union stack
;; ---------------------------------------------------------------------------

(defn union-needs
  "The {consumer Needs} that `cells` need together, in the shape the
   capability resolver (hc-3 `resolve-needs`) takes. It holds every project
   under test with its `:project/needs` (an empty map when it has none; the
   project still comes up) and every suite with `:suite/needs`, under
   `suite-consumer`. Nothing else goes in, and the order of `cells` does not
   change the result. The maps are sorted."
  [suites projects cells]
  (reduce (fn [acc {sid :suite/id p :project}]
            (let [sneeds (get-in suites [sid :suite/needs])]
              (cond-> (assoc acc p (into (sorted-map) (get-in projects [p :project/needs])))
                (seq sneeds) (assoc (suite-consumer sid) (into (sorted-map) sneeds)))))
          (sorted-map)
          cells))

(defn matrix-stack
  "The Stack value of the matrix of `cells` in `run`: id `test-<run>`, with
   the projects under test as its consumers. The suites' own needs are not
   part of it; they reach the resolver as extra needs (see `union-needs`)."
  [run cells]
  {:stack/id (str "test-" run)
   :stack/consumers (vec (sort (distinct (map :project cells))))
   :stack/description (str "suite matrix of run " run)})

;; ---------------------------------------------------------------------------
;; (c) Runner spec
;; ---------------------------------------------------------------------------

(def correlation-keys
  "The env vars hive-compose owns in a runner's env. Nothing a suite or a
   wiring sets overrides them, so every run stays correlated."
  ["HIVE_RUN_ID" "HIVE_SUITE_ID" "HIVE_TEST_ID_PREFIX" "HIVE_REPORTS_DIR" "TRACEPARENT" "BAGGAGE"])

(def container-reports-dir
  "Where a container runner writes reports, unless its suite names another
   directory."
  "/reports")

(defn run-dir
  "`<state-dir>/runs/<run>/<suite>/<project>`: everything one cell leaves
   behind."
  [state-dir run {:suite/keys [id] :keys [project]}]
  (str (str/replace (str state-dir) #"/+\z" "") "/runs/" run "/" id "/" project))

(defn reports-dir
  "The host directory a cell's reports land in: `<run-dir>/reports`."
  [state-dir run cell]
  (str (run-dir state-dir run cell) "/reports"))

(defn baggage
  "BAGGAGE of a cell: `test.id=<test-id>`, the value percent-encoded as
   `hive-compose.promote.otel/percent-encode` does it: every byte outside the
   RFC 3986 unreserved set, so `/` is `%2F`. Every baggage decoder restores
   the value."
  [cell]
  (str "test.id=" (otel/percent-encode (test-id cell))))

(defn- hex-id
  "Result<hex>: `n` bytes from `id-source`, as 2n lowercase hex chars. It is
   refused when malformed or all zero; W3C trace context forbids both."
  [id-source n what]
  (let [v (try (id-source n) (catch Exception e e))]
    (cond
      (instance? Exception v)
      (r/err :compose/invalid-trace-id {:what what :message (str "id source failed: " (ex-message v))})

      (not (and (string? v) (re-matches (re-pattern (str "[0-9a-f]{" (* 2 n) "}")) v)))
      (r/err :compose/invalid-trace-id {:what what :value v
                                        :message (str "id source gave no " (* 2 n) " lowercase hex chars")})

      (every? #{\0} v)
      (r/err :compose/invalid-trace-id {:what what :value v :message "an all-zero id is invalid"})

      :else (r/ok v))))

(defn traceparent
  "Result<{:trace-id :span-id :traceparent}>: a fresh sampled W3C
   traceparent, `00-<trace-id>-<span-id>-01`. `id-source` is (fn [n-bytes])
   returning 2n lowercase hex chars. This fn calls it and never calls `rand`."
  [id-source]
  (r/let-ok [trace (hex-id id-source 16 :trace-id)
             span (hex-id id-source 8 :span-id)]
    (r/ok {:trace-id trace :span-id span
           :traceparent (str "00-" trace "-" span "-01")})))

(defn- otlp
  "Result<Env> from the injected `otlp-env` fn. A nil fn means no telemetry."
  [otlp-env opts]
  (if (nil? otlp-env)
    (r/ok {})
    (try
      (let [env (otlp-env opts)]
        (if (map? env)
          (r/ok env)
          (r/err :compose/otlp-env-failed {:message "otlp-env answered no map" :value env})))
      (catch Exception e
        (r/err :compose/otlp-env-failed {:message (ex-message e) :data (ex-data e)})))))

(def ^:private resource-attributes "OTEL_RESOURCE_ATTRIBUTES")

(defn layer-env
  "The runner env, built in layers: `wiring` (the suite consumer's endpoints),
   then `otlp`, then `suite-env`, then `correlation`, forced on top.
   OTEL_EXPORTER_OTLP_ENDPOINT and OTEL_EXPORTER_OTLP_PROTOCOL are one unit
   (`otel/exporter-pair`): a layer that names either one replaces both.
   OTEL_RESOURCE_ATTRIBUTES is merged per key through `merge-ra` (fn [base
   over], default `otel/merge-resource-attributes`) in the same order, wiring
   < otlp < suite. On top of that the `hive.*` correlation keys of the otlp
   attributes are forced, and `hive.run.id=<run>` always is: only those win
   over the suite, which may still set service.name and its own attributes."
  [{:keys [wiring otlp suite-env correlation merge-ra run]}]
  (let [merge-ra (or merge-ra otel/merge-resource-attributes)
        pair otel/exporter-pair
        over (fn [lower upper]
               (merge (if (some #(contains? upper %) pair) (apply dissoc lower pair) lower)
                      upper))
        env (-> (over wiring otlp) (over suite-env) (merge correlation))
        ras (keep #(let [v (get % resource-attributes)] (when (string? v) v)) [wiring otlp suite-env])
        forced (otel/render-resource-attributes
                (cond-> (into {} (filter (comp otel/correlation-attribute? key))
                              (otel/parse-resource-attributes (get otlp resource-attributes)))
                  run (assoc "hive.run.id" run)))]
    (if (seq ras)
      (assoc env resource-attributes (reduce merge-ra (concat ras [forced])))
      env)))

(defn- join-path [dir path]
  (cond
    (str/starts-with? path "/") path
    dir (str (str/replace dir #"/+\z" "") "/" path)
    :else nil))

(defn runner-spec
  "Result<RunnerSpec> of `cell` in run `run`.

   `ctx`:
     :suites     {id Suite}
     :run        RunId
     :state-dir  hive-compose's state directory
     :id-source  (fn [n-bytes]) returning 2n lowercase hex chars, for the
                 trace and span ids
     :otlp-env   optional (fn [{:run-id :service :test-id :side}]) -> Env,
                 i.e. `hive-compose.promote.otel/otlp-env`; nil injects no OTLP
     :merge-resource-attributes  optional, see `layer-env`
     :wiring-env optional Env of the suite consumer's wiring (hc-5)

   A suite with `:suite/image` gives a container:
     {:runner :container :image :command? :env :mounts [{:host :container}]
      :network (the view network of the project under test) :labels :name}
   Any other suite gives a host command {:runner :command :command :dir? :env}.
   A container of a suite with `:suite/needs` also joins the view network of
   `suite:<id>` (:extra-networks), where the suite's own instances answer.
   Both carry :cell :run :test-id :trace-id :reports-dir and :report-format,
   plus :timeout-ms when the suite sets one. :reports-dir is always the
   cell's own `<state-dir>/runs/<run>/<suite>/<project>/reports`, so no two
   cells share one. A command that names `:suite/reports-dir` (relative to
   `:suite/dir`) also carries it as :collect-from: the pipeline empties it
   before the cell and moves what the runner leaves there into :reports-dir
   after. An env holding half the OTLP pair is :compose/otlp-pair-broken."
  [{:keys [suites run state-dir id-source wiring-env] :as ctx} cell]
  (let [sid (:suite/id cell)
        project (:project cell)
        suite (get suites sid)]
    (cond
      (nil? suite)
      (r/err :compose/unknown-names {:suites [sid] :message (str "unknown suite: " sid)})

      (not (m/validate schema/RunId run))
      (r/err :compose/invalid-run-id {:run run :message (str "not a run id: " (pr-str run))})

      (str/blank? (str state-dir))
      (r/err :compose/invalid-param {:param "state-dir" :message "runner-spec needs a :state-dir"})

      (nil? id-source)
      (r/err :compose/invalid-param {:param "id-source" :message "runner-spec needs an :id-source"})

      :else
      (let [image (:suite/image suite)
            container? (some? image)
            state-reports (reports-dir state-dir run cell)
            declared (:suite/reports-dir suite)
            collect-from (when-not container? (some->> declared (join-path (:suite/dir suite))))
            runner-reports (if container? (or declared container-reports-dir) state-reports)
            suite-net (when (seq (:suite/needs suite)) (schema/view-network run (suite-consumer sid)))
            tid (test-id cell)]
        (r/let-ok [tp (traceparent id-source)
                   otlp-vars (otlp (:otlp-env ctx) {:run-id run
                                                    :service (str "suite-" sid)
                                                    :test-id tid
                                                    :side (if container? :container :host)})]
          (let [env (layer-env {:wiring wiring-env
                                :otlp otlp-vars
                                :suite-env (:suite/env suite)
                                :merge-ra (:merge-resource-attributes ctx)
                                :run run
                                :correlation {"HIVE_RUN_ID" run
                                              "HIVE_SUITE_ID" sid
                                              "HIVE_TEST_ID_PREFIX" (test-id-prefix cell)
                                              "HIVE_REPORTS_DIR" runner-reports
                                              "TRACEPARENT" (:traceparent tp)
                                              "BAGGAGE" (baggage cell)}})
                common (cond-> {:cell {:suite/id sid :project project}
                                :run run
                                :test-id tid
                                :trace-id (:trace-id tp)
                                :env (into (sorted-map) env)
                                :reports-dir state-reports
                                :report-format (:suite/report-format suite :auto)}
                         (:suite/timeout-ms suite) (assoc :timeout-ms (:suite/timeout-ms suite))
                         collect-from (assoc :collect-from collect-from))
                command (:suite/command suite)
                present (filter #(contains? env %) otel/exporter-pair)]
            (if (= 1 (count present))
              (r/err :compose/otlp-pair-broken {:cell (:cell common) :present (vec present)
                                                :pair otel/exporter-pair
                                                :message "an OTLP endpoint without its protocol (or the reverse)"})
              (r/ok
               (if container?
                 (cond-> (assoc common
                                :runner :container
                                :image image
                                :name (str "hive-" run "-" (schema/slug sid) "-" (schema/slug project))
                                :mounts [{:host state-reports :container runner-reports}]
                                :network (schema/view-network run project)
                                :labels {(:run schema/docker-labels) run
                                         (:consumer schema/docker-labels) (suite-consumer sid)})
                   command (assoc :command command)
                   suite-net (assoc :extra-networks [suite-net]))
                 (cond-> (assoc common :runner :command :command command)
                   (:suite/dir suite) (assoc :dir (:suite/dir suite))))))))))))

(def RunnerSpec
  "What `runner-spec` answers. Kept here because the stack schema belongs to
   core."
  [:and
   [:map
    [:runner [:enum :container :command]]
    [:cell [:map [:suite/id :string] [:project :string]]]
    [:run schema/RunId]
    [:test-id :string]
    [:trace-id [:re #"^[0-9a-f]{32}\z"]]
    [:env schema/Env]
    [:reports-dir [:string {:min 1}]]
    [:report-format schema/ReportFormat]
    [:timeout-ms {:optional true} :int]
    [:image {:optional true} :string]
    [:name {:optional true} :string]
    [:command {:optional true} [:or :string [:vector :string]]]
    [:dir {:optional true} :string]
    [:mounts {:optional true} [:vector [:map [:host :string] [:container :string]]]]
    [:network {:optional true} :string]
    [:extra-networks {:optional true} [:vector :string]]
    [:collect-from {:optional true} :string]
    [:labels {:optional true} [:map-of :string :string]]]
   [:fn {:error/message "a container has an image, mounts and a network; a command has a command"}
    (fn [s] (case (:runner s)
              :container (every? #(contains? s %) [:image :mounts :network])
              :command (contains? s :command)))]])

;; ---------------------------------------------------------------------------
;; (d) Cell result
;; ---------------------------------------------------------------------------

(defn- failures
  "Failed and errored cases of `report`: the larger of its totals and its
   cases, so a report whose totals were not recounted still fails."
  [report]
  (let [t (:report/totals report)]
    (max (+ (:failed t 0) (:error t 0))
         (count (filter #(#{:failed :error} (:case/status %)) (:report/cases report))))))

(defn cell-status
  "How a cell ended.
   - No report and a non-zero or missing exit (killed, timed out): :error,
     because the runner died before it reported.
   - A report with failed or errored cases, or with a bail-out, or a
     non-zero or missing exit: :failed.
   - Anything else: :passed. With no report, exit 0 is the verdict."
  [exit report]
  (let [clean? (and (number? exit) (zero? exit))]
    (cond
      (nil? report) (if clean? :passed :error)
      (or (not clean?) (pos? (failures report)) (:report/bail-out report)) :failed
      :else :passed)))

(defn cell-result
  "The run result of `cell` in `run`, in the shape hc-13 `record-run!` takes:
     {:run/id :suite/id :project :exit :status :passed|:failed|:error
      :report Report?  :report-error {:error :message ..}?
      :started-at :finished-at :duration-ms}
   `outcome` is {:exit n-or-nil :report Result<Report> :started-at ms
   :finished-at ms}. A report error such as :report/none (nothing written)
   counts as no report; the error is kept under :report-error."
  [run cell {:keys [exit report started-at finished-at]}]
  (let [rep (when (r/ok? report) (:ok report))]
    (cond-> {:run/id run
             :suite/id (:suite/id cell)
             :project (:project cell)
             :exit exit
             :status (cell-status exit rep)}
      rep (assoc :report rep)
      (r/err? report) (assoc :report-error (select-keys report [:error :message :reason :dir]))
      started-at (assoc :started-at started-at)
      finished-at (assoc :finished-at finished-at)
      (and (number? started-at) (number? finished-at))
      (assoc :duration-ms (max 0 (- finished-at started-at))))))

;; ---------------------------------------------------------------------------
;; (e) Summary
;; ---------------------------------------------------------------------------

(def ^:private status-rank {:passed 0 :failed 1 :error 2})

(defn- worst [statuses]
  (reduce (fn [a b] (if (> (status-rank b) (status-rank a)) b a)) :passed statuses))

(defn- failed-cases [result]
  (->> (get-in result [:report :report/cases])
       (filter #(#{:failed :error} (:case/status %)))
       (map :case/id)
       sort
       vec))

(defn matrix-summary
  "The cell results folded. The order of `results` does not change it:
     {:status   the worst cell (:error > :failed > :passed; :passed for none)
      :exit     0 when every cell passed, else 1
      :cells n  :passed n  :failed n  :error n
      :cases    the report totals summed {:passed :failed :error :skipped}
      :by-suite {suite {status n}}  :by-project {project {status n}}
      :failures [{:suite/id :project :status :exit :cases [case ids]}], sorted
      :duration-ms  the sum over the cells that report one}"
  [results]
  (let [counts (merge {:passed 0 :failed 0 :error 0} (frequencies (map :status results)))
        by (fn [k] (into (sorted-map)
                         (map (fn [[id rs]] [id (into (sorted-map) (frequencies (map :status rs)))]))
                         (group-by k results)))
        status (worst (map :status results))]
    (merge {:status status
            :exit (if (= :passed status) 0 1)
            :cells (count results)
            :cases (apply merge-with + {:passed 0 :failed 0 :error 0 :skipped 0}
                          (map #(select-keys (get-in % [:report :report/totals]) [:passed :failed :error :skipped])
                               results))
            :by-suite (by :suite/id)
            :by-project (by :project)
            :failures (->> results
                           (remove #(= :passed (:status %)))
                           (map #(assoc (select-keys % [:suite/id :project :status :exit])
                                        :cases (failed-cases %)))
                           (sort-by (juxt :suite/id :project))
                           vec)
            :duration-ms (reduce + 0 (keep :duration-ms results))}
           (select-keys counts [:passed :failed :error]))))
