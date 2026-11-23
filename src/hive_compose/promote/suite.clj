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

(defn cell-key
  "The suite part of a cell's identity: its suite id, or `<suite>@<variant>`
   for a cell of a `:suite/variants` variant. Test ids, run dirs, container
   names, the flake history and `why` all key on it, so two variants of one
   suite never share a history or a reports dir."
  [{:suite/keys [id] :keys [variant]}]
  (if variant (str id "@" variant) id))

(defn test-id
  "The test id of a cell, `<suite>/<project>` (`<suite>@<variant>/<project>`
   for a variant cell). Baggage `test.id` and the OTLP resource attribute
   `hive.test.id` carry it."
  [{:keys [project] :as cell}]
  (str (cell-key cell) "/" project))

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

(defn- unknown-message [bad-suites bad-projects bad-suite-projects & [bad-variants]]
  (str/join "; "
            (cond-> []
              (seq bad-suites) (conj (str "unknown suites: " (str/join ", " bad-suites)))
              (seq bad-projects) (conj (str "unknown projects: " (str/join ", " bad-projects)))
              (seq bad-variants) (conj (str "variants no selected suite declares: " (str/join ", " bad-variants)))
              (seq bad-suite-projects)
              (conj (str "suites naming unknown projects: "
                         (str/join ", " (for [[s ps] bad-suite-projects]
                                          (str s " -> " (str/join "," ps)))))))))

(defn expand-matrix
  "Result<[{:suite/id :project :variant?}]>: the cells of `suites` ({id Suite})
   over `projects` ({id Project}), sorted by suite, then variant, then project.

   `filters` is {:suites :projects :variants}, each in a form `parse-names`
   takes, nil for all. A suite that lists `:suite/projects` runs against
   those, any other suite against every project. The projects filter narrows
   that further. A suite that declares `:suite/variants` gives one cell per
   variant (`:variant` its name), each a cell of its own; the variants filter
   narrows those, and leaves a suite without variants alone (it runs once).

   No selected suite ever vanishes: with a variants filter, every selected
   suite that declares variants must declare EVERY requested one, else the
   call is refused naming the missing ones per suite.

   Errors: :compose/unknown-names when a filter or a selected suite's
   `:suite/projects` names a suite or project that does not exist, or a
   requested variant is missing from a selected suite that declares variants
   (:variants {suite [missing]}), or no selected suite declares variants at
   all (:variants {\"*\" [requested]}). Every such name is listed under
   :suites, :projects, :suite-projects or :variants. The error is
   :compose/empty-matrix when the filters leave no cell."
  [suites projects filters]
  (let [suite-filter (parse-names (:suites filters))
        project-filter (parse-names (:projects filters))
        variant-filter (parse-names (:variants filters))
        suite-ids (or suite-filter (vec (sort (keys suites))))
        bad-suites (unknown suites suite-filter)
        bad-projects (unknown projects project-filter)
        bad-suite-projects (into (sorted-map)
                                 (keep (fn [id]
                                         (let [bad (unknown projects (get-in suites [id :suite/projects]))]
                                           (when (seq bad) [id bad]))))
                                 (remove (set bad-suites) suite-ids))
        ;; every selected suite that declares variants must keep at least one
        ;; under the filter: no selected suite ever vanishes from the matrix.
        ;; {suite [requested variant it does not declare]}
        bad-variants (when variant-filter
                       (into (sorted-map)
                             (keep (fn [id]
                                     (let [vs (get-in suites [id :suite/variants])
                                           missing (vec (remove #(contains? vs %) variant-filter))]
                                       (when (and (seq vs) (seq missing))
                                         [id missing]))))
                             (remove (set bad-suites) suite-ids)))
        ;; a requested variant no selected suite declares at all
        declared (into #{} (mapcat #(keys (get-in suites [% :suite/variants]))) suite-ids)
        undeclared (unknown declared variant-filter)
        bad-variants (cond-> bad-variants
                       (and (seq undeclared) (empty? bad-variants))
                       (assoc "*" undeclared))]
    (if (or (seq bad-suites) (seq bad-projects) (seq bad-suite-projects) (seq bad-variants))
      (r/err :compose/unknown-names
             (cond-> {:message (unknown-message bad-suites bad-projects bad-suite-projects
                                                (for [[s vs] bad-variants]
                                                  (str (if (= "*" s) "any selected suite" s) " -> " (str/join "," vs))))}
               (seq bad-suites) (assoc :suites bad-suites)
               (seq bad-projects) (assoc :projects bad-projects)
               (seq bad-suite-projects) (assoc :suite-projects bad-suite-projects)
               (seq bad-variants) (assoc :variants bad-variants)))
      (let [cells (vec (for [sid suite-ids
                             :let [own (get-in suites [sid :suite/projects])
                                   ps (cond->> (if own (set own) (set (keys projects)))
                                        project-filter (filter (set project-filter)))
                                   vs (get-in suites [sid :suite/variants])
                                   vnames (if (seq vs)
                                            (cond->> (sort (keys vs))
                                              variant-filter (filter (set variant-filter)))
                                            [nil])]
                             v vnames
                             p (sort ps)]
                         (cond-> {:suite/id sid :project p} v (assoc :variant v))))]
        (if (seq cells)
          (r/ok cells)
          (r/err :compose/empty-matrix
                 (cond-> {:suites suite-ids :projects project-filter
                          :message (str "no selected suite applies to the selected projects"
                                        (when project-filter (str " (" (str/join ", " project-filter) ")"))
                                        (when variant-filter (str " and variants (" (str/join ", " variant-filter) ")")))}
                   variant-filter (assoc :variants variant-filter))))))))

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
  ["HIVE_RUN_ID" "HIVE_SUITE_ID" "HIVE_TEST_ID_PREFIX" "HIVE_REPORTS_DIR" "TRACEPARENT" "BAGGAGE"
   ;; set on a variant cell only: the variant's name
   "HIVE_VARIANT"])

(def container-reports-dir
  "Where a container runner writes reports, unless its suite names another
   directory."
  "/reports")

(defn run-dir
  "`<state-dir>/runs/<run>/<cell-key>/<project>`: everything one cell leaves
   behind (`<suite>@<variant>` for a variant cell)."
  [state-dir run {:keys [project] :as cell}]
  (str (str/replace (str state-dir) #"/+\z" "") "/runs/" run "/" (cell-key cell) "/" project))

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
   then `otlp`, then `suite-env`, then `correlation`, forced on top. At each
   layer `otel/otlp-pair-merge` keeps OTEL_EXPORTER_OTLP_ENDPOINT and
   OTEL_EXPORTER_OTLP_PROTOCOL one unit. OTEL_RESOURCE_ATTRIBUTES is merged
   per key through `merge-ra` (fn [base over], default
   `otel/merge-resource-attributes`) in the same order, wiring < otlp <
   suite. Then the `hive.*` correlation keys are forced: those of the otlp
   attributes, plus `hive.run.id=<run>` and `hive.test.id=<test-id>` always.
   The suite still sets service.name and its own attributes."
  [{:keys [wiring otlp suite-env correlation merge-ra run test-id]}]
  (let [merge-ra (or merge-ra otel/merge-resource-attributes)
        env (-> (otel/otlp-pair-merge (or wiring {}) (or otlp {}))
                (otel/otlp-pair-merge (or suite-env {}))
                (merge correlation))
        ras (keep #(let [v (get % resource-attributes)] (when (string? v) v)) [wiring otlp suite-env])
        forced (otel/render-resource-attributes
                (cond-> (into {} (filter (comp otel/correlation-attribute? key))
                              (otel/parse-resource-attributes (get otlp resource-attributes)))
                  run (assoc "hive.run.id" run)
                  test-id (assoc "hive.test.id" test-id)))]
    (if (seq ras)
      (assoc env resource-attributes (reduce merge-ra (concat ras [forced])))
      env)))

(defn path-part?
  "True when `s` is one safe path segment: `hive-compose.boundary.runs/PathPart`
   (letters, digits, `_` `.` `-`, starting with a letter or digit, never only
   dots). Suite and project ids name directories under the run dir and the
   container name, so anything else (`..`, `a/../b`, `/abs`, `a:b`, `a@b`) is
   refused. A variant cell's `<suite>@<variant>` key is built from two such
   parts, never parsed from one."
  [s]
  (boolean (and (string? s)
                (re-matches #"[A-Za-z0-9][A-Za-z0-9_.-]*" s)
                (not (re-matches #"\.+" s)))))

(defn- relative-inside?
  "True when `path` is relative and never climbs out with a `..` segment."
  [path]
  (and (not (str/starts-with? path "/"))
       (not-any? #{".."} (str/split path #"/"))))

(defn- join-path
  "`path` under `dir` (both checked by the caller: `path` relative and inside)."
  [dir path]
  (str (str/replace dir #"/+\z" "") "/" path))

(defn container-name
  "`hive-<run>-<slug suite>-<slug project>-<8 hex>`: the slugs keep it
   readable, the hash of the exact ids keeps two cells whose ids slug alike
   (\"a.b\" and \"a-b\") from sharing a container name."
  [run sid project]
  (str "hive-" run "-" (schema/slug sid) "-" (schema/slug project) "-"
       (format "%08x" (bit-and (hash [sid project]) 0xffffffff))))

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
     :default-resources optional, `:compose/default-resources`

   A suite with `:suite/image` gives a container:
     {:runner :container :image :command? :env :mounts [{:host :container}]
      :network (the view network of the project under test) :labels :name}
   Any other suite gives a host command {:runner :command :command :dir? :env}.
   A container of a suite with `:suite/needs` also joins the view network of
   `suite:<id>` (:extra-networks), where the suite's own instances answer.
   Both carry :cell :run :test-id :trace-id :reports-dir and :report-format,
   :resources (`:suite/resources`, else `:default-resources`; `:unbounded`
   passes through) and :timeout-ms when set. :reports-dir is always the
   cell's own `<state-dir>/runs/<run>/<suite>/<project>/reports`, so no two
   cells share one. A command that names `:suite/reports-dir` (relative to
   `:suite/dir`, never leaving it) also carries it as :collect-from: the
   pipeline empties it before the cell and moves what the runner leaves there
   into :reports-dir after.

   Refused: an unknown suite, a bad run id, a suite or project id that is not
   `path-part?`, a command's reports dir that is absolute, climbs out with
   `..` or has no `:suite/dir` to be relative to (:compose/invalid-param), and
   an env holding half the OTLP pair (:compose/otlp-pair-broken)."
  [{:keys [suites run state-dir id-source wiring-env] :as ctx} cell]
  (let [sid (:suite/id cell)
        project (:project cell)
        suite (get suites sid)
        declared (:suite/reports-dir suite)]
    (cond
      (not (path-part? sid))
      (r/err :compose/invalid-param {:param "suite" :value sid
                                     :message (str "not a safe suite id: " (pr-str sid))})

      (not (path-part? project))
      (r/err :compose/invalid-param {:param "project" :value project
                                     :message (str "not a safe project id: " (pr-str project))})

      (nil? suite)
      (r/err :compose/unknown-names {:suites [sid] :message (str "unknown suite: " sid)})

      (and (:variant cell) (or (not (path-part? (:variant cell)))
                               (not (contains? (:suite/variants suite) (:variant cell)))))
      (r/err :compose/unknown-names {:variants [(:variant cell)]
                                     :message (str "suite " sid " declares no variant " (pr-str (:variant cell)))})

      (not (m/validate schema/RunId run))
      (r/err :compose/invalid-run-id {:run run :message (str "not a run id: " (pr-str run))})

      (str/blank? (str state-dir))
      (r/err :compose/invalid-param {:param "state-dir" :message "runner-spec needs a :state-dir"})

      (nil? id-source)
      (r/err :compose/invalid-param {:param "id-source" :message "runner-spec needs an :id-source"})

      (and declared (nil? (:suite/image suite))
           (not (and (:suite/dir suite) (relative-inside? declared))))
      (r/err :compose/invalid-param {:param "suite/reports-dir" :suite sid :value declared
                                     :message (str "a command's :suite/reports-dir must be relative to its "
                                                   ":suite/dir and stay inside it: " (pr-str declared))})

      :else
      (let [image (:suite/image suite)
            container? (some? image)
            state-reports (reports-dir state-dir run cell)
            collect-from (when-not container? (some->> declared (join-path (:suite/dir suite))))
            runner-reports (if container? (or declared container-reports-dir) state-reports)
            suite-net (when (seq (:suite/needs suite)) (schema/view-network run (suite-consumer sid)))
            resources (or (:suite/resources suite) (:default-resources ctx))
            tid (test-id cell)]
        (r/let-ok [tp (traceparent id-source)
                   otlp-vars (otlp (:otlp-env ctx) {:run-id run
                                                    :service (str "suite-" sid)
                                                    :test-id tid
                                                    :side (if container? :container :host)})]
          (let [variant (:variant cell)
                env (layer-env {:wiring wiring-env
                                :otlp otlp-vars
                                ;; a variant's env goes over the suite's own
                                :suite-env (merge (:suite/env suite)
                                                  (when variant (get-in suite [:suite/variants variant])))
                                :merge-ra (:merge-resource-attributes ctx)
                                :run run
                                :test-id tid
                                :correlation (cond-> {"HIVE_RUN_ID" run
                                                      "HIVE_SUITE_ID" sid
                                                      "HIVE_TEST_ID_PREFIX" (test-id-prefix cell)
                                                      "HIVE_REPORTS_DIR" runner-reports
                                                      "TRACEPARENT" (:traceparent tp)
                                                      "BAGGAGE" (baggage cell)}
                                               variant (assoc "HIVE_VARIANT" variant))})
                common (cond-> {:cell (cond-> {:suite/id sid :project project} variant (assoc :variant variant))
                                :run run
                                :test-id tid
                                :trace-id (:trace-id tp)
                                :env (into (sorted-map) env)
                                :reports-dir state-reports
                                :report-format (:suite/report-format suite :auto)}
                         (:suite/timeout-ms suite) (assoc :timeout-ms (:suite/timeout-ms suite))
                         resources (assoc :resources resources)
                         collect-from (assoc :collect-from collect-from))
                command (:suite/command suite)]
            (if (otel/otlp-pair-broken? env)
              (r/err :compose/otlp-pair-broken {:cell (:cell common)
                                                :present (vec (filter #(contains? env %) otel/exporter-pair))
                                                :pair otel/exporter-pair
                                                :message "an OTLP endpoint without its protocol (or the reverse)"})
              (r/ok
               (if container?
                 (cond-> (assoc common
                                :runner :container
                                :image image
                                :name (container-name run (cell-key cell) project)
                                :mounts [{:host state-reports :container runner-reports}]
                                :network (schema/view-network run project)
                                :labels {(:run schema/docker-labels) run
                                         (:consumer schema/docker-labels) (suite-consumer sid)})
                   command (assoc :command command)
                   (:suite/pull-timeout-ms suite) (assoc :pull-timeout-ms (:suite/pull-timeout-ms suite))
                   suite-net (assoc :extra-networks [suite-net]))
                 (cond-> (assoc common :runner :command :command command)
                   (:suite/dir suite) (assoc :dir (:suite/dir suite))))))))))))

(def RunnerSpec
  "What `runner-spec` answers. Kept here because the stack schema belongs to
   core."
  [:and
   [:map
    [:runner [:enum :container :command]]
    [:cell [:map [:suite/id :string] [:project :string] [:variant {:optional true} :string]]]
    [:run schema/RunId]
    [:test-id :string]
    [:trace-id [:re #"^[0-9a-f]{32}\z"]]
    [:env schema/Env]
    [:reports-dir [:string {:min 1}]]
    [:report-format schema/ReportFormat]
    [:resources {:optional true} schema/Resources]
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

(defn report-required?
  "Whether a cell of `suite` must leave a report to pass: when it says so with
   `:suite/report-required?`, else when it is a container suite
   (`:suite/image`: it always has its reports mount) or declares where or how
   it reports (`:suite/reports-dir` or `:suite/report-format`). Only an
   explicit `:suite/report-required? false` opts out (its exit code is then
   the verdict when no report is found)."
  [suite]
  (if (contains? suite :suite/report-required?)
    (boolean (:suite/report-required? suite))
    ;; a container suite always has its reports mount: it must report too
    (boolean (or (:suite/image suite) (:suite/reports-dir suite) (:suite/report-format suite)))))

(defn verdict-policy
  "The cell-verdict policy of `suite` for a call whose `skip-fails?` is given
   (nil: the suite's own `:suite/skip-fails?`, default false):
   {:report-required? bool :skip-fails? bool}."
  [suite skip-fails?]
  {:report-required? (report-required? suite)
   :skip-fails? (boolean (if (some? skip-fails?) skip-fails? (:suite/skip-fails? suite)))})

(defn- skipped-count [report]
  (max (get-in report [:report/totals :skipped] 0)
       (count (filter #(= :skipped (:case/status %)) (:report/cases report)))))

(defn cell-verdict
  "{:status :passed|:failed|:error :reason kw?} of a cell that ended with
   `exit` (nil: killed or timed out) and `report` (a Result<Report> as the
   reader answers it, a bare Report, or nil for none), under `policy`
   ({:report-required? :skip-fails? :timed-out? :oom?}, all default false).

   The law: :passed ONLY when the runner ran to a clean exit 0, was neither
   killed nor timed out, its report (when one is required) parsed completely
   and holds at least one case, and no case failed. A test saying no is
   :failed; anything that could not run, or whose report cannot be trusted,
   is :error (exit 2).
   - A report error other than :report/none (malformed, truncated,
     unreadable, uncollectable): :error with the report category as reason,
     whatever the exit code and whatever :report-required? says.
   - No report: :error (reason :report/none) when one is required, :error
     (reason :runner/oom or :runner/timeout) when it was killed, else the
     exit code is the verdict (0 :passed, else :error: it died before it
     reported).
   - A required report with no case: :error (reason :report/empty).
   - Failed or errored cases: :failed (reason :cases/failed).
   - Killed (OOM, timeout) with no failed case: :error (reason as above).
   - A bail-out, or a non-zero or missing exit: :failed.
   - A skipped case under :skip-fails?: :failed :skipped-as-failure (the
     case itself stays :skipped in the report).
   - Anything else: :passed."
  ([exit report] (cell-verdict exit report nil))
  ([exit report {:keys [report-required? skip-fails? timed-out? oom?]}]
   (let [clean? (and (number? exit) (zero? exit))
         [rep rerr] (cond (r/ok? report) [(:ok report) nil]
                          (r/err? report) [nil (:error report)]
                          :else [report nil])
         killed (cond oom? :runner/oom timed-out? :runner/timeout)]
     (cond
       (and rerr (not= :report/none rerr)) {:status :error :reason rerr}
       (nil? rep) (cond report-required? {:status :error :reason :report/none}
                        killed {:status :error :reason killed}
                        clean? {:status :passed}
                        :else {:status :error :reason (if (nil? exit) :runner/no-exit :runner/died)})
       (and report-required? (empty? (:report/cases rep))) {:status :error :reason :report/empty}
       (pos? (failures rep)) {:status :failed :reason :cases/failed}
       killed {:status :error :reason killed}
       (:report/bail-out rep) {:status :failed :reason :report/bail-out}
       (not clean?) {:status :failed :reason (if (nil? exit) :runner/no-exit :runner/exit)}
       (and skip-fails? (pos? (skipped-count rep))) {:status :failed :reason :skipped-as-failure}
       :else {:status :passed}))))

(defn cell-status
  "How a cell ended: the :status of `cell-verdict`."
  ([exit report] (:status (cell-verdict exit report nil)))
  ([exit report policy] (:status (cell-verdict exit report policy))))

(defn cell-result
  "The run result of `cell` in `run`, in the shape hc-13 `record-run!` takes:
     {:run/id :suite/id :project :variant? :exit :status :passed|:failed|:error
      :reason? :report Report?  :report-error {:error :message ..}?
      :started-at :finished-at :duration-ms  :oom Finding?}
   `:suite/id` is the cell's `cell-key` (`<suite>@<variant>` for a variant
   cell), so the flake history and `why` keep variants apart; `:variant`
   names the variant. `outcome` is {:exit n-or-nil :report Result<Report>
   :started-at ms :finished-at ms :oom Finding? :policy VerdictPolicy?}. A
   report error such as :report/none (nothing written) counts as no report;
   the error is kept under :report-error. `:reason` is why the cell did not
   pass (`cell-verdict`). `:oom` is the runner's OOM finding (hc-16b
   `hive-compose.promote.oom`), kept as is: `why` prints its line and the
   flake history classes the run :oom."
  [run cell {:keys [exit report started-at finished-at oom policy timed-out?]}]
  (let [rep (when (r/ok? report) (:ok report))
        {:keys [status reason]} (cell-verdict exit report (cond-> (or policy {})
                                                            (map? oom) (assoc :oom? true)
                                                            timed-out? (assoc :timed-out? true)))]
    (cond-> {:run/id run
             :suite/id (cell-key cell)
             :project (:project cell)
             :exit exit
             :status status}
      (:variant cell) (assoc :variant (:variant cell))
      (and reason (not= :passed status)) (assoc :reason reason)
      (:skip-fails? policy) (assoc :skip-fails? true)
      rep (assoc :report rep)
      (r/err? report) (assoc :report-error (select-keys report [:error :message :reason :dir]))
      started-at (assoc :started-at started-at)
      finished-at (assoc :finished-at finished-at)
      (and (number? started-at) (number? finished-at))
      (assoc :duration-ms (max 0 (- finished-at started-at)))
      (map? oom) (assoc :oom oom))))

(defn runner-outcome
  "The `cell-result` outcome of an ISuiteRunner answer `answer` (hc-16b
   `hive-compose.port/-run!`: {:exit :started-at :finished-at :oom ..}) and
   the cell's `report` (Result<Report>). A timed-out runner has exit nil."
  [answer report]
  (cond-> {:exit (when-not (:timed-out? answer) (:exit answer))
           :report report}
    (:started-at answer) (assoc :started-at (:started-at answer))
    (:finished-at answer) (assoc :finished-at (:finished-at answer))
    (:timed-out? answer) (assoc :timed-out? true)
    (map? (:oom answer)) (assoc :oom (:oom answer))))

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

(def exit-codes
  "The exit code of a matrix, by its status: 0 every cell passed, 1 a test
   failed (and nothing could not run), 2 something could not run (infra, the
   runner, a missing or empty report, a data phase). 2 wins over 1."
  {:passed 0 :failed 1 :error 2})

(defn matrix-exit
  "The exit code of matrix status `status` (`exit-codes`)."
  [status]
  (get exit-codes status 2))

(defn matrix-summary
  "The cell results folded. The order of `results` does not change it:
     {:status   the worst cell (:error > :failed > :passed; :passed for none)
      :exit     `matrix-exit` of :status: 0 passed, 1 failed, 2 could not run
      :cells n  :passed n  :failed n  :error n
      :cases    the report totals summed {:passed :failed :error :skipped}
      :by-suite {suite {status n}}  :by-project {project {status n}}
      :failures [{:suite/id :project :status :exit :reason? :cases [case ids]}], sorted
      :duration-ms  the sum over the cells that report one}"
  [results]
  (let [counts (merge {:passed 0 :failed 0 :error 0} (frequencies (map :status results)))
        by (fn [k] (into (sorted-map)
                         (map (fn [[id rs]] [id (into (sorted-map) (frequencies (map :status rs)))]))
                         (group-by k results)))
        status (worst (map :status results))]
    (merge {:status status
            :exit (matrix-exit status)
            :cells (count results)
            :cases (apply merge-with + {:passed 0 :failed 0 :error 0 :skipped 0}
                          (map #(select-keys (get-in % [:report :report/totals]) [:passed :failed :error :skipped])
                               results))
            :by-suite (by :suite/id)
            :by-project (by :project)
            :failures (->> results
                           (remove #(= :passed (:status %)))
                           (map #(cond-> (assoc (select-keys % [:suite/id :project :status :exit :reason])
                                                :cases (failed-cases %))
                                   (get-in % [:oom :oom/line]) (assoc :oom/line (get-in % [:oom :oom/line]))))
                           (sort-by (juxt :suite/id :project))
                           vec)
            :duration-ms (reduce + 0 (keep :duration-ms results))}
           (select-keys counts [:passed :failed :error]))))
