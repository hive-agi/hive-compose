(ns hive-compose.promote.why
  "Pure: the failure bundle that answers `compose why test=T` (hc-10).

   From the case that failed (a Report case, hc-8), its cell result, the
   traces carrying its test id, the container logs of the run and the stack
   plan, `bundle` folds one FailureBundle:

     {:why/test-id :why/run :why/window [start-ms end-ms]
      :why/case {:case/id :case/name :case/status :case/message ..}
      :why/cell {:suite/id :project :status :exit
                 :oom/service :oom/cap :oom/line}   the :oom keys only when
                                                    the runner was OOM-killed
      :why/failing-span {:span/id :trace/id :span/service :span/name
                         :span/status :span/status-message :span/attrs}?
      :why/span-path [{:span/id :span/service :span/name :span/status
                       :span/status-message}]  root first, failing span last
      :why/traces [{:trace/id :trace/root-name :errors n :spans n}]
      :why/logs {service [line]}   lines inside the window +- slack
      :why/plan {:run :stack :status :instances :consumers}?
      :why/summary \"one line\"}

   `render-markdown` writes it for a human. `log-window` is the timestamp
   filter of log lines. No IO: the pipeline (`hive-compose.pipeline.why`)
   reads the files, queries the trace store and the engine."
  (:require [clojure.pprint :as pprint]
            [clojure.string :as str]
            [hive-compose.promote.trace :as trace])
  (:import (java.time Instant OffsetDateTime)
           (java.time.format DateTimeParseException)))

;; SPDX-License-Identifier: MIT

;; ---------------------------------------------------------------- logs

(def default-slack-ms
  "How far before and after the case window log lines are still kept."
  2000)

(def max-log-lines
  "Lines kept per service in a bundle; the last ones win (nearest the failure)."
  200)

(def ^:private ts-re
  #"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:[.,]\d{1,9})?(?:Z|[+-]\d{2}:?\d{2}))")

(defn- parse-ts [^String s]
  (let [s (str/replace s "," ".")
        s (if (re-find #"[+-]\d{4}\z" s) (str (subs s 0 (- (count s) 2)) ":" (subs s (- (count s) 2))) s)]
    (try
      (if (str/ends-with? s "Z")
        (.toEpochMilli (Instant/parse s))
        (.toEpochMilli (.toInstant (OffsetDateTime/parse s))))
      (catch DateTimeParseException _unparseable nil))))

(defn line-ms
  "Epoch ms of the first RFC 3339 timestamp in log line `line` (docker's
   `--timestamps` prefix, or one the service wrote), nil when it holds none."
  [line]
  (some-> (re-find ts-re (str line)) second parse-ts))

(defn log-window
  "The lines of log `text` that fall inside `[start-ms end-ms]` widened by
   `slack-ms` on both sides, in order. A line with no timestamp continues the
   timestamped line before it (a stack trace) and is kept with it; untimed
   lines before the first timestamp are dropped. A nil bound is open. Widening
   the window never drops a line it kept."
  ([text window] (log-window text window default-slack-ms))
  ([text [start-ms end-ms] slack-ms]
   (let [lo (some-> start-ms (- (long slack-ms)))
         hi (some-> end-ms (+ (long slack-ms)))
         in? (fn [t] (and (or (nil? lo) (>= t lo)) (or (nil? hi) (<= t hi))))]
     (loop [lines (str/split-lines (str text)) keep? false out []]
       (if-let [[l & more] (seq lines)]
         (let [t (line-ms l)
               k (if t (in? t) keep?)]
           (recur more k (if (and k (not (str/blank? l))) (conj out l) out)))
         out)))))

(def ^:private compose-prefix-re #"^(\S+?)(?:-\d+)?\s+\|\s?(.*)$")

(defn split-compose-logs
  "{service text} of `docker compose logs` output `text`, each line keyed by
   the service its `<service>-<n>  | ` prefix names (the line kept whole).
   Lines with no prefix go under \"\"."
  [text]
  (->> (str/split-lines (str text))
       (remove str/blank?)
       (group-by #(or (second (re-matches compose-prefix-re %)) ""))
       (into (sorted-map) (map (fn [[k ls]] [k (str/join "\n" ls)])))))

;; ---------------------------------------------------------------- spans

(defn- start-key [s]
  (or (:span/start-ns s) (some-> (:span/start-ms s) (* 1000000)) 0))

(defn- rank
  "[message? depth] of error span `s` in `index` (higher explains more), nil
   when its parent chain loops."
  [index s]
  (when-let [d (trace/depth index s)]
    [(if (:span/status-message s) 1 0) d]))

(defn- order-key [[m d] s tie]
  [(- m) (- d) (start-key s) (str tie)])

(defn trace-failing-span
  "The span of `trace` that explains its failure: the deepest error span
   carrying a status message (otelhttp blanks the message of a 5xx server
   span, so the cause sits on a child), else the deepest error span, as
   `trace/failing-span`. Ties go to the earliest start, then the least id.
   Nil when no span errs below a root."
  [trace]
  (let [index (trace/by-id trace)]
    (->> (trace/error-spans trace)
         (keep (fn [s] (when-let [k (rank index s)] [k s])))
         (sort-by (fn [[k s]] (order-key k s (:span/id s))))
         first
         second)))

(defn failing-span
  "[trace span] explaining the failure across `traces`: each trace's
   `trace-failing-span`, then the one with a message, the deepest, the
   earliest, the least trace id. Nil when none errs."
  [traces]
  (->> traces
       (keep (fn [t] (when-let [s (trace-failing-span t)]
                       [(rank (trace/by-id t) s) t s])))
       (sort-by (fn [[k t s]] (order-key k s (:trace/id t))))
       first
       rest
       vec
       not-empty))

(defn- span-row [s]
  (select-keys s [:span/id :span/service :span/name :span/status :span/status-message]))

(defn span-path
  "[span-row] from the root of `trace` down to span `s`, root first, `s` last."
  [trace s]
  (mapv span-row (reverse (trace/ancestry (trace/by-id trace) s))))

(defn trace-row
  "A trace in a bundle: id, root, how many spans and errors."
  [t]
  (let [index (trace/by-id t)
        root (first (filter #(trace/root? index %) (:trace/spans t)))]
    {:trace/id (:trace/id t)
     :trace/root-service (:span/service root)
     :trace/root-name (:span/name root)
     :spans (count (:trace/spans t))
     :errors (count (trace/error-spans t))}))

;; ---------------------------------------------------------------- bundle

(defn plan-summary
  "A stack-status row (hc-6 `stack-status`) or a StackPlan, reduced to what a
   bundle needs: run, stack, status, instances {id {:compose-project :shared?
   :services}} and consumers {id {:compose-project :services}}. Nil for nil."
  [plan]
  (when (map? plan)
    (if (contains? plan :stack/instances)
      {:run (:stack/run plan)
       :stack (:stack/id plan)
       :instances (into (sorted-map)
                        (for [[iid ip] (:stack/instances plan)]
                          [iid {:compose-project (:instance/compose-project ip)
                                :shared? (boolean (:instance/share? ip))
                                :services (:instance/services ip)}]))
       :consumers (into (sorted-map)
                        (for [[c cp] (:stack/consumers plan)]
                          [c {:network (:consumer/network cp)}]))}
      (select-keys plan [:run :stack :status :started-at :instances :consumers]))))

(defn case-window
  "[start-ms end-ms] of a case: `window` when given, else the cell's
   :started-at/:finished-at."
  [window cell-result]
  (or (not-empty (vec window))
      [(:started-at cell-result) (:finished-at cell-result)]))

(defn- clip-lines [lines]
  (let [n (count lines)]
    (if (> n max-log-lines) (subvec lines (- n max-log-lines)) lines)))

(defn summary-line
  "One line: what failed, the span that explains it and, when the cell's
   runner was OOM-killed, its why line ('<service> OOM-killed at <cap>')."
  [{:why/keys [test-id case failing-span cell]}]
  (let [base (str (or test-id (:case/id case) "?") " " (name (or (:case/status case) :failed))
                  (if failing-span
                    (str ": " (trace/describe-span failing-span))
                    (when-let [m (:case/message case)] (str ": " (first (str/split-lines m))))))
        oom (:oom/line cell)]
    (if (and oom (not (str/includes? base oom)))
      (str base "; " oom)
      base)))

(defn cell-row
  "A cell result in a bundle: suite, project, status, exit and, when its
   runner was OOM-killed (hc-16b), the finding's service, cap and line."
  [cell-result]
  (merge (select-keys cell-result [:suite/id :project :status :exit])
         (select-keys (:oom cell-result) [:oom/service :oom/cap :oom/line])))

(defn bundle
  "The FailureBundle of one failed case (see the ns doc). `in`:
     :case        a Report case {:case/id :case/name :case/status :case/message ..}
     :test-id     the test id the spans carry (default the case id)
     :cell-result the hc-8 cell result {:run/id :suite/id :project :exit :status
                  :started-at :finished-at ..}
     :traces      [Trace] found for the case
     :logs        {service text}
     :plan        a stack-status row or StackPlan, optional
     :window      [start-ms end-ms], default the cell's
     :slack-ms    default `default-slack-ms`
   The failing span, when there is one, is a span of one of `traces`, and the
   span path runs from that trace's root down to it."
  [{:keys [case test-id cell-result traces logs plan window slack-ms]}]
  (let [w (case-window window cell-result)
        slack (or slack-ms default-slack-ms)
        [t s] (failing-span traces)
        b {:why/test-id (or test-id (:case/id case))
           :why/run (:run/id cell-result)
           :why/window w
           :why/case (select-keys case [:case/id :case/name :case/classname :case/status
                                        :case/message :case/time-s])
           :why/cell (cell-row cell-result)
           :why/traces (mapv trace-row (sort-by :trace/id traces))
           :why/logs (into (sorted-map)
                           (keep (fn [[svc text]]
                                   (let [ls (clip-lines (log-window text w slack))]
                                     (when (seq ls) [svc ls]))))
                           logs)
           :why/span-path (if s (span-path t s) [])}
        b (cond-> b
            s (assoc :why/failing-span
                     (assoc (select-keys s [:span/id :span/service :span/name :span/status
                                            :span/status-message :span/attrs :span/start-ms
                                            :span/duration-ms])
                            :trace/id (:trace/id t)))
            plan (assoc :why/plan (plan-summary plan)))]
    (assoc b :why/summary (summary-line b))))

;; ---------------------------------------------------------------- markdown

(defn- fence
  "`text` in a code fence longer than any backtick run inside it."
  [text]
  (let [longest (reduce max 2 (map count (re-seq #"`+" (str text))))
        f (apply str (repeat (inc longest) \`))]
    (str f "\n" text "\n" f)))

(defn- md-inline [s]
  (str/replace (str s) #"([\\`*_\[\]|<>])" "\\\\$1"))

(defn render-markdown
  "FailureBundle `b` as Markdown: summary, case, failing span with its
   attributes, the span path from the root, traces, plan and logs."
  [b]
  (let [{:why/keys [test-id run window case cell failing-span span-path traces logs plan summary]} b
        line (fn [k v] (when (some? v) (str "- **" k "**: " (md-inline v))))]
    (str/join
     "\n"
     (concat
      [(str "# why " (md-inline test-id)) ""
       (md-inline summary) ""
       "## Case" ""]
      (keep identity [(line "id" (:case/id case))
                      (line "status" (some-> (:case/status case) name))
                      (line "run" run)
                      (line "suite" (:suite/id cell))
                      (line "project" (:project cell))
                      (line "exit" (:exit cell))
                      (line "oom" (:oom/line cell))
                      (line "window" (str (first window) " .. " (second window)))])
      (when-let [m (:case/message case)] ["" (fence m)])
      ["" "## Failing span" ""]
      (if failing-span
        (concat
         (keep identity [(line "service" (:span/service failing-span))
                         (line "name" (:span/name failing-span))
                         (line "message" (:span/status-message failing-span))
                         (line "trace" (:trace/id failing-span))
                         (line "span" (:span/id failing-span))])
         (when-let [attrs (not-empty (:span/attrs failing-span))]
           (concat ["" "| attribute | value |" "|---|---|"]
                   (for [[k v] (sort-by (comp str key) attrs)]
                     (str "| " (md-inline k) " | " (md-inline (pr-str v)) " |")))))
        ["No error span found for this test."])
      (when (seq span-path)
        (concat ["" "## Span path" ""]
                (map-indexed (fn [i s]
                               (str (apply str (repeat (* 2 i) \space)) "- "
                                    (md-inline (trace/describe-span s))))
                             span-path)))
      ["" "## Traces" ""]
      (if (seq traces)
        (for [t traces]
          (str "- " (:trace/id t) " " (md-inline (:trace/root-name t))
               " (" (:spans t) " spans, " (:errors t) " errors)"))
        ["None found."])
      (when plan
        ["" "## Plan" "" (fence (with-out-str (pprint/pprint plan)))])
      (when (seq logs)
        (concat ["" "## Logs" ""]
                (mapcat (fn [[svc ls]] [(str "### " (md-inline (if (= "" svc) "(unprefixed)" svc))) ""
                                        (fence (str/join "\n" ls)) ""])
                        logs)))
      [""]))))
