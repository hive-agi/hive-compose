(ns hive-compose.promote.trace
  "Traces as values: the TraceQL that finds a test's traces, the Tempo HTTP
   answers read into data, and the span that explains a failure. Pure: the
   adapter (`hive-compose.adapter.tempo`) does the HTTP, this namespace only
   reads and writes strings and maps.

   TraceSummary (an `-find-traces` row):
     {:trace/id \"<32 hex>\" :trace/root-service :trace/root-name
      :trace/start-ms :trace/duration-ms}

   Trace (a `-trace` answer):
     {:trace/id \"<32 hex>\"
      :trace/spans [{:span/id \"<16 hex>\" :span/parent-id \"<16 hex>\"|nil
                     :span/name :span/service :span/start-ms :span/duration-ms
                     :span/status :ok|:error|:unset :span/status-message
                     :span/attrs {\"key\" value}}]}

   Tempo answers OTLP JSON in its protobuf mapping: ids base64 (the OTLP
   JSON spec says hex; both are read, told apart by their alphabet and
   length), int64 as strings, attribute values as {stringValue|intValue|
   boolValue|doubleValue|arrayValue|kvlistValue|bytesValue}, spans under
   resourceSpans (or the older batches) and scopeSpans (or
   instrumentationLibrarySpans). Span attributes and resource attributes are
   merged into :span/attrs, the span's own winning."
  (:require [clojure.string :as str])
  (:import (java.util Base64)))

;; SPDX-License-Identifier: MIT

;; ---------------------------------------------------------------- TraceQL

(def default-scopes
  "The TraceQL scope an attribute is matched in when its key names none:
   hive.run.id travels in OTEL_RESOURCE_ATTRIBUTES, test.id in baggage
   copied onto spans. Any other key is matched unscoped (`.key`)."
  {"hive.run.id" "resource"
   "test.id" "span"})

(defn escape
  "`s` as the body of a TraceQL double-quoted string: backslash and quote
   escaped, control characters written as escapes, so the literal ends only
   at its closing quote and stays on one line."
  [s]
  (let [sb (StringBuilder.)]
    (doseq [c (str s)]
      (case c
        \\ (.append sb "\\\\")
        \" (.append sb "\\\"")
        \newline (.append sb "\\n")
        \return (.append sb "\\r")
        \tab (.append sb "\\t")
        (if (< (int c) 0x20)
          (.append sb (format "\\u%04x" (int c)))
          (.append sb c))))
    (str sb)))

(defn string-literal
  "`s` as a TraceQL string literal, quotes included."
  [s]
  (str "\"" (escape s) "\""))

(def ^:private bare-name #"[A-Za-z_][A-Za-z0-9_.:/-]*")

(defn- attribute-name
  "The TraceQL name of attribute `k` in `scope`: bare when it is a plain
   dotted name, quoted otherwise (`span.\"a b\"`)."
  [scope k]
  (str scope "." (if (re-matches bare-name k) k (string-literal k))))

(defn attribute
  "The TraceQL attribute reference of key `k`. A key that names its scope
   (\"span.x\", \"resource.x\") keeps it, one starting with \".\" is
   unscoped; any other takes its scope from `default-scopes`, else none."
  [k]
  (let [k (str k)]
    (cond
      (str/starts-with? k "span.") (attribute-name "span" (subs k 5))
      (str/starts-with? k "resource.") (attribute-name "resource" (subs k 9))
      (str/starts-with? k ".") (attribute-name "" (subs k 1))
      :else (attribute-name (get default-scopes k "") k))))

(defn- literal
  "A TraceQL literal of value `v`: numbers and booleans bare, anything else a string."
  [v]
  (cond
    (boolean? v) (str v)
    (integer? v) (str v)
    (and (number? v) (Double/isFinite (double v))) (str (double v))
    :else (string-literal (if (keyword? v) (name v) (str v)))))

(defn traceql
  "The TraceQL spanset filter matching every entry of `attrs` ({key value}),
   keys sorted so equal maps give equal queries: `{ span.test.id = \"t\" &&
   resource.hive.run.id = \"r\" }`. Empty `attrs` matches every span: `{ }`."
  [attrs]
  (if (empty? attrs)
    "{ }"
    (str "{ "
         (str/join " && " (for [[k v] (sort-by (comp str key) attrs)]
                            (str (attribute k) " = " (literal v))))
         " }")))

(defn search-params
  "{param value} of Tempo's /api/search for `query` ({:attrs :start-ms
   :end-ms :limit}): q (TraceQL), start and end in epoch seconds (start
   rounded down, end up, so the window is never narrowed), limit."
  [{:keys [attrs start-ms end-ms limit]}]
  (cond-> {"q" (traceql attrs)}
    start-ms (assoc "start" (str (quot (long start-ms) 1000)))
    end-ms (assoc "end" (str (quot (+ (long end-ms) 999) 1000)))
    limit (assoc "limit" (str (long limit)))))

;; ---------------------------------------------------------------- ids

(def ^:private hex-chars "0123456789abcdef")

(defn- bytes->hex [^bytes bs]
  (let [sb (StringBuilder.)]
    (doseq [b bs]
      (let [v (bit-and (int b) 0xff)]
        (.append sb (.charAt ^String hex-chars (bit-shift-right v 4)))
        (.append sb (.charAt ^String hex-chars (bit-and v 0xf)))))
    (str sb)))

(defn- pad-hex
  "Hex `s` lower-cased and left-padded with zeros to `width` (Tempo's search
   drops a trace id's leading zeros)."
  [s width]
  (let [s (str/lower-case s)]
    (if (< (count s) width)
      (str (apply str (repeat (- width (count s)) \0)) s)
      s)))

(defn hex-id?
  "True when `s` is a trace or span id in hex: 1 to 32 hex digits."
  [s]
  (boolean (and (string? s) (re-matches #"[0-9a-fA-F]{1,32}" s))))

(defn normalize-id
  "The lower-case hex of OTLP id `s` of `width` hex digits (32 for a trace,
   16 for a span): hex as is (padded), else base64 decoded. Base64 of a
   trace or span id always ends in `=` (16 and 8 bytes), never a hex digit,
   so the two never read alike. Nil for a blank or unreadable id, or one
   longer than `width`."
  [s width]
  (when (and (string? s) (not (str/blank? s)))
    (if (re-matches #"[0-9a-fA-F]+" s)
      (when (<= (count s) width) (pad-hex s width))
      (try (let [bs (.decode (Base64/getDecoder) ^String s)]
             (when (<= 1 (alength bs) (quot width 2))
               (pad-hex (bytes->hex bs) width)))
           (catch IllegalArgumentException _not-base64 nil)))))

;; ---------------------------------------------------------------- values

(defn- ->long
  "The long of `v` (a number or a decimal string), nil when it is none or
   lies outside the long range."
  [v]
  (cond
    (instance? Long v) v
    (integer? v) (when (<= Long/MIN_VALUE v Long/MAX_VALUE) (long v))
    (number? v) (let [d (double v)]
                  (when (and (Double/isFinite d) (<= -9.2e18 d 9.2e18)) (long d)))
    (string? v) (try (Long/parseLong (str/trim v)) (catch NumberFormatException _malformed nil))
    :else nil))

(defn- ->double [v]
  (cond
    (number? v) (double v)
    (string? v) (try (Double/parseDouble (str/trim v)) (catch NumberFormatException _malformed nil))
    :else nil))

(defn any-value
  "The Clojure value of an OTLP AnyValue map: string, long, boolean, double,
   vector (arrayValue), map (kvlistValue) or base64 string (bytesValue).
   Nil when it carries none."
  [v]
  (when (map? v)
    (cond
      (contains? v "stringValue") (str (get v "stringValue"))
      (contains? v "intValue") (->long (get v "intValue"))
      (contains? v "boolValue") (let [b (get v "boolValue")] (if (string? b) (= "true" b) (boolean b)))
      (contains? v "doubleValue") (->double (get v "doubleValue"))
      (contains? v "arrayValue") (mapv any-value (get-in v ["arrayValue" "values"]))
      (contains? v "kvlistValue") (into {} (for [kv (get-in v ["kvlistValue" "values"])]
                                             [(get kv "key") (any-value (get kv "value"))]))
      (contains? v "bytesValue") (str (get v "bytesValue"))
      :else nil)))

(defn attributes
  "{key value} of an OTLP attribute list [{\"key\" k \"value\" AnyValue}]."
  [kvs]
  (into {} (keep (fn [kv]
                   (when-let [k (get kv "key")]
                     [k (any-value (get kv "value"))])))
        kvs))

(defn- nanos->ms
  "Epoch ms (long, rounded down) of a unix-nano value (string or number)."
  [v]
  (some-> (->long v) (quot 1000000)))

(defn status
  "The :ok, :error or :unset of an OTLP span status code (2/1/0 or
   STATUS_CODE_ERROR/STATUS_CODE_OK/STATUS_CODE_UNSET)."
  [code]
  (case (if (string? code) (str/upper-case code) code)
    (2 "2" "STATUS_CODE_ERROR" "ERROR") :error
    (1 "1" "STATUS_CODE_OK" "OK") :ok
    :unset))

;; ---------------------------------------------------------------- reading

(defn search-result
  "[TraceSummary] of a Tempo /api/search answer (parsed JSON, string keys).
   No traces is []."
  [body]
  (vec (for [t (get body "traces")
             :let [id (normalize-id (get t "traceID") 32)]
             :when id]
         {:trace/id id
          :trace/root-service (get t "rootServiceName")
          :trace/root-name (get t "rootTraceName")
          :trace/start-ms (nanos->ms (get t "startTimeUnixNano"))
          :trace/duration-ms (or (->long (get t "durationMs")) 0)})))

(defn- span
  "A Span of OTLP span map `s` exported by a resource of `resource-attrs`.
   :span/start-ns keeps the exact start, so spans starting within one ms
   still order."
  [resource-attrs s]
  (let [start (->long (get s "startTimeUnixNano"))
        end (->long (get s "endTimeUnixNano"))
        st (let [st (get s "status")] (if (map? st) st {}))
        msg (get st "message")]
    {:span/id (normalize-id (get s "spanId") 16)
     :span/parent-id (normalize-id (get s "parentSpanId") 16)
     :span/name (some-> (get s "name") str)
     :span/service (some-> (get resource-attrs "service.name") str)
     :span/start-ms (some-> start (quot 1000000))
     :span/start-ns start
     :span/duration-ms (if (and start end) (/ (double (- end start)) 1e6) 0.0)
     :span/status (status (get st "code"))
     :span/status-message (when (and (string? msg) (not (str/blank? msg))) msg)
     :span/attrs (merge resource-attrs (attributes (get s "attributes")))}))

(defn- resource-spans
  "The resourceSpans of a Tempo trace answer: v2 {trace {resourceSpans}},
   OTLP {resourceSpans}, or Tempo v1 {batches}."
  [body]
  (or (get-in body ["trace" "resourceSpans"])
      (get body "resourceSpans")
      (get body "batches")))

(defn trace-result
  "The Trace `trace-id` of a Tempo /api/traces/<id> answer (parsed JSON),
   spans sorted by start then id."
  [trace-id body]
  {:trace/id (or (normalize-id trace-id 32) trace-id)
   :trace/spans (->> (for [rs (resource-spans body)
                           :let [res (attributes (get-in rs ["resource" "attributes"]))]
                           ss (or (get rs "scopeSpans") (get rs "instrumentationLibrarySpans"))
                           s (get ss "spans")]
                       (span res s))
                     (filter :span/id)
                     (sort-by (juxt #(or (:span/start-ms %) 0) :span/id))
                     vec)})

;; ---------------------------------------------------------------- why

(defn error-spans
  "The spans of `trace` whose status is :error, in trace order."
  [trace]
  (filterv #(= :error (:span/status %)) (:trace/spans trace)))

(defn by-id
  "{span-id span} of `trace`'s spans."
  [trace]
  (into {} (map (juxt :span/id identity)) (:trace/spans trace)))

(defn root?
  "True when span `s` has no parent among `index` ({span-id span}): no parent
   id, or a parent the trace does not hold. A root of the trace's forest."
  [index s]
  (not (contains? index (:span/parent-id s))))

(defn ancestry
  "[span] from span `s` up to its root in `index`, `s` first; nil when the
   parent chain loops and reaches no root."
  [index s]
  (loop [cur s acc [] seen #{}]
    (cond
      (seen (:span/id cur)) nil
      (root? index cur) (conj acc cur)
      :else (recur (get index (:span/parent-id cur)) (conj acc cur) (conj seen (:span/id cur))))))

(defn depth
  "How many parents lie between span `s` and its root in `index` (0 for a
   root); nil when its parent chain loops."
  [index s]
  (some-> (ancestry index s) count dec))

(defn- start-key
  "The exact start of span `s` for ordering: its ns when read from OTLP,
   else its ms."
  [s]
  (or (:span/start-ns s) (some-> (:span/start-ms s) (* 1000000)) 0))

(defn failing-span
  "The span that explains `trace`'s failure: the deepest error span (the
   error furthest from its root is closest to the cause), the earliest of
   equally deep ones, then the least id. Nil when no span errs. A span whose
   parent chain loops (no root) is never chosen."
  [trace]
  (let [index (by-id trace)]
    (->> (error-spans trace)
         (keep (fn [s] (when-let [d (depth index s)] [d s])))
         (sort-by (fn [[d s]] [(- d) (start-key s) (:span/id s)]))
         first
         second)))

(defn describe-span
  "One line naming span `s` for a report: service, name, status message."
  [s]
  (str (or (:span/service s) "?") " " (pr-str (:span/name s))
       (when (= :error (:span/status s)) " failed")
       (when-let [m (:span/status-message s)] (str ": " m))))
