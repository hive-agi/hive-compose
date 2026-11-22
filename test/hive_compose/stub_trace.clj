(ns hive-compose.stub-trace
  "An in-memory ITraceQuery: a world of traces (see `hive-compose.promote.trace`
   for their shape), a recording of every call, and faults injectable per
   method. A trace matches a search when one of its spans carries every
   asked attribute (keys with or without their span./resource. scope) and it
   started inside the window."
  (:require [clojure.string :as str]
            [hive-compose.port :as port]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- record! [world call]
  (swap! world update :calls conj call))

(defn- fault [world method]
  (when (contains? (:fail @world) method)
    (r/err :stub/fault {:method method})))

(defn- bare-key
  "`k` without a span./resource./. scope prefix."
  [k]
  (let [k (str k)]
    (cond
      (str/starts-with? k "span.") (subs k 5)
      (str/starts-with? k "resource.") (subs k 9)
      (str/starts-with? k ".") (subs k 1)
      :else k)))

(defn- span-matches? [attrs s]
  (every? (fn [[k v]] (= (str v) (some-> (get (:span/attrs s) (bare-key k)) str))) attrs))

(defn- start-ms [t]
  (reduce min Long/MAX_VALUE (keep :span/start-ms (:trace/spans t))))

(defn- end-ms [t]
  (reduce max 0 (keep (fn [s] (some-> (:span/start-ms s) (+ (long (or (:span/duration-ms s) 0)))))
                      (:trace/spans t))))

(defn summary
  "The TraceSummary of trace `t`: its first root span names it."
  [t]
  (let [ids (set (map :span/id (:trace/spans t)))
        root (or (first (remove #(contains? ids (:span/parent-id %)) (:trace/spans t)))
                 (first (:trace/spans t)))
        start (start-ms t)]
    {:trace/id (:trace/id t)
     :trace/root-service (:span/service root)
     :trace/root-name (:span/name root)
     :trace/start-ms start
     :trace/duration-ms (max 0 (- (end-ms t) start))}))

(defrecord StubTrace [world]
  port/ITraceQuery
  (-find-traces [_ {:keys [attrs start-ms end-ms limit] :as q}]
    (record! world [:find-traces q])
    (or (fault world :find-traces)
        (r/ok (->> (vals (:traces @world))
                   (filter (fn [t] (some #(span-matches? attrs %) (:trace/spans t))))
                   (map summary)
                   (filter (fn [s] (and (or (nil? start-ms) (>= (:trace/start-ms s) start-ms))
                                        (or (nil? end-ms) (<= (:trace/start-ms s) end-ms)))))
                   (sort-by (juxt :trace/start-ms :trace/id))
                   (take (or limit 20))
                   vec))))
  (-trace [_ trace-id]
    (record! world [:trace trace-id])
    (or (fault world :trace)
        (if-let [t (get-in @world [:traces trace-id])]
          (r/ok t)
          (r/err :trace/not-found {:trace-id trace-id}))))
  (-health [_]
    (or (fault world :health) (r/ok {:ready? true}))))

(defn query
  "StubTrace over `traces` ([Trace])."
  ([] (query []))
  ([traces]
   (->StubTrace (atom {:traces (into {} (map (juxt :trace/id identity)) traces) :calls [] :fail #{}}))))

(defn add!
  "Make the store hold `traces` too."
  [q & traces]
  (swap! (:world q) update :traces into (map (juxt :trace/id identity)) traces))

(defn calls [q] (:calls @(:world q)))
(defn fail! [q & methods] (swap! (:world q) update :fail into methods))
