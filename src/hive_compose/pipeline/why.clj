(ns hive-compose.pipeline.why
  "`compose why test=T [run=R]` (hc-10): find the failed case in the run
   results hc-8 wrote, read the traces carrying its test id (ITraceQuery),
   the container logs of its run in the case's window (the engine's `-logs`),
   and the stack plan, fold them into a FailureBundle
   (`hive-compose.promote.why`) and write it beside the run:
   <state-dir>/runs/<run>/why/<case>.md and .edn.

   ctx: the ops/stacks context (:engine :settings :state) plus
     :trace-query  an ITraceQuery (e.g. `hive-compose.adapter.tempo/make-query`);
                   absent, the bundle has no traces and says so
     :state-dir    where runs/ lives; default the state file's directory
     :note!        optional (fn [{:test-id :run :summary :files}]) told about
                   every bundle written (a memory note); its failure is
                   reported, never thrown
     :why/tail     log lines read per compose project (default 2000)
     :why/slack-ms window slack (default `why/default-slack-ms`)"
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [hive-compose.boundary.runs :as runs]
            [hive-compose.pipeline.stacks :as stacks]
            [hive-compose.port :as port]
            [hive-compose.promote.why :as why]
            [hive-compose.schema :as schema]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def default-tail 2000)

(def max-traces
  "Traces fetched whole for one case."
  10)

(defn state-dir
  "Where runs/ lives: `:state-dir`, else the state file's directory."
  [ctx]
  (or (:state-dir ctx)
      (some-> (get-in ctx [:settings :compose/state-file]) io/file .getParent)
      "."))

;; ---------------------------------------------------------------- the case

(defn- leaf
  "A case id without the JUnit suite prefix (`orders-suite/orders.x` -> `orders.x`)."
  [id]
  (last (str/split (str id) #"/")))

(defn case-names
  "Every name test `t` may give case `c` of cell result `res`: its id, its
   name, classname.name, the id without its suite prefix, and
   <suite>/<project>/<id or leaf>."
  [res c]
  (let [id (:case/id c)
        cell (str (:suite/id res) "/" (:project res) "/")]
    (into #{} (remove str/blank?)
          [id (:case/name c) (leaf id)
           (when (:case/classname c) (str (:case/classname c) "." (:case/name c)))
           (str cell id) (str cell (leaf id))])))

(def ^:private bad #{:failed :error})

(defn find-case
  "Result<{:result :case}>: the case `test` names among `results` (cell
   results), a failed or errored one before a passed one, then the newest
   run (latest :finished-at, then run id). :compose/unknown-test lists the
   failed cases there are."
  [results test]
  (let [hits (for [res results
                   c (get-in res [:report :report/cases])
                   :when (contains? (case-names res c) test)]
               {:result res :case c})
        best (first (sort-by (fn [{:keys [result case]}]
                               [(if (bad (:case/status case)) 0 1)
                                (- (or (:finished-at result) 0))
                                (str (:run/id result))])
                             hits))]
    (if best
      (r/ok best)
      (r/err :compose/unknown-test
             {:test test
              :known (vec (take 20 (sort (distinct (for [res results
                                                         c (get-in res [:report :report/cases])
                                                         :when (bad (:case/status c))]
                                                     (str (:suite/id res) "/" (:project res) "/" (:case/id c)))))))
              :hint "test=<case id> or <suite>/<project>/<case id>; run=<run> narrows"}))))

(defn locate
  "Result<{:result :case :skipped}>: the case `test` names in the results
   under the state dir (of `run` when given)."
  [ctx {:keys [test run]}]
  (cond
    (str/blank? (str test)) (r/err :compose/missing-param {:param "test"})
    :else
    (let [{:keys [results skipped]} (runs/read-results (state-dir ctx) run)]
      (if (and (empty? results) (empty? skipped))
        (r/err :compose/no-results {:state-dir (state-dir ctx) :run run
                                    :hint "no runs/<run>/<suite>/<project>/result.edn yet: run compose test first"})
        (r/map-ok (find-case results (str test)) #(assoc % :skipped skipped))))))

;; ---------------------------------------------------------------- traces

(defn trace-queries
  "The searches tried in order for case `c` of cell result `res`, the first
   answering any trace wins: span test.id = the case (its leaf id, then its
   full id), span hive.test.id = the case, then test.id = the cell's test id
   (<suite>/<project>, the runner's own baggage). Each also names
   hive.run.id = the run."
  [res c [start end]]
  (let [run (:run/id res)
        ids (distinct [(leaf (:case/id c)) (:case/id c)])
        q (fn [k v] {:attrs (cond-> {k v} run (assoc "hive.run.id" run))
                     :start-ms start :end-ms end :limit max-traces})]
    (vec (concat (map #(q "test.id" %) ids)
                 (map #(q "span.hive.test.id" %) ids)
                 [(q "test.id" (str (:suite/id res) "/" (:project res)))]))))

(defn fetch-traces
  "{:traces [Trace] :query q :errors [..]}: the traces of the first of
   `queries` that finds any, each read whole. No query finding one answers
   :traces []."
  [tq queries]
  (loop [qs queries errors []]
    (if-let [q (first qs)]
      (let [found (port/-find-traces tq q)]
        (if (and (r/ok? found) (seq (:ok found)))
          (let [got (map #(port/-trace tq (:trace/id %)) (:ok found))]
            {:traces (vec (keep #(when (r/ok? %) (:ok %)) got))
             :query q
             :errors (into errors (filter r/err? got))})
          (recur (rest qs) (cond-> errors (r/err? found) (conj found)))))
      {:traces [] :query nil :errors errors})))

;; ---------------------------------------------------------------- logs

(defn run-projects
  "[{:project :dir :files}] of the compose projects run `run` holds in the
   stack state: its consumers and its private instances (shared ones run
   under their provider's own project and are included too, as their logs
   hold the run's requests)."
  [ctx run]
  (let [e (get-in @(:state ctx) [:stacks run])
        projects (get-in ctx [:settings :compose/projects])
        of (fn [pid] (let [p (get projects pid)]
                       (cond-> {:dir (:project/dir p)} (seq (:project/files p)) (assoc :files (:project/files p)))))]
    (vec (distinct
          (concat
           (for [[c {:keys [compose-project]}] (sort-by key (:consumers e))]
             (assoc (of (first (str/split c #"/" 2))) :project compose-project))
           (for [[_ ip] (sort-by key (get-in e [:plan :stack/instances]))]
             (assoc (of (:instance/provider ip)) :project (:instance/compose-project ip))))))))

(defn- logs-profile [{:keys [project dir files]}]
  (cond-> {:profile/id project :profile/project project :profile/dir dir :profile/timestamps? true}
    (seq files) (assoc :profile/files files)))

(defn fetch-logs
  "{:logs {service text} :errors [..]}: the last `tail` log lines of every
   project of `run`, split by service; a service name is prefixed with its
   compose project when two projects of the run hold it."
  [ctx run tail]
  (let [rows (for [p (run-projects ctx run)]
               [p (port/-logs (:engine ctx) (logs-profile p) [] tail)])
        split (for [[p res] rows :when (r/ok? res)
                    [svc text] (why/split-compose-logs (get-in res [:ok :out]))]
                [(:project p) svc text])
        clash (set (for [[svc n] (frequencies (map second split)) :when (> n 1)] svc))]
    {:logs (into (sorted-map) (for [[cp svc text] split]
                                [(if (clash svc) (str cp "/" svc) svc) text]))
     :errors (vec (for [[p res] rows :when (r/err? res)] (assoc res :project (:project p))))}))

;; ---------------------------------------------------------------- why!

(defn- plan-of [ctx run]
  (when (and run (contains? (:stacks @(:state ctx)) run)
             (satisfies? port/IStackEngine (:engine ctx)))
    (let [st (stacks/stack-status ctx {:run run})]
      (if (r/ok? st)
        (first (get-in st [:ok :stacks]))
        (get-in @(:state ctx) [:stacks run :plan])))))

(defn- note! [ctx bundle files]
  (when-let [f (:note! ctx)]
    (try (f {:test-id (:why/test-id bundle) :run (:why/run bundle)
             :summary (:why/summary bundle) :files files})
         nil
         (catch Exception e (r/err :compose/note-failed {:message (ex-message e)})))))

(defn why!
  "Result<FailureBundle> of test `test` (a case id, `<suite>/<project>/<case>`,
   or a name the case goes by) in run `run` (default: the newest run holding
   it, a failed case first). Writes <state-dir>/runs/<run>/why/<case>.md and
   .edn and answers the bundle with :why/files {:md :edn} and, when a source
   failed, :why/gaps (what could not be read: traces, logs, note)."
  [ctx {:keys [test run] :as opts}]
  (r/let-ok [_ (if (and run (not (re-matches #"^[a-z0-9]{3,40}\z" (str run))))
                 (r/err :compose/invalid-run {:run run :problems (schema/explain schema/RunId run)})
                 (r/ok nil))
             {:keys [result case skipped]} (locate ctx opts)]
    (let [rid (:run/id result)
          window (why/case-window nil result)
          tq (:trace-query ctx)
          {:keys [traces query] terr :errors} (if tq
                                                (fetch-traces tq (trace-queries result case window))
                                                {:traces [] :errors [(r/err :compose/no-trace-query {})]})
          {:keys [logs] lerr :errors} (fetch-logs ctx rid (or (:why/tail ctx) default-tail))
          b (why/bundle {:case case
                         :test-id (leaf (:case/id case))
                         :cell-result result
                         :traces traces
                         :logs logs
                         :plan (plan-of ctx rid)
                         :window window
                         :slack-ms (:why/slack-ms ctx)})
          b (cond-> b query (assoc :why/trace-query query))
          md (why/render-markdown b)]
      (r/let-ok [files (runs/write-bundle! (state-dir ctx) rid (:case/id case) b md)]
        (let [nerr (note! ctx b files)
              gaps (cond-> {}
                     (seq terr) (assoc :traces (mapv #(select-keys % [:error :url :message :status]) terr))
                     (seq lerr) (assoc :logs (mapv #(select-keys % [:error :project :stderr]) lerr))
                     (seq skipped) (assoc :results skipped)
                     nerr (assoc :note (select-keys nerr [:error :message])))]
          (r/ok (cond-> (assoc b :why/files files :why/markdown md)
                  (seq gaps) (assoc :why/gaps gaps))))))))
