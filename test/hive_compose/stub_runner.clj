(ns hive-compose.stub-runner
  "An in-memory IProgramRunner: directories with build-file facts, a set of live
   pids, a recording of every start and stop, and faults injectable per method."
  (:require [clojure.string]
            [hive-compose.port :as port]
            [hive-compose.promote.oom :as oom]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- record! [world call]
  (swap! world update :calls conj call))

(defn- fault [world method]
  (when (contains? (:fail @world) method)
    (r/err :stub/fault {:method method})))

(defrecord StubRunner [world]
  port/IProgramRunner
  (-facts [_ dir]
    (or (fault world :facts) (r/ok (get-in @world [:facts dir] {}))))
  (-start! [_ p log]
    (record! world [:start (:program/id p) (:program/command p) log])
    (or (fault world :start)
        (let [pid (:next-pid (swap! world update :next-pid inc))
              death (get-in @world [:dies (:program/id p)])]
          (if death
            (swap! world update :exits assoc pid death)
            (swap! world update :alive conj pid))
          (r/ok {:pid pid}))))
  (-alive? [_ pid] (contains? (:alive @world) pid))
  (-listening? [_ port] (contains? (:listening @world) port))
  (-nrepl-port [_ dir] (get-in @world [:nrepl-files dir]))
  (-halt! [_ pid]
    (record! world [:stop pid])
    (or (fault world :stop)
        (do (swap! world update :alive disj pid)
            (r/ok {:pid pid :stopped? true}))))
  port/IProgramOutcome
  (-exit-code [_ pid] (get-in @world [:exits pid :exit-code]))
  (-log-tail [_ log n]
    (vec (take-last n (some (fn [[_ d]] (when (= log (:log d)) (:tail d)))
                            (:exits @world))))))

(defn runner
  "StubRunner over `facts` ({dir {file-name parsed}})."
  ([] (runner {}))
  ([facts]
   (->StubRunner (atom {:facts facts :alive #{} :listening #{} :nrepl-files {}
                        :next-pid 100 :calls [] :fail #{}}))))

(defn calls [rn] (:calls @(:world rn)))
(defn starts [rn] (filterv #(= :start (first %)) (calls rn)))
(defn stops [rn] (filterv #(= :stop (first %)) (calls rn)))
(defn alive [rn] (:alive @(:world rn)))
(defn kill! [rn pid] (swap! (:world rn) update :alive disj pid))
(defn listen! [rn port] (swap! (:world rn) update :listening conj port))
(defn fail! [rn & methods] (swap! (:world rn) update :fail into methods))
(defn die-on-start!
  "Program `program-id` exits at once when started, with `exit-code` and
   `tail` as the last lines of its log `log`."
  [rn program-id exit-code log tail]
  (swap! (:world rn) assoc-in [:dies program-id] {:exit-code exit-code :log log :tail tail}))

;; ICommandRunner: host commands run to completion, recorded as
;; [:run argv opts]; `fail!` :run makes every one fail.
(extend-type StubRunner
  port/ICommandRunner
  (-run-command! [{:keys [world]} argv opts]
    (record! world [:run (vec argv) opts])
    (or (fault world :run)
        (r/ok {:exit 0 :out ""}))))

(defn runs [rn] (filterv #(= :run (first %)) (calls rn)))

;; ---------------------------------------------------------------------------
;; ISuiteRunner (hc-8, hc-16b): scripted per test id `<suite>/<project>` (or
;; per suite id), writing JUnit fixtures into the spec's reports dir. A
;; script's `:peak-mem` (bytes) over its spec's `:resources :mem` cap is
;; OOM-killed as the real container runner reports it: exit 137 and the
;; finding `hive-compose.promote.oom/finding` builds.

(defn junit
  "JUnit XML with `passed` passing and `failed` failing cases of `suite`."
  [suite passed failed]
  (str "<testsuite name=\"" suite "\" tests=\"" (+ passed failed) "\" failures=\"" failed "\">"
       (apply str (for [i (range passed)] (str "<testcase classname=\"c\" name=\"ok" i "\"/>")))
       (apply str (for [i (range failed)] (str "<testcase classname=\"c\" name=\"bad" i "\"><failure message=\"x\"/></testcase>")))
       "</testsuite>"))

(defn- script-of [world spec]
  (or (get-in @world [:scripts (:test-id spec)])
      (get-in @world [:scripts (get-in spec [:cell :suite/id])])
      {:exit 0 :passed 1 :failed 0}))

(defn- run-suite! [world spec]
  (swap! world update :runs conj spec)
  (let [{:keys [exit passed failed error raw scrub throws collect peak-mem output] :or {exit 0}} (script-of world spec)
        _ (when (and throws (not error)) (throw (if (instance? Throwable throws) throws (ex-info "stub runner threw" {}))))
        scrub-fn (fn [s] (reduce (fn [s [v r]] (clojure.string/replace s v r)) s scrub))
        t (:clock (swap! world update :clock + 10))
        cap (oom/mem-bytes (get-in spec [:resources :mem]))
        killed? (and cap peak-mem (> peak-mem cap))]
    (if error
      (r/err error (cond-> {:exit nil :message (str "runner died" (when (string? throws) throws))}
                     scrub (assoc :scrub scrub-fn)))
      (do
        (when (or raw passed failed)
          (let [dir (java.io.File. (str (if collect (:collect-from spec) (:reports-dir spec))))]
            (.mkdirs dir)
            (spit (java.io.File. dir "junit.xml")
                  (or raw (junit (get-in spec [:cell :suite/id]) (or passed 0) (or failed 0))))))
        (r/ok (cond-> {:exit (if killed? 137 exit)
                       :started-at (- t 10) :finished-at t
                       :output (or output (if killed? ["Killed"] [(str "exit " exit)]))
                       :timed-out? false
                       :cap (cond-> {:mode (if cap :docker :none)} cap (assoc :mem cap))}
                killed? (assoc :oom (oom/finding (:test-id spec) cap :container 137))
                scrub (assoc :scrub scrub-fn)))))))

(defrecord StubSuiteRunner [world]
  port/ISuiteRunner
  (-run! [_ spec] (run-suite! world spec)))

(defn script!
  "Replace the scripts of stub suite runner `sr`."
  [sr scripts]
  (swap! (:world sr) assoc :scripts scripts))

(defn suite-runner
  "StubSuiteRunner over `scripts` ({test-id-or-suite-id {:exit n :passed n
   :failed n :peak-mem bytes :output [line]} | {:error kw} | {:exit n :raw
   text}}, any with :scrub {value rendering}, answered as the runner's :scrub
   fn, or :throws true); a script with neither :passed, :failed nor :raw
   writes no report."
  ([] (suite-runner {}))
  ([scripts] (->StubSuiteRunner (atom {:scripts scripts :runs [] :clock 1795367920000}))))

(defn capped-suite-runner
  "The stub suite runner over `scripts` ({test-id {:peak-mem bytes :exit n}})."
  ([] (suite-runner {}))
  ([scripts] (suite-runner scripts)))

(defn suite-runs
  "Every runner spec the stub suite runner ran, in order."
  [sr]
  (:runs @(:world sr)))
