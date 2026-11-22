(ns hive-compose.stub-runner
  "An in-memory IProgramRunner: directories with build-file facts, a set of live
   pids, a recording of every start and stop, and faults injectable per method."
  (:require [hive-compose.port :as port]
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

;; ---------------------------------------------------------------------------
;; ISuiteRunner (hc-8): scripted per test id `<suite>/<project>` (or per
;; suite id), writing JUnit fixtures into the spec's reports dir.

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

(defn- run-suite! [world kind spec]
  (swap! world update :runs conj (assoc spec ::kind kind))
  (let [{:keys [exit passed failed error raw scrub]} (script-of world spec)
        scrub-fn (fn [s] (reduce (fn [s [v r]] (clojure.string/replace s v r)) s scrub))
        t (:clock (swap! world update :clock + 10))]
    (if error
      (r/err error {:exit nil :log-tail ["runner died"] :started-ms (- t 10) :ended-ms t})
      (do
        (when (or raw passed failed)
          (let [dir (java.io.File. (str (:reports-dir spec)))]
            (.mkdirs dir)
            (spit (java.io.File. dir "junit.xml")
                  (or raw (junit (get-in spec [:cell :suite/id]) (or passed 0) (or failed 0))))))
        (r/ok (cond-> {:exit exit :started-ms (- t 10) :ended-ms t :log-tail [(str "exit " exit)]}
                scrub (assoc :scrub scrub-fn)))))))

(defn script!
  "Replace the scripts of stub suite runner `sr`."
  [sr scripts]
  (swap! (:world sr) assoc :scripts scripts))

(defrecord StubSuiteRunner [world]
  port/ISuiteRunner
  (-run-container! [_ spec] (run-suite! world :container spec))
  (-run-command! [_ spec] (run-suite! world :command spec)))

(defn suite-runner
  "StubSuiteRunner over `scripts` ({test-id-or-suite-id {:exit n :passed n
   :failed n} | {:error kw} | {:exit n :raw text}}, any with :scrub {value
   rendering}, answered as the runner's :scrub fn); a script with neither
   :passed, :failed nor :raw writes no report."
  ([] (suite-runner {}))
  ([scripts] (->StubSuiteRunner (atom {:scripts scripts :runs [] :clock 1000}))))

(defn suite-runs
  "Every RunnerSpec the stub ran, in order, with ::kind :container|:command."
  [sr]
  (:runs @(:world sr)))
