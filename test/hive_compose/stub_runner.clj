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

;; ICommandRunner: host commands run to completion, recorded as
;; [:run argv opts]; `fail!` :run makes every one fail.
(extend-type StubRunner
  port/ICommandRunner
  (-run! [{:keys [world]} argv opts]
    (record! world [:run (vec argv) opts])
    (or (fault world :run)
        (r/ok {:exit 0 :out ""}))))

(defn runs [rn] (filterv #(= :run (first %)) (calls rn)))
