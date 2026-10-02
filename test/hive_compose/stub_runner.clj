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
        (let [pid (:next-pid (swap! world update :next-pid inc))]
          (swap! world update :alive conj pid)
          (r/ok {:pid pid}))))
  (-alive? [_ pid] (contains? (:alive @world) pid))
  (-listening? [_ port] (contains? (:listening @world) port))
  (-nrepl-port [_ dir] (get-in @world [:nrepl-files dir]))
  (-halt! [_ pid]
    (record! world [:stop pid])
    (or (fault world :stop)
        (do (swap! world update :alive disj pid)
            (r/ok {:pid pid :stopped? true})))))

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
