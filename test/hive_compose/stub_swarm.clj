(ns hive-compose.stub-swarm
  "An in-memory ISwarm (agents set by the test, notices recorded) and IActivity
   (peers per port set by the test)."
  (:require [hive-compose.port :as port]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defrecord StubSwarm [world]
  port/ISwarm
  (-agents [_]
    (if (:absent? @world)
      (r/err :host/absent {})
      (r/ok (vec (:agents @world)))))
  (-notify! [_ event data]
    (swap! world update :notices conj [event data])
    (r/ok true)))

(defn swarm
  "StubSwarm with `agents` ([{:agent/id :agent/cwd}])."
  ([] (swarm []))
  ([agents] (->StubSwarm (atom {:agents agents :notices [] :absent? false}))))

(defn agents! [s agents] (swap! (:world s) assoc :agents agents))
(defn absent! [s] (swap! (:world s) assoc :absent? true))
(defn notices [s] (:notices @(:world s)))

(defrecord StubActivity [world]
  port/IActivity
  (-peers [_ ports]
    (select-keys (:peers @world) ports)))

(defn activity [] (->StubActivity (atom {:peers {}})))
(defn peers! [a m] (swap! (:world a) assoc :peers m))
