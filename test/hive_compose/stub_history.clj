(ns hive-compose.stub-history
  "An in-memory IFlakeHistory: the rows appended, in order, and faults
   injectable per method."
  (:require [hive-compose.port :as port]
            [hive-compose.promote.flaky :as flaky]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defrecord MemoryHistory [world]
  port/IFlakeHistory
  (-append! [_ rows]
    (if (contains? (:fail @world) :append!)
      (r/err :stub/fault {:method :append!})
      (do (swap! world update :rows into rows)
          (r/ok {:appended (count rows)}))))
  (-history [_ query]
    (if (contains? (:fail @world) :history)
      (r/err :stub/fault {:method :history})
      (r/ok (vec (flaky/matching query (:rows @world)))))))

(defn history
  "An in-memory history holding `rows` (default none)."
  ([] (history []))
  ([rows] (->MemoryHistory (atom {:rows (vec rows) :fail #{}}))))

(defn rows
  "Every row `h` holds, in append order."
  [h]
  (:rows @(:world h)))

(defn fail!
  "Make `method` (:append! or :history) of `h` answer an error."
  [h method]
  (swap! (:world h) update :fail conj method))
