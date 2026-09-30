(ns hive-compose.stub
  "An in-memory IComposeEngine: a world of projects with running services, a
   recording of every call, and faults injectable per method."
  (:require [hive-compose.port :as port]
            [hive-compose.promote.profile :as profile]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- record! [world call]
  (swap! world update :calls conj call))

(defn- fault [world method]
  (when (contains? (:fail @world) method)
    (r/err :stub/fault {:method method})))

(defrecord StubEngine [world]
  port/IComposeEngine
  (-version [_] (or (fault world :version) (r/ok "stub")))
  (-closure [_ p]
    (record! world [:closure (:profile/id p)])
    (or (fault world :closure)
        (r/ok (vec (sort (get-in @world [:closures (:profile/id p)]))))))
  (-running [_ p]
    (or (fault world :running)
        (r/ok (into (sorted-set) (get-in @world [:running (profile/project p)])))))
  (-projects [_]
    (r/ok (mapv (fn [[k v]] {:name k :status (str "running(" (count v) ")") :config-files ""})
                (:running @world))))
  (-up! [_ p services]
    (record! world [:up (profile/project p) (vec services)])
    (or (fault world :up)
        (do (swap! world update-in [:running (profile/project p)] (fnil into #{}) services)
            (r/ok {:out ""}))))
  (-stop! [_ p services]
    (record! world [:stop (profile/project p) (vec services)])
    (or (fault world :stop)
        (do (swap! world update-in [:running (profile/project p)] #(reduce disj (or % #{}) services))
            (r/ok {:out ""}))))
  (-down! [_ p services]
    (record! world [:down (profile/project p) (vec services)])
    (or (fault world :down)
        (do (swap! world update-in [:running (profile/project p)] #(reduce disj (or % #{}) services))
            (r/ok {:out ""}))))
  (-logs [_ p services tail]
    (record! world [:logs (profile/project p) (vec services) tail])
    (r/ok {:out "log"})))

(defn engine
  "StubEngine over `closures` ({profile-id [service]}) and `running`
   ({project #{service}})."
  ([closures] (engine closures {}))
  ([closures running]
   (->StubEngine (atom {:closures closures :running running :calls [] :fail #{}}))))

(defn calls [e] (:calls @(:world e)))
(defn running [e project] (get-in @(:world e) [:running project] #{}))
(defn fail! [e & methods] (swap! (:world e) update :fail into methods))
