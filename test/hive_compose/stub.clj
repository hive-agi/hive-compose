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

(defn- enabled
  "Services of `model` enabled under native `profiles`: always-on plus members."
  [model profiles]
  (sort (for [[svc {ps :profiles}] model
              :when (or (empty? ps) (some (set profiles) ps))]
          svc)))

(defn- closure
  "`targets` plus everything they transitively depend on in `model`."
  [model targets]
  (loop [seen (sorted-set) todo (vec targets)]
    (if-let [[s & more] (seq todo)]
      (if (seen s)
        (recur seen (vec more))
        (recur (conj seen s) (into (vec more) (get-in model [s :deps]))))
      (vec seen))))

(defrecord StubEngine [world]
  port/IComposeEngine
  (-version [_] (or (fault world :version) (r/ok "stub")))
  (-closure [_ p]
    (record! world [:closure (:profile/id p)])
    (or (fault world :closure)
        (if-let [c (get-in @world [:closures (:profile/id p)])]
          (r/ok (vec (sort c)))
          (let [model (get-in @world [:projects (profile/project p)])
                targets (or (seq (:profile/services p)) (enabled model (:profile/compose-profiles p)))]
            (if-let [missing (first (remove model targets))]
              (r/err :compose/command-failed {:exit 1 :stderr (str "no such service: " missing)})
              (r/ok (closure model targets)))))))
  (-listed [_ p]
    (r/ok (vec (enabled (get-in @world [:projects (profile/project p)]) (:profile/compose-profiles p)))))
  (-profiles [_ p]
    (r/ok (into #{} (mapcat :profiles) (vals (get-in @world [:projects (profile/project p)])))))
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
  "StubEngine over `closures` ({preset-id [service]}), `running`
   ({project #{service}}) and compose `projects`
   ({project {service {:deps [service] :profiles #{name}}}})."
  ([closures] (engine closures {}))
  ([closures running] (engine closures running {}))
  ([closures running projects]
   (->StubEngine (atom {:closures closures :running running :projects projects :calls [] :fail #{}}))))

(defn calls [e] (:calls @(:world e)))
(defn running [e project] (get-in @(:world e) [:running project] #{}))
(defn fail! [e & methods] (swap! (:world e) update :fail into methods))
