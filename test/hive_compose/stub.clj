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

(defn- undefined
  "compose's refusal when `services` names one the world marks :undefined."
  [world services]
  (when-let [s (first (filter (or (:undefined @world) #{}) services))]
    (r/err :compose/command-failed {:exit 1 :stderr (str "no such service: " s "\n")})))

(defn- unset
  "compose's refusal to load a project whose files need a variable the
   world marks unset (`unset!`)."
  [world p]
  (when-let [v (get-in @world [:unset (profile/project p)])]
    (r/err :compose/command-failed
           {:exit 1 :stderr (str "error while interpolating services: required variable "
                                 v " is missing a value: set it\n")})))

(defn unset!
  "Make every compose call that loads `project`'s files refuse, naming `var`
   as a required variable with no value. Container calls by name still work."
  [e project var]
  (swap! (:world e) assoc-in [:unset project] var))

(defn undefine!
  "Make compose refuse `services` as no longer defined."
  [e & services]
  (swap! (:world e) update :undefined (fnil into #{}) services))

(defn- enabled
  "Services of `model` enabled under native `profiles`: always-on plus members."
  [model profiles]
  (sort (for [[svc {ps :profiles}] model
              :when (or (empty? ps) (some (set profiles) ps))]
          svc)))

(defn- closure
  "`targets` plus everything they transitively depend on in `model`, the
   `required: false` (:optional) entries included, as `compose config` does."
  [model targets]
  (loop [seen (sorted-set) todo (vec targets)]
    (if-let [[s & more] (seq todo)]
      (if (seen s)
        (recur seen (vec more))
        (recur (conj seen s) (-> (vec more)
                                 (into (get-in model [s :deps]))
                                 (into (get-in model [s :optional])))))
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
    (or (fault world :listed)
        (r/ok (vec (enabled (get-in @world [:projects (profile/project p)]) (:profile/compose-profiles p))))))
  (-profiles [_ p]
    (r/ok (into #{} (mapcat :profiles) (vals (get-in @world [:projects (profile/project p)])))))
  (-running [_ p]
    (or (fault world :running)
        (r/ok (into (sorted-set) (get-in @world [:running (profile/project p)])))))
  (-projects [_]
    (r/ok (or (:ls @world)
              (mapv (fn [[k v]] {:name k :status (str "running(" (count v) ")") :config-files ""})
                    (:running @world)))))
  (-up! [_ p services]
    (record! world [:up (profile/project p) (vec services)])
    (swap! world assoc :last-up-no-deps? (boolean (:profile/no-deps? p)))
    (or (fault world :up)
        (do (swap! world update-in [:running (profile/project p)] (fnil into #{}) services)
            (r/ok {:out ""}))))
  (-stop! [_ p services]
    (record! world [:stop (profile/project p) (vec services)])
    (or (fault world :stop)
        (unset world p)
        (undefined world services)
        (do (swap! world update-in [:running (profile/project p)] #(reduce disj (or % #{}) services))
            (r/ok {:out ""}))))
  (-down! [_ p services]
    (record! world [:down (profile/project p) (vec services)])
    (or (fault world :down)
        (unset world p)
        (undefined world services)
        (do (swap! world update-in [:running (profile/project p)] #(reduce disj (or % #{}) services))
            (r/ok {:out ""}))))
  (-restart! [_ p services recreate?]
    (record! world [(if recreate? :recreate :restart) (profile/project p) (vec services)])
    (or (fault world :restart)
        (do (swap! world update-in [:running (profile/project p)] (fnil into #{}) services)
            (r/ok {:out ""}))))
  (-logs [_ p services tail]
    (record! world [:logs (profile/project p) (vec services) tail])
    (r/ok {:out "log"}))

  port/IDependencyGraph
  ;; As `compose config --format json [targets]`: the targets' closure when
  ;; the profile names services, else what its native profiles enable. A
  ;; graph injected under :graphs answers as is (an engine that disagrees).
  (-dependencies [_ p]
    (or (fault world :dependencies)
        (if-let [g (get-in @world [:graphs (profile/project p)])]
          (r/ok g)
          (let [model (get-in @world [:projects (profile/project p)])
                shown (set (closure model (or (seq (:profile/services p))
                                              (enabled model (:profile/compose-profiles p)))))]
            (r/ok (into {}
                        (for [[svc {:keys [deps optional]}] model
                              :when (shown svc)]
                          [svc (merge (zipmap deps (repeat {:required? true}))
                                      (zipmap optional (repeat {:required? false})))])))))))

  port/IContainerControl
  ;; By name: neither an unset variable nor an undefined service refuses it;
  ;; only a service with a running container is acted on.
  (-stop-containers! [_ p services action]
    (record! world [(if (= :down action) :rm-by-name :stop-by-name) (profile/project p) (vec services)])
    (or (fault world :stop-containers)
        (let [project (profile/project p)
              hit (vec (sort (filter (get-in @world [:running project] #{}) services)))]
          (swap! world update-in [:running project] #(reduce disj (or % #{}) hit))
          (r/ok {:out "" :containers (mapv #(str project "-" % "-1") hit)}))))

  port/IPublishedPorts
  ;; {service #{port}} set with `published!`, of the services running now.
  (-published [_ p]
    (or (fault world :published)
        (let [project (profile/project p)
              running (get-in @world [:running project] #{})]
          (r/ok (select-keys (get-in @world [:published project] {}) running)))))

  port/IContainerOwnership
  ;; #{id} set with `live!`; {service {:volumes :ports}} set with `holds!`.
  (-live-containers [_]
    (or (fault world :live-containers) (r/ok (get @world :live #{}))))
  (-resources [_ p services]
    (or (fault world :resources)
        (let [m (get-in @world [:holds (profile/project p)] {})]
          (r/ok {:volumes (into #{} (mapcat #(get-in m [% :volumes])) services)
                 :ports (into #{} (mapcat #(get-in m [% :ports])) services)}))))

  port/IContainerAddresses
  ;; #{ip} set with `addresses!`.
  (-addresses [_ p]
    (or (fault world :addresses)
        (r/ok (get-in @world [:addresses (profile/project p)] #{}))))

  port/IContainerLoad
  ;; {service percent} set with `load!`, of the services running now.
  (-cpu [_ p]
    (or (fault world :cpu)
        (let [project (profile/project p)
              running (get-in @world [:running project] #{})]
          (r/ok (select-keys (get-in @world [:load project] {}) running))))))

;; ---------------------------------------------------------------------------
;; IStackEngine: networks and labelled containers.
;;
;; World keys: :networks {name {:labels {k v} :members {container [alias]}}},
;; :containers {name {:project :service :labels :state :health :networks
;; #{name}}}, :health {[project service] :healthy|:unhealthy|:starting|:none}
;; (default :healthy) set with `health!`. A container is `<project>-<svc>-1`.

(defn- container-name [project svc] (str project "-" svc "-1"))

(defn- has-labels? [want labels]
  (every? (fn [[k v]] (= v (get labels k))) want))

(defn- missing-network [world spec]
  (when-let [n (first (remove (or (:networks @world) {}) (:networks spec)))]
    (r/err :compose/command-failed {:exit 1 :stderr (str "network " n " declared as external, but could not be found\n")})))

(defn- unhealthy [world spec]
  (when (:wait? spec)
    (when-let [s (first (filter #(#{:unhealthy :starting} (get-in @world [:health [(:project spec) %]]))
                                (:services spec)))]
      (r/err :compose/command-failed {:exit 1 :stderr (str "container " (container-name (:project spec) s)
                                                           " is unhealthy\n")}))))

(extend-type StubEngine
  port/IStackEngine
  (-network-create! [{:keys [world]} network labels]
    (record! world [:network-create network labels])
    (or (fault world :network-create)
        (if (get-in @world [:networks network])
          (r/ok {:network network :created? false})
          (do (swap! world assoc-in [:networks network] {:labels (into {} labels) :members {}})
              (r/ok {:network network :created? true})))))
  (-network-rm! [{:keys [world]} network]
    (record! world [:network-rm network])
    (or (fault world :network-rm)
        (if-let [{:keys [members]} (get-in @world [:networks network])]
          (do (swap! world (fn [w]
                             (-> (reduce (fn [w c] (update-in w [:containers c :networks] disj network))
                                         w (keys members))
                                 (update :networks dissoc network))))
              (r/ok {:network network :removed? true :disconnected (vec (sort (keys members)))}))
          (r/ok {:network network :removed? false :disconnected []}))))
  (-network-connect! [{:keys [world]} network {:keys [project service]} aliases]
    (record! world [:network-connect network project service (vec aliases)])
    (or (fault world :network-connect)
        (let [c (container-name project service)]
          (cond
            (not (get-in @world [:networks network]))
            (r/err :compose/command-failed {:exit 1 :stderr (str "network " network " not found\n")})
            (not (get-in @world [:containers c]))
            (r/err :compose/no-container {:project project :service service :network network})
            :else
            (do (swap! world #(-> %
                                  (assoc-in [:networks network :members c] (vec aliases))
                                  (update-in [:containers c :networks] (fnil conj #{}) network)))
                (r/ok {:network network :containers [c]}))))))
  (-up-project! [{:keys [world]} spec]
    (record! world [:up-project (:project spec) (vec (:services spec))
                    (select-keys spec [:labels :networks :wait? :container-env :files])])
    (if (empty? (:services spec))
      (r/ok {:out "" :skipped :no-services})
      (or (fault world :up-project)
          (missing-network world spec)
          (let [{:keys [project services labels networks]} spec]
            (swap! world
                   (fn [w]
                     (-> (reduce (fn [w s]
                                   (let [c (container-name project s)
                                         nets (set networks)
                                         w (reduce #(assoc-in %1 [:networks %2 :members c] [s]) w nets)]
                                     (assoc-in w [:containers c]
                                               {:project project :service s
                                                :labels (merge {"com.docker.compose.project" project
                                                                "com.docker.compose.service" s}
                                                               labels)
                                                :state "running"
                                                :health (get-in w [:health [project s]] :healthy)
                                                :networks nets})))
                                 w services)
                         (update-in [:running project] (fnil into #{}) services))))
            (or (unhealthy world spec)
                (r/ok {:out ""}))))))
  (-down-project! [{:keys [world]} spec]
    (record! world [:down-project (:project spec) (if (:all? spec) :all (vec (:services spec)))])
    (or (fault world :down-project)
        (let [{:keys [project]} spec
              gone (if (:all? spec)
                     (set (or (get-in @world [:running project]) #{}))
                     (set (:services spec)))
              names (set (map #(container-name project %) gone))]
          (swap! world (fn [w]
                         (-> w
                             (update :containers #(apply dissoc % names))
                             (update :networks (fn [ns] (into {} (for [[n m] ns]
                                                                   [n (update m :members #(apply dissoc % names))]))))
                             (update-in [:running project] #(reduce disj (or % #{}) gone)))))
          (r/ok {:out ""}))))
  (-running-labelled [{:keys [world]} labels]
    (or (fault world :running-labelled)
        (r/ok (vec (sort-by :name
                            (for [[nm c] (:containers @world)
                                  :when (has-labels? labels (:labels c))]
                              (assoc (select-keys c [:project :service :labels :state :health]) :name nm)))))))
  (-networks-labelled [{:keys [world]} labels]
    (or (fault world :networks-labelled)
        (r/ok (vec (sort-by :name
                            (for [[nm n] (:networks @world)
                                  :when (has-labels? labels (:labels n))]
                              {:name nm :labels (:labels n)})))))))

(defn health!
  "Make the container of `service` in compose project `project` report `h`
   (:healthy :unhealthy :starting :none); `up --wait` fails on :unhealthy or
   :starting, as compose's does on a timeout."
  [e project service h]
  (swap! (:world e) (fn [w]
                      (cond-> (assoc-in w [:health [project service]] h)
                        (get-in w [:containers (container-name project service)])
                        (assoc-in [:containers (container-name project service) :health] h)))))

(defn networks
  "{name {:labels :members {container [alias]}}} of the stub world."
  [e]
  (:networks @(:world e) {}))

(defn containers
  "{name {:project :service :labels :state :health :networks}} of the stub world."
  [e]
  (:containers @(:world e) {}))

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

(defn published!
  "Make the running services of `project` publish host ports `m` ({service #{port}})."
  [e project m]
  (swap! (:world e) assoc-in [:published project] m))

(defn load!
  "Make the running services of `project` use CPU `m` ({service percent})."
  [e project m]
  (swap! (:world e) assoc-in [:load project] m))

(defn addresses!
  "Make the containers of `project` hold network addresses `ips`."
  [e project ips]
  (swap! (:world e) assoc-in [:addresses project] (set ips)))

(defn graph!
  "Make `-dependencies` answer `g` for `project`, whatever the model says."
  [e project g]
  (swap! (:world e) assoc-in [:graphs project] g))

(defn holds!
  "Make the services of `project` mount volumes and publish ports `m`
   ({service {:volumes #{name} :ports #{port}}})."
  [e project m]
  (swap! (:world e) assoc-in [:holds project] m))
