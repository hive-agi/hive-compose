(ns hive-compose.pipeline.ops
  "Profile operations over a context:

     {:engine   IComposeEngine
      :settings Settings
      :state    (atom {:active {id Active} :current id-or-nil})
      :now      (fn [] epoch-ms)
      :save!    (fn [persisted-state] Result)}   ; optional

   Every operation answers a Result (reap! and adopt! answer reports). The
   active map changes only after the engine confirmed the action."
  (:require [hive-compose.port :as port]
            [hive-compose.promote.plan :as plan]
            [hive-compose.promote.profile :as profile]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- persist! [{:keys [state save!]}]
  (when save! (save! (select-keys @state [:active :current]))))

(defn- configured [ctx id]
  (get-in ctx [:settings :compose/profiles id]))

(defn lookup
  "Result<Profile> for `id`: the configured one, else the snapshot an active
   entry ran with."
  [ctx id]
  (if-let [p (or (configured ctx id) (get-in @(:state ctx) [:active id :profile]))]
    (r/ok p)
    (r/err :compose/unknown-profile
           {:profile id :known (vec (sort (keys (get-in ctx [:settings :compose/profiles]))))})))

(defn- execute! [{:keys [engine]} p {:keys [action services]}]
  (case action
    :stop (port/-stop! engine p services)
    :down (port/-down! engine p services)))

(defn- forget [st id]
  (-> st
      (update :active dissoc id)
      (update :current #(when-not (= % id) %))))

(defn- release!
  "Run Release `step`; on success the profile leaves the active map. Answers the
   step with its :result."
  [ctx {id :profile/id :as step}]
  (let [res (r/bind (lookup ctx id) #(execute! ctx % step))]
    (when (r/ok? res) (swap! (:state ctx) forget id))
    (assoc step :result res)))

(defn- step-report [{:keys [result] :as step}]
  (cond-> (-> step (dissoc :result) (assoc :ok? (r/ok? result)))
    (r/err? result) (assoc :error result)))

(defn- bring-up! [ctx id p services]
  (r/let-ok [out (port/-up! (:engine ctx) p services)]
    (let [now ((:now ctx))]
      (swap! (:state ctx) update-in [:active id]
             (fn [a] {:profile/id id
                      :project (profile/project p)
                      :services services
                      :profile p
                      :started-at (or (:started-at a) now)
                      :last-touch now}))
      (r/ok {:profile id :project (profile/project p) :services services :out (:out out)}))))

(defn up!
  "Bring profile `id` up alongside whatever is active. Becomes current when
   nothing is."
  [ctx id]
  (r/let-ok [p (lookup ctx id)
             services (port/-closure (:engine ctx) p)
             up (bring-up! ctx id p services)]
    (swap! (:state ctx) update :current #(or % id))
    (persist! ctx)
    (r/ok up)))

(defn switch!
  "Make `id` the current profile: release what only the previous current profile
   needs, then bring `id` up. Shared services stay up throughout."
  [ctx id]
  (r/let-ok [p (lookup ctx id)
             services (port/-closure (:engine ctx) p)]
    (let [{:keys [active current]} @(:state ctx)
          released (mapv #(step-report (release! ctx %))
                         (plan/switch-releases active current id (profile/project p) services))
          up (bring-up! ctx id p services)]
      (when (r/ok? up) (swap! (:state ctx) assoc :current id))
      (persist! ctx)
      (r/map-ok up #(assoc % :released released)))))

(defn down!
  "Take profile `id` down with `action` (:down removes containers, :stop keeps
   them; volumes are never touched). Services another active profile needs stay
   up."
  [ctx id action]
  (r/let-ok [p (lookup ctx id)
             :let [active (:active @(:state ctx))]
             services (if-let [a (get active id)]
                        (r/ok (:services a))
                        (port/-closure (:engine ctx) p))]
    (let [step (release! ctx (plan/down-release active id (profile/project p) services action))]
      (persist! ctx)
      (r/map-ok (:result step) (fn [o] (assoc (step-report step) :out (:out o)))))))

(defn touch!
  "Reset the idle clock of active profile `id`."
  [ctx id]
  (if (get-in @(:state ctx) [:active id])
    (let [now ((:now ctx))]
      (swap! (:state ctx) assoc-in [:active id :last-touch] now)
      (persist! ctx)
      (r/ok {:profile id :last-touch now}))
    (r/err :compose/not-active {:profile id})))

(defn ps
  "Result<{:profile :needs :running}> for profile `id`; touches it when active."
  [ctx id]
  (r/let-ok [p (lookup ctx id)
             needs (port/-closure (:engine ctx) p)
             running (port/-running (:engine ctx) p)]
    (when (get-in @(:state ctx) [:active id]) (touch! ctx id))
    (r/ok {:profile id :project (profile/project p) :needs needs
           :running (vec (filter (set running) needs))})))

(defn logs
  "Result<{:out}>: the last `tail` log lines of profile `id`; touches it when active."
  [ctx id tail]
  (r/let-ok [p (lookup ctx id)
             out (port/-logs (:engine ctx) p (or (:profile/services p) []) tail)]
    (when (get-in @(:state ctx) [:active id]) (touch! ctx id))
    (r/ok out)))

(defn reconcile!
  "Forget active profiles none of whose services still run (taken down outside
   the addon). Answers the forgotten ids."
  [ctx]
  (let [dropped (vec (for [[id a] (:active @(:state ctx))
                           :let [running (r/bind (lookup ctx id) #(port/-running (:engine ctx) %))]
                           :when (and (r/ok? running) (not-any? (:ok running) (:services a)))]
                       id))]
    (when (seq dropped)
      (swap! (:state ctx) #(reduce forget % dropped))
      (persist! ctx))
    dropped))

(defn reap!
  "One reaper pass: reconcile, then release every idle profile."
  [ctx]
  (let [reconciled (reconcile! ctx)
        steps (plan/reap-releases (:settings ctx) ((:now ctx)) (:active @(:state ctx)))
        reaped (mapv #(step-report (release! ctx %)) steps)]
    (when (seq reaped) (persist! ctx))
    {:reconciled reconciled :reaped reaped}))

(defn adopt!
  "Take charge of configured profiles that are running but not active (started
   by hand, or before a restart lost the state), with a fresh idle clock.
   Answers the adopted ids."
  [ctx]
  (let [{:keys [engine settings]} ctx
        active (:active @(:state ctx))
        now ((:now ctx))
        adopted (vec (for [[id p] (sort-by key (:compose/profiles settings))
                           :when (not (contains? active id))
                           :let [closure (port/-closure engine p)]
                           :when (r/ok? closure)
                           :let [running (port/-running engine p)]
                           :when (and (r/ok? running) (some (:ok running) (:ok closure)))]
                       {:profile/id id :project (profile/project p) :services (:ok closure)
                        :profile p :started-at now :last-touch now}))]
    (when (seq adopted)
      (swap! (:state ctx) update :active into (map (juxt :profile/id identity)) adopted)
      (persist! ctx))
    (mapv :profile/id adopted)))

(defn status
  "Every configured profile with its activity and reaper countdown."
  [ctx]
  (let [{:keys [settings]} ctx
        {:keys [active current]} @(:state ctx)
        now ((:now ctx))
        row (fn [id p]
              (let [a (get active id)
                    ttl (profile/ttl-ms settings p)]
                (cond-> {:id id
                         :project (profile/project p)
                         :active? (some? a)
                         :ttl-minutes (quot ttl 60000)
                         :idle-action (profile/idle-action settings p)}
                  (:profile/description p) (assoc :description (:profile/description p))
                  a (assoc :services (:services a)
                           :idle-seconds (quot (- now (:last-touch a)) 1000)
                           :reap-in-seconds (max 0 (quot (- (+ (:last-touch a) ttl) now) 1000))))))]
    {:current current
     :profiles (into (mapv (fn [[id p]] (row id p)) (sort-by key (:compose/profiles settings)))
                     (for [[id a] (sort-by key active)
                           :when (not (configured ctx id))]
                       (assoc (row id (:profile a)) :unconfigured? true)))}))

(defn projects
  "Result<[project]>: every compose project on the host, managed ones marked."
  [ctx]
  (let [managed (into #{} (map :project) (vals (:active @(:state ctx))))]
    (r/map-ok (port/-projects (:engine ctx))
              (fn [ps] (mapv #(assoc % :managed? (contains? managed (:name %))) ps)))))
