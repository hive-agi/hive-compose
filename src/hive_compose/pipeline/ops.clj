(ns hive-compose.pipeline.ops
  "Section and profile operations over a context:

     {:engine   IComposeEngine
      :settings Settings
      :state    (atom {:active {id Active} :current id-or-nil})
      :now      (fn [] epoch-ms)
      :save!    (fn [persisted-state] Result)}   ; optional

   A subject is a Profile: a configured preset (`lookup`) or a section resolved
   from a target against the configured compose projects (`resolve-target`).
   Every operation answers a Result (reap! and adopt! answer reports). The
   active map changes only after the engine confirmed the action."
  (:require [hive-compose.port :as port]
            [hive-compose.promote.plan :as plan]
            [hive-compose.promote.profile :as profile]
            [hive-compose.promote.target :as target]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- persist! [{:keys [state save!]}]
  (when save! (save! (select-keys @state [:active :current]))))

(defn- configured [ctx id]
  (get-in ctx [:settings :compose/profiles id]))

(defn- active-entry [ctx id]
  (get-in @(:state ctx) [:active id]))

;; ---------------------------------------------------------------------------
;; subjects

(defn lookup
  "Result<Profile> for `id`: the configured preset, else the snapshot an active
   entry (a preset or a section) ran with."
  [ctx id]
  (if-let [p (or (configured ctx id) (:profile (active-entry ctx id)))]
    (r/ok p)
    (r/err :compose/unknown-profile
           {:profile id :known (vec (sort (keys (get-in ctx [:settings :compose/profiles]))))})))

(defn- expand-name
  "Result<[service] | nil>: what `nm` stands for in project `proj`: itself when
   it is a service, the members of the native profile when it is one, nil when
   the project defines neither."
  [engine proj nm]
  (let [whole (target/project-profile proj)]
    (r/let-ok [profiles (port/-profiles engine whole)]
      (if (contains? profiles nm)
        (r/let-ok [with (port/-listed engine (assoc whole :profile/compose-profiles [nm]))
                   always (port/-listed engine whole)]
          (r/ok (target/profile-members with always)))
        (let [c (port/-closure engine (assoc whole :profile/services [nm]))]
          (cond
            (r/ok? c) (r/ok [nm])
            (target/unknown-service? c) (r/ok nil)
            :else c))))))

(defn- expand-names
  "Result<[service] | nil> for all `names` in `proj`; nil when one is unknown there."
  [engine proj names]
  (reduce (fn [acc nm]
            (let [x (expand-name engine proj nm)]
              (cond
                (r/err? x) (reduced x)
                (nil? (:ok x)) (reduced (r/ok nil))
                :else (r/ok (into (:ok acc) (:ok x))))))
          (r/ok [])
          names))

(defn resolve-target
  "Result<Profile>: the section running exactly what `target-str` names
   (services or native compose profiles, see `promote.target/parse`), in the
   first configured project that defines all of them. Its services close over
   the compose file's depends_on when brought up."
  [ctx target-str]
  (let [{:keys [project names]} (target/parse target-str)
        projects (get-in ctx [:settings :compose/projects])
        candidates (if project
                     (some-> (get projects project) vector)
                     (map val (sort-by key projects)))]
    (cond
      (empty? names) (r/err :compose/missing-param {:param "target"})
      (and project (empty? candidates)) (r/err :compose/unknown-project
                                                {:project project :known (vec (sort (keys projects)))})
      (empty? candidates) (r/err :compose/no-projects
                                 {:hint "declare compose projects under :compose/projects"})
      :else
      (loop [[proj & more] candidates]
        (if-not proj
          (r/err :compose/unknown-target {:target target-str :projects (vec (sort (keys projects)))})
          (let [x (expand-names (:engine ctx) proj names)]
            (cond
              (r/err? x) x
              (nil? (:ok x)) (recur more)
              :else (r/ok (target/section proj names (:ok x))))))))))

(defn subject
  "Result<Profile> for a tool call: `target` resolves a section, else `id`
   looks up a preset or an active section."
  [ctx {:keys [target id]}]
  (cond
    (seq target) (let [{:keys [project names]} (target/parse target)
                       sid (when project (target/section-id project names))]
                   (if (and sid (active-entry ctx sid))
                     (lookup ctx sid)
                     (resolve-target ctx target)))
    (seq id) (lookup ctx id)
    :else (r/err :compose/missing-param {:param "profile or target"})))

;; ---------------------------------------------------------------------------
;; release

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

;; ---------------------------------------------------------------------------
;; operations on a Profile

(defn- bring-up! [ctx p services]
  (let [id (:profile/id p)]
    (r/let-ok [out (port/-up! (:engine ctx) p services)]
      (let [now ((:now ctx))]
        (swap! (:state ctx) update-in [:active id]
               (fn [a] {:profile/id id
                        :project (profile/project p)
                        :services services
                        :profile p
                        :started-at (or (:started-at a) now)
                        :last-touch now}))
        (r/ok {:profile id :project (profile/project p) :services services :out (:out out)})))))

(defn up-profile!
  "Bring `p` up alongside whatever is active. Becomes current when nothing is."
  [ctx p]
  (r/let-ok [services (port/-closure (:engine ctx) p)
             up (bring-up! ctx p services)]
    (swap! (:state ctx) update :current #(or % (:profile/id p)))
    (persist! ctx)
    (r/ok up)))

(defn switch-profile!
  "Make `p` current: release what only the previous current profile needs, then
   bring `p` up. Shared services stay up throughout."
  [ctx p]
  (r/let-ok [services (port/-closure (:engine ctx) p)]
    (let [id (:profile/id p)
          {:keys [active current]} @(:state ctx)
          released (mapv #(step-report (release! ctx %))
                         (plan/switch-releases active current id (profile/project p) services))
          up (bring-up! ctx p services)]
      (when (r/ok? up) (swap! (:state ctx) assoc :current id))
      (persist! ctx)
      (r/map-ok up #(assoc % :released released)))))

(defn down-profile!
  "Take `p` down with `action` (:down removes containers, :stop keeps them;
   volumes are never touched). Services another active profile needs stay up."
  [ctx p action]
  (let [id (:profile/id p)
        active (:active @(:state ctx))]
    (r/let-ok [services (if-let [a (get active id)]
                          (r/ok (:services a))
                          (port/-closure (:engine ctx) p))]
      (let [step (plan/down-release active id (profile/project p) services action)
            res (execute! ctx p step)]
        (when (r/ok? res) (swap! (:state ctx) forget id))
        (persist! ctx)
        (r/map-ok res (fn [o] (-> (step-report (assoc step :result res)) (assoc :out (:out o)))))))))

(defn touch!
  "Reset the idle clock of active profile `id`."
  [ctx id]
  (if (active-entry ctx id)
    (let [now ((:now ctx))]
      (swap! (:state ctx) assoc-in [:active id :last-touch] now)
      (persist! ctx)
      (r/ok {:profile id :last-touch now}))
    (r/err :compose/not-active {:profile id})))

(defn- touch-if-active! [ctx p]
  (when (active-entry ctx (:profile/id p)) (touch! ctx (:profile/id p))))

(defn ps-profile
  "Result<{:profile :needs :running}> for `p`; touches it when active."
  [ctx p]
  (r/let-ok [needs (port/-closure (:engine ctx) p)
             running (port/-running (:engine ctx) p)]
    (touch-if-active! ctx p)
    (r/ok {:profile (:profile/id p) :project (profile/project p) :needs needs
           :running (vec (filter (set running) needs))})))

(defn logs-profile
  "Result<{:out}>: the last `tail` log lines of `p`; touches it when active."
  [ctx p tail]
  (r/let-ok [out (port/-logs (:engine ctx) p (or (:profile/services p) []) tail)]
    (touch-if-active! ctx p)
    (r/ok out)))

;; ---------------------------------------------------------------------------
;; the same by preset or active id

(defn up! [ctx id] (r/bind (lookup ctx id) #(up-profile! ctx %)))
(defn switch! [ctx id] (r/bind (lookup ctx id) #(switch-profile! ctx %)))
(defn down! [ctx id action] (r/bind (lookup ctx id) #(down-profile! ctx % action)))
(defn ps [ctx id] (r/bind (lookup ctx id) #(ps-profile ctx %)))
(defn logs [ctx id tail] (r/bind (lookup ctx id) #(logs-profile ctx % tail)))

;; ---------------------------------------------------------------------------
;; reaper

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

(defn- adopt-entry [p services now]
  {:profile/id (:profile/id p) :project (profile/project p) :services (vec services)
   :profile p :started-at now :last-touch now})

(defn adopt!
  "Take charge, with a fresh idle clock, of what runs without an owner:
   configured presets that are running, and the running services of configured
   compose projects that no active profile accounts for (as section
   `<project>/adopted`). Answers the adopted ids."
  [ctx]
  (let [{:keys [engine settings]} ctx
        now ((:now ctx))
        presets (vec (for [[id p] (sort-by key (:compose/profiles settings))
                           :when (not (active-entry ctx id))
                           :let [closure (port/-closure engine p)]
                           :when (r/ok? closure)
                           :let [running (port/-running engine p)]
                           :when (and (r/ok? running) (some (:ok running) (:ok closure)))]
                       (adopt-entry p (:ok closure) now)))
        _ (swap! (:state ctx) update :active into (map (juxt :profile/id identity)) presets)
        owned (fn [project] (into #{} (comp (filter #(= project (:project %))) (mapcat :services))
                                  (vals (:active @(:state ctx)))))
        strays (vec (for [[_ proj] (sort-by key (:compose/projects settings))
                          :let [whole (target/project-profile proj)
                                running (port/-running engine whole)]
                          :when (r/ok? running)
                          :let [unowned (remove (owned (profile/project whole)) (:ok running))]
                          :when (seq unowned)]
                      (adopt-entry (target/section proj ["adopted"] unowned) unowned now)))
        adopted (into presets strays)]
    (when (seq strays)
      (swap! (:state ctx) update :active
             (fn [active] (reduce (fn [m e] (update m (:profile/id e)
                                                     #(if % (update % :services (comp vec distinct into) (:services e)) e)))
                                  active strays))))
    (when (seq adopted) (persist! ctx))
    (mapv :profile/id adopted)))

;; ---------------------------------------------------------------------------
;; reads

(defn status
  "Every configured preset and every active section, with activity and reaper
   countdown."
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
     :projects (vec (sort (keys (:compose/projects settings))))
     :profiles (into (mapv (fn [[id p]] (row id p)) (sort-by key (:compose/profiles settings)))
                     (for [[id a] (sort-by key active)
                           :when (not (configured ctx id))]
                       (assoc (row id (:profile a)) :section? true)))}))

(defn targets
  "Result<[{:project :services :profiles}]>: what can be named as a target in each
   configured compose project."
  [ctx]
  (let [engine (:engine ctx)]
    (reduce (fn [acc [id proj]]
              (let [whole (target/project-profile proj)
                    x (r/let-ok [services (port/-listed engine whole)
                                 profiles (port/-profiles engine whole)]
                        (r/ok {:project id :always-on services :profiles (vec (sort profiles))}))]
                (if (r/err? x)
                  (reduced (assoc x :project id))
                  (r/map-ok acc #(conj % (:ok x))))))
            (r/ok [])
            (sort-by key (get-in ctx [:settings :compose/projects])))))

(defn projects
  "Result<[project]>: every compose project on the host, managed ones marked."
  [ctx]
  (let [managed (into #{} (map :project) (vals (:active @(:state ctx))))]
    (r/map-ok (port/-projects (:engine ctx))
              (fn [ps] (mapv #(assoc % :managed? (contains? managed (:name %))) ps)))))
