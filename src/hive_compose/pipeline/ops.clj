(ns hive-compose.pipeline.ops
  "Section and profile operations over a context:

     {:engine   IComposeEngine
      :runner   IProgramRunner                   ; optional: host programs
      :settings Settings
      :state    (atom {:active {id Active} :current id-or-nil})
      :now      (fn [] epoch-ms)
      :save!    (fn [persisted-state] Result)}   ; optional

   A subject is a Profile: a configured preset (`lookup`) or a section resolved
   from a target against the configured compose projects (`resolve-target`).
   Every operation answers a Result (reap! and adopt! answer reports). The
   active map changes only after the engine confirmed the action.

   A section also holds the host programs paired with its services: they start
   once its containers are up and stop when the last section holding them is
   released. A program that fails to start is reported, never fatal."
  (:require [hive-compose.port :as port]
            [hive-compose.promote.plan :as plan]
            [hive-compose.promote.profile :as profile]
            [hive-compose.promote.program :as program]
            [hive-compose.promote.target :as target]
            [hive-dsl.result :as r]
            [clojure.string :as str]))

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
   the project defines neither. `env` is the call's own, added to the
   project's for these compose queries too."
  [engine proj nm env]
  (let [whole (cond-> (target/project-profile proj)
                (seq env) (update :profile/env merge env))]
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
  [engine proj names env]
  (reduce (fn [acc nm]
            (let [x (expand-name engine proj nm env)]
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
   the compose file's depends_on when brought up. `env`, the call's own, is
   given to the compose queries that resolve the target, so a variable the
   YAML requires can come from the call."
  ([ctx target-str] (resolve-target ctx target-str nil))
  ([ctx target-str env]
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
           (let [x (expand-names (:engine ctx) proj names env)]
             (cond
               (r/err? x) x
               (nil? (:ok x)) (recur more)
               :else (r/ok (target/section proj names (:ok x)))))))))))

(defn subject
  "Result<Profile> for a tool call: `target` resolves a section, else `id`
   looks up a preset or an active section. A target is resolved against the
   config as it is now, so an active section picks up what its project gained
   since (a program, a TTL); the snapshot it ran with answers only when the
   target no longer resolves. `env` is the call's env, used by the compose
   queries that resolve the target."
  [ctx {:keys [target id env]}]
  (cond
    (seq target) (let [{:keys [project names]} (target/parse target)
                       sid (when project (target/section-id project names))
                       fresh (resolve-target ctx target env)]
                   (if (and (r/err? fresh) sid (active-entry ctx sid))
                     (lookup ctx sid)
                     fresh))
    (seq id) (lookup ctx id)
    :else (r/err :compose/missing-param {:param "profile or target"})))

(defn rebase-sections!
  "After the configured projects changed from `old-projects` to the ones in
   `ctx`'s settings, re-base the snapshot of every active section of a changed
   project on its project as configured now, so a release, ps or logs by id
   runs with the new files and env. Answers {:rebased [id] :kept [{:id :reason}]};
   a section whose project left the config keeps the snapshot it ran with."
  [ctx old-projects]
  (let [new-projects (get-in ctx [:settings :compose/projects])
        rows (for [[id a] (sort-by key (:active @(:state ctx)))
                   :let [pid (first (str/split id #"/" 2))]
                   :when (and (:profile a) (str/includes? id "/") (not (configured ctx id))
                              (contains? old-projects pid)
                              (not= (get old-projects pid) (get new-projects pid)))]
               (if-let [proj (get new-projects pid)]
                 {:id id :profile (target/rebase-section (get old-projects pid) proj (:profile a))}
                 {:id id :reason :project-removed}))
        rebased (filter :profile rows)]
    (when (seq rebased)
      (swap! (:state ctx) update :active
             (fn [m] (reduce (fn [m {:keys [id profile]}] (assoc-in m [id :profile] profile)) m rebased)))
      (persist! ctx))
    (cond-> {:rebased (mapv :id rebased)}
      (seq (remove :profile rows)) (assoc :kept (vec (remove :profile rows))))))

;; ---------------------------------------------------------------------------
;; release

(defn- runner
  "The program runner, nil when the context has none or programs are off."
  [ctx]
  (when (get-in ctx [:settings :compose/programs?] true) (:runner ctx)))

(defn- live-programs
  "{program-id Running} of the programs the active entries of `project` hold
   that are still alive."
  [ctx project]
  (if-let [rn (runner ctx)]
    (into {}
          (filter (fn [[_ run]] (port/-alive? rn (:pid run))))
          (program/running-in (:active @(:state ctx)) project nil))
    {}))

(defn- start-program!
  "Result<Running>: Program `p` of compose project `project`, completed from the
   build files of its directory and started. When its nREPL port already
   answers, somebody else runs it: nothing starts and the answer is
   {:external? true}, a program this addon does not hold and will not stop."
  [ctx project p]
  (let [rn (runner ctx)
        log (program/log-file (get-in ctx [:settings :compose/log-dir] "logs") project p)]
    (r/let-ok [facts (port/-facts rn (:program/dir p))
               ready (program/resolved p facts)]
      (let [nrepl (:program/nrepl-port ready)]
        (if (and nrepl (port/-listening? rn nrepl))
          (r/ok {:program/id (:program/id p) :external? true :nrepl-port nrepl
                 :dir (:program/dir p) :kind (:program/kind ready)})
          (r/let-ok [started (port/-start! rn ready log)]
            (r/ok (cond-> {:program/id (:program/id p)
                           :pid (:pid started)
                           :log log
                           :dir (:program/dir p)
                           :kind (:program/kind ready)
                           :started-at ((:now ctx))}
                    nrepl (assoc :nrepl-port nrepl)))))))))

(defn- start-programs!
  "Start the programs `p` pairs with `services`, reusing the live ones in
   `carried`. Answers {:held {program-id Running} :report [..]}; a program
   running outside the addon is reported and not held."
  [ctx p services carried]
  (if-not (runner ctx)
    {:held {} :report []}
    (reduce (fn [acc prog]
              (let [id (:program/id prog)]
                (if-let [run (get carried id)]
                  (-> acc
                      (assoc-in [:held id] run)
                      (update :report conj (assoc run :reused? true)))
                  (let [res (start-program! ctx (profile/project p) prog)]
                    (cond
                      (r/err? res) (update acc :report conj {:program/id id :error res})
                      (:external? (:ok res)) (update acc :report conj (:ok res))
                      :else (-> acc
                                (assoc-in [:held id] (:ok res))
                                (update :report conj (assoc (:ok res) :started? true))))))))
            {:held {} :report []}
            (program/wanted (:profile/programs p) services))))

(defn- stop-programs!
  "Stop the programs only active entry `id` holds, those in `keep` spared.
   Answers one report row per program."
  [ctx id keep]
  (if-let [rn (runner ctx)]
    (mapv (fn [[program-id run]]
            (let [res (port/-halt! rn (:pid run))]
              (cond-> {:program/id program-id :pid (:pid run) :ok? (r/ok? res)}
                (r/err? res) (assoc :error res))))
          (sort-by key (program/release-programs (:active @(:state ctx)) id keep)))
    []))

(defn- execute! [{:keys [engine]} p {:keys [action services]}]
  (case action
    :stop (port/-stop! engine p services)
    :down (port/-down! engine p services)))

(defn- forget [st id]
  (-> st
      (update :active dissoc id)
      (update :current #(when-not (= % id) %))))

(defn- release!
  "Run Release `step`; on success the profile stops the programs it alone holds
   (those in `keep` spared) and leaves the active map. Answers the step with its
   :result."
  ([ctx step] (release! ctx step #{}))
  ([ctx {id :profile/id :as step} keep]
   (let [res (r/bind (lookup ctx id) #(execute! ctx % step))]
     (if (r/ok? res)
       (let [stopped (stop-programs! ctx id keep)]
         (swap! (:state ctx) forget id)
         (cond-> (assoc step :result res)
           (seq stopped) (assoc :programs stopped)))
       (assoc step :result res)))))

(defn- step-report [{:keys [result] :as step}]
  (cond-> (-> step (dissoc :result) (assoc :ok? (r/ok? result)))
    (r/err? result) (assoc :error result)))

;; ---------------------------------------------------------------------------
;; operations on a Profile

(defn required-closure
  "What `roots` need under the depends_on edges of `graph`
   ({service {dep {:required? bool}}}): {:start [service]} follows only the
   required edges (an edge without :required? is required); {:needs [service]}
   adds the `required: false` dependencies met on the way that are already in
   `running`, together with everything those require in turn (and the running
   optional ones they meet), so release never strands a kept service. What
   :needs adds is waited on and kept, never started. Pure."
  [graph roots running]
  (let [running (set running)
        optional? (fn [[_ e]] (false? (:required? e)))
        walk (fn [seen roots]
               (loop [seen seen optional #{} todo (vec roots)]
                 (if-let [[s & more] (seq todo)]
                   (if (seen s)
                     (recur seen optional (vec more))
                     (let [edges (get graph s)]
                       (recur (conj seen s)
                              (into optional (map key) (filter optional? edges))
                              (into (vec more) (map key) (remove optional? edges)))))
                   [seen optional])))
        [start optional] (walk #{} roots)]
    (loop [kept start optional optional]
      (let [fresh (vec (sort (remove kept (filter running optional))))]
        (if (empty? fresh)
          {:start (vec (sort start)) :needs (vec (sort kept))}
          (let [[kept more] (walk kept fresh)]
            (recur kept more)))))))

(defn- up-plan
  "Result<{:start :needs :pin?}> for bringing `p` up. The engine's closure
   follows every depends_on entry; when the engine also answers the dependency
   graph, the entries marked `required: false` are left out of :start (never
   built or started) and kept in :needs only when already running, with what
   they require. :pin? is true when something was left out, so compose must
   start exactly :start. Without a graph, with no optional edge in the
   closure, or with a graph that disagrees with the closure (a closure service
   it has no entry for, a required dependency outside the closure), :start and
   :needs are the closure and nothing is pinned, as before."
  [ctx p]
  (let [engine (:engine ctx)
        whole (fn [closure] (r/ok {:start closure :needs closure :pin? false}))]
    (r/let-ok [closure (port/-closure engine p)]
      (if-not (satisfies? port/IDependencyGraph engine)
        (whole closure)
        (r/let-ok [graph (port/-dependencies engine p)]
          (let [in-closure (set closure)
                complete? (every? #(contains? graph %) closure)
                optional? (some (fn [s] (some #(false? (:required? %)) (vals (get graph s))))
                                closure)]
            (if-not (and complete? optional?)
              (whole closure)
              (r/let-ok [roots (if (seq (:profile/services p))
                                 (r/ok (:profile/services p))
                                 (port/-listed engine p))
                         running (port/-running engine p)]
                (let [{:keys [start needs]} (required-closure graph (filter in-closure roots) running)]
                  (if-not (and (every? in-closure start) (every? in-closure needs))
                    (whole closure)
                    (r/ok {:start start :needs needs :pin? (not= start closure)})))))))))))

(defn- bring-up!
  "Start `services` of `p`, then the programs paired with them (`carried` are
   live ones to reuse), and track the profile as active. `needs` is the
   depends_on closure of `services`, kept when wider so release spares it.
   `pin?` starts exactly `services` (compose --no-deps), so compose does not
   pull in the optional dependencies the closure left out."
  [ctx p services needs carried pin?]
  (let [id (:profile/id p)]
    (r/let-ok [out (port/-up! (:engine ctx) (cond-> p pin? (assoc :profile/no-deps? true)) services)]
      (let [now ((:now ctx))
            {:keys [held report]} (start-programs! ctx p services carried)
            wider (seq (remove (set services) needs))]
        (swap! (:state ctx) update-in [:active id]
               (fn [a] (cond-> {:profile/id id
                                :project (profile/project p)
                                :services services
                                :profile p
                                :started-at (or (:started-at a) now)
                                :last-touch now}
                         wider (assoc :needs (vec needs))
                         (seq held) (assoc :programs held))))
        (r/ok (cond-> {:profile id :project (profile/project p) :services services :out (:out out)}
                wider (assoc :needs (vec needs))
                (seq report) (assoc :programs report)))))))

(defn up-profile!
  "Bring `p` up alongside whatever is active. Becomes current when nothing is.
   A depends_on entry marked `required: false` is neither built nor started;
   it is only needed when already running."
  [ctx p]
  (r/let-ok [{:keys [start needs pin?]} (up-plan ctx p)
             up (bring-up! ctx p (profile/up-services p start) needs
                           (live-programs ctx (profile/project p)) pin?)]
    (swap! (:state ctx) update :current #(or % (:profile/id p)))
    (persist! ctx)
    (r/ok up)))

(defn switch-profile!
  "Make `p` current: release what only the previous current profile needs, then
   bring `p` up. Shared services and the programs `p` wants stay up throughout.
   Optional (`required: false`) dependencies are treated as in `up-profile!`."
  [ctx p]
  (r/let-ok [{:keys [start needs pin?]} (up-plan ctx p)]
    (let [id (:profile/id p)
          project (profile/project p)
          services (profile/up-services p start)
          {:keys [active current]} @(:state ctx)
          carried (live-programs ctx project)
          keep (if (= project (get-in active [current :project]))
                 (into #{} (map :program/id) (program/wanted (:profile/programs p) services))
                 #{})
          released (mapv #(step-report (release! ctx % keep))
                         (plan/switch-releases active current id project needs))
          up (bring-up! ctx p services needs carried pin?)]
      (when (r/ok? up) (swap! (:state ctx) assoc :current id))
      (persist! ctx)
      (r/map-ok up #(assoc % :released released)))))

(defn down-profile!
  "Take `p` down with `action` (:down removes containers, :stop keeps them;
   volumes are never touched). Services and programs another active profile
   needs stay up."
  [ctx p action]
  (let [id (:profile/id p)
        active (:active @(:state ctx))]
    (r/let-ok [services (if-let [a (get active id)]
                          (r/ok (:services a))
                          (port/-closure (:engine ctx) p))]
      (let [step (plan/down-release active id (profile/project p) services action)
            res (execute! ctx p step)
            stopped (when (r/ok? res) (stop-programs! ctx id #{}))]
        (when (r/ok? res) (swap! (:state ctx) forget id))
        (persist! ctx)
        (r/map-ok res (fn [o] (cond-> (-> (step-report (assoc step :result res)) (assoc :out (:out o)))
                                (seq stopped) (assoc :programs stopped))))))))

(defn touch!
  "Reset the idle clock of active profile `id`."
  [ctx id]
  (if (active-entry ctx id)
    (let [now ((:now ctx))]
      (swap! (:state ctx) assoc-in [:active id :last-touch] now)
      (persist! ctx)
      (r/ok {:profile id :last-touch now}))
    (r/err :compose/not-active {:profile id})))

(defn touch-all!
  "Reset the idle clock of every active profile, or only those of compose
   project `project` (the configured project id is accepted too)."
  [ctx project]
  (let [compose-name (some->> project
                              (get (get-in ctx [:settings :compose/projects]))
                              target/project-profile
                              profile/project)
        names (set (remove nil? [project compose-name]))
        wanted? (fn [a] (or (nil? project) (contains? names (:project a))))
        ids (vec (sort (for [[id a] (:active @(:state ctx)) :when (wanted? a)] id)))
        now ((:now ctx))]
    (swap! (:state ctx) update :active
           (fn [m] (reduce #(assoc-in %1 [%2 :last-touch] now) m ids)))
    (when (seq ids) (persist! ctx))
    (r/ok {:touched ids :last-touch now})))

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
   the addon), stopping the programs they alone hold. Answers the forgotten ids."
  [ctx]
  (let [dropped (vec (for [[id a] (:active @(:state ctx))
                           :let [running (r/bind (lookup ctx id) #(port/-running (:engine ctx) %))]
                           :when (and (r/ok? running) (not-any? (:ok running) (:services a)))]
                       id))]
    (when (seq dropped)
      (doseq [id dropped]
        (stop-programs! ctx id #{})
        (swap! (:state ctx) forget id))
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

(defn- adopt-entry
  ([p services now] (adopt-entry p services services now))
  ([p services needs now]
   (cond-> {:profile/id (:profile/id p) :project (profile/project p) :services (vec services)
            :profile p :started-at now :last-touch now}
     (seq (remove (set services) needs)) (assoc :needs (vec needs)))))

(defn merge-adopted
  "Active entry `old` with the newly adopted strays of entry `e` folded in;
   `e` itself when there is no `old`. Pure.

   The `:profile` snapshot's `:profile/services` follows the merged
   `:services`: logs and ps address a section through that snapshot, so a
   stale one would keep naming the subset adopted first. An entry without
   `:needs` needs exactly its `:services`, so a merged `:needs` starts from
   whichever of the two `old` carries."
  [old e]
  (if old
    (let [services (vec (distinct (into (:services old) (:services e))))]
      (cond-> (assoc old :services services)
        (:profile old) (assoc-in [:profile :profile/services] services)
        (or (:needs old) (:needs e))
        (assoc :needs (vec (distinct (into (or (:needs old) (:services old))
                                           (or (:needs e) (:services e))))))))
    e))

(defn adopt!
  "Take charge, with a fresh idle clock, of what runs without an owner:
   configured presets that are running, and the running services of configured
   compose projects that no active profile accounts for (as section
   `<project>/adopted`, needing their depends_on closure). Configured projects
   that share one compose project adopt each stray once, in id order. Answers
   the adopted ids."
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
        owned (fn [project taken]
                (into #{} (comp (filter #(= project (:project %))) (mapcat :services))
                      (concat (vals (:active @(:state ctx))) taken)))
        strays (reduce (fn [taken [_ proj]]
                         (let [whole (target/project-profile proj)
                               running (port/-running engine whole)
                               unowned (when (r/ok? running)
                                         (vec (remove (owned (profile/project whole) taken) (:ok running))))]
                           (if (seq unowned)
                             (let [closure (port/-closure engine (assoc whole :profile/services unowned))
                                   needs (if (r/ok? closure) (:ok closure) unowned)]
                               (conj taken (adopt-entry (target/section proj ["adopted"] unowned)
                                                        unowned needs now)))
                             taken)))
                       []
                       (sort-by key (:compose/projects settings)))
        adopted (into presets strays)]
    (when (seq strays)
      (swap! (:state ctx) update :active
             (fn [active] (reduce (fn [m e] (update m (:profile/id e) merge-adopted e))
                                  active strays))))
    (when (seq adopted) (persist! ctx))
    (mapv :profile/id adopted)))

;; ---------------------------------------------------------------------------
;; reads

(defn- program-row
  "What is known of started program `run` right now: alive, and whether its
   nREPL answers. A port the program chose itself is read from its .nrepl-port."
  [ctx run]
  (let [rn (runner ctx)
        port (or (:nrepl-port run) (when rn (port/-nrepl-port rn (:dir run))))]
    (cond-> (select-keys run [:program/id :pid :kind :log :dir])
      rn (assoc :alive? (port/-alive? rn (:pid run)))
      port (assoc :nrepl-port port)
      (and rn port) (assoc :nrepl-ready? (port/-listening? rn port)))))

(defn config-health
  "{project-id {:compose-project name :ok? bool :error ..}}: whether `docker
   compose config` accepts each configured project as configured now (its
   files and env; references rendered, never read). A missing required
   variable, a bad file or an absent overlay shows here."
  [ctx]
  (into (sorted-map)
        (for [[id proj] (get-in ctx [:settings :compose/projects])
              :let [whole (target/project-profile proj)
                    res (port/-listed (:engine ctx) whole)]]
          [id (cond-> {:compose-project (profile/project whole) :ok? (r/ok? res)}
                (r/err? res) (assoc :error (select-keys res [:error :exit :stderr])))])))

(defn status
  "Every configured preset and every active section, with activity, reaper
   countdown and the host programs it holds. Up front, :ok? and :config say
   whether compose accepts each configured project now; a row of a project it
   rejects carries :config-ok? false, and an active one :reap-blocked? true,
   since no compose call on that project, the reaper's included, can run."
  [ctx]
  (let [{:keys [settings]} ctx
        {:keys [active current]} @(:state ctx)
        now ((:now ctx))
        health (config-health ctx)
        broken (into {} (for [[id h] health :when (not (:ok? h))] [(:compose-project h) id]))
        row (fn [id p]
              (let [a (get active id)
                    ttl (profile/ttl-ms settings p)
                    bad (get broken (profile/project p))]
                (cond-> {:id id
                         :project (profile/project p)
                         :active? (some? a)
                         :ttl-minutes (quot ttl 60000)
                         :idle-action (profile/idle-action settings p)}
                  (:profile/description p) (assoc :description (:profile/description p))
                  bad (assoc :config-ok? false :config-project bad)
                  a (assoc :services (:services a)
                           :idle-seconds (quot (- now (:last-touch a)) 1000)
                           :reap-in-seconds (max 0 (quot (- (+ (:last-touch a) ttl) now) 1000)))
                  (and a bad) (assoc :reap-blocked? true)
                  (seq (:programs a)) (assoc :programs (mapv #(program-row ctx (val %))
                                                             (sort-by key (:programs a)))))))]
    (cond-> {:ok? (empty? broken)
             :config health
             :current current
             :projects (vec (sort (keys (:compose/projects settings))))
             :profiles (into (mapv (fn [[id p]] (row id p)) (sort-by key (:compose/profiles settings)))
                             (for [[id a] (sort-by key active)
                                   :when (not (configured ctx id))]
                               (assoc (row id (:profile a)) :section? true)))}
      (seq broken) (assoc :problem (str "compose rejects the config of "
                                        (str/join ", " (sort (vals broken)))
                                        "; every compose call on it fails, the reaper's included (see :config)")))))

(defn programs
  "Result<[row]>: every program the configured projects declare, as it would
   start (kind, command, nREPL port, the services it is paired with) and, when
   it runs, its pid and whether its nREPL answers."
  [ctx]
  (let [rn (runner ctx)]
    (r/ok
     (vec
      (for [[id proj] (sort-by key (get-in ctx [:settings :compose/projects]))
            :let [project (profile/project (target/project-profile proj))
                  live (live-programs ctx project)]
            p (:project/programs proj)
            :let [ready (if rn
                          (r/bind (port/-facts rn (:program/dir p)) #(program/resolved p %))
                          (r/ok p))
                  run (get live (:program/id p))]]
        (cond-> {:project id :program/id (:program/id p) :dir (:program/dir p)
                 :with (vec (:program/with p)) :running? (some? run)}
          (r/ok? ready) (merge (select-keys (:ok ready) [:program/kind :program/command :program/nrepl-port]))
          (r/err? ready) (assoc :error ready)
          run (merge (select-keys (program-row ctx run) [:pid :log :alive? :nrepl-ready?]))))))))

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

(defn- label-files
  "The compose files docker recorded on a project's containers (`ls`
   ConfigFiles, comma-separated absolute paths)."
  [s]
  (vec (remove str/blank? (map str/trim (str/split (or s "") #",")))))

(defn- configured-files
  "Absolute compose files configured project `proj` runs with: its
   :project/files resolved against its dir, else compose's own defaults (nil)."
  [proj]
  (when-let [fs (seq (:project/files proj))]
    (mapv #(if (str/starts-with? % "/") % (str (str/replace (:project/dir proj) #"/+$" "") "/" %)) fs)))

(defn projects
  "Result<[project]>: every compose project on the host, managed ones marked.
   :label-files are the files docker recorded when its containers were
   created. For a project a configured one runs as, :configured lists those
   with their :files as configured now, and :stale-files the label files
   no configured project of that name uses (a different checkout, an overlay
   since dropped); :files-match? false says the running containers were not
   created from the files configured now."
  [ctx]
  (let [managed (into #{} (map :project) (vals (:active @(:state ctx))))
        by-name (group-by (comp profile/project target/project-profile)
                          (vals (get-in ctx [:settings :compose/projects])))]
    (r/map-ok (port/-projects (:engine ctx))
              (fn [ps]
                (mapv (fn [{nm :name :as row}]
                        (let [labels (label-files (:config-files row))
                              confs (get by-name nm)
                              conf-rows (vec (for [p (sort-by :project/id confs)]
                                               (cond-> {:id (:project/id p)}
                                                 (configured-files p) (assoc :files (configured-files p)))))
                              known (set (mapcat :files conf-rows))
                              explicit? (every? :files conf-rows)
                              stale (when (and (seq confs) explicit?) (vec (remove known labels)))]
                          (cond-> (-> row
                                      (dissoc :config-files)
                                      (assoc :managed? (contains? managed nm) :label-files labels))
                            (seq confs) (assoc :configured conf-rows)
                            (and (seq confs) explicit?)
                            (assoc :stale-files stale
                                   :files-match? (boolean (some #(= (set labels) (set (:files %))) conf-rows))))))
                      ps)))))
