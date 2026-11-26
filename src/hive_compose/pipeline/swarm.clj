(ns hive-compose.pipeline.swarm
  "Swarm work on the sections the addon runs, over the ops context plus:

     {:swarm    ISwarm      ; optional: live agents and notices
      :activity IActivity}  ; optional: connections to local ports

   * routes: which shared nREPL drives which module (a program directory).
   * claims: an agent declares it works on a section; a claim keeps the
     section alive while renewed, and ends after the claim TTL or when the
     agent leaves the swarm.
   * observe!: one pass of the reaper. It expires claims, counts as use the
     claims that hold, the agents whose cwd lies in the section's project and
     the connections newly opened to its nREPL (and, opt-in per project with
     :project/activity, published) ports, the connections its own containers
     hold to those left out, and (opt-in with :cpu) its services busier than
     :compose/cpu-busy-percent, touches what is in use, records which source
     counted (:usage :sources), marks a section whose last claim ended orphaned
     (reaped after the shorter orphan TTL), and tells each agent once which
     shared nREPL drives the module it works in.

   Nothing here starts a JVM for an agent: routes point at the programs the
   sections run, so a swarm shares one nREPL per module instead of each agent
   booting its own."
  (:require [clojure.string :as str]
            [hive-compose.pipeline.ops :as ops]
            [hive-compose.port :as port]
            [hive-compose.promote.config :as config]
            [hive-compose.promote.plan :as plan]
            [hive-compose.promote.profile :as profile]
            [hive-compose.promote.program :as program]
            [hive-compose.promote.swarm :as sw]
            [hive-compose.promote.target :as target]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- persist! [{:keys [state save!]}]
  (when save! (save! (select-keys @state [:active :current]))))

(defn- setting
  "Setting `k` of `ctx`, its default when the settings predate it."
  [ctx k]
  (get-in ctx [:settings k] (get config/defaults k)))

(defn- swarm [ctx]
  (when (setting ctx :compose/swarm?) (:swarm ctx)))

(defn- agents
  "[agent] of the swarm, nil when no swarm answers."
  [ctx]
  (when-let [s (swarm ctx)]
    (let [res (try (port/-agents s) (catch Throwable _swarm-threw nil))]
      (when (and res (r/ok? res)) (:ok res)))))

(defn- notify!
  "Result of telling the swarm `event` with `data`; nil when notices are off or
   no swarm is wired. Never throws."
  [ctx event data]
  (when-let [s (and (setting ctx :compose/notify?) (:swarm ctx))]
    (try (port/-notify! s event data)
         (catch Throwable t (r/err :compose/notice-failed {:message (ex-message t)})))))

(defn- projects-named
  "Configured projects that run as compose project `compose-name`."
  [ctx compose-name]
  (filter #(= compose-name (profile/project (target/project-profile %)))
          (vals (get-in ctx [:settings :compose/projects]))))

;; ---------------------------------------------------------------------------
;; routes

(defn- route
  "The route of `program` of compose project `cname` (configured `project`),
   nil when it has no nREPL port to point at."
  [rn active project cname program]
  (let [pid (:program/id program)
        held (some (fn [[_ a]] (when (= cname (:project a)) (get (:programs a) pid))) active)
        live (when (and held (port/-alive? rn (:pid held))) held)
        ready (r/bind (port/-facts rn (:program/dir program)) #(program/resolved program %))
        nrepl (or (:nrepl-port live)
                  (when live (port/-nrepl-port rn (:dir live)))
                  (when (r/ok? ready) (:program/nrepl-port (:ok ready)))
                  (port/-nrepl-port rn (:program/dir program)))
        listening? (boolean (and nrepl (port/-listening? rn nrepl)))]
    (when nrepl
      (cond-> {:dir (sw/clean-dir (:program/dir program))
               :program/id pid
               :project project
               :compose-project cname
               :nrepl-port nrepl
               :running? (or (some? live) listening?)
               :nrepl-ready? listening?
               :sections (vec (sort (for [[sid a] active
                                          :when (and (= cname (:project a)) (contains? (:programs a) pid))]
                                      sid)))}
        live (assoc :pid (:pid live))
        (and listening? (not live)) (assoc :external? true)
        (r/ok? ready) (assoc :kind (:program/kind (:ok ready)))))))

(defn routes
  "[route]: every program with an nREPL that a configured project declares or
   an active section holds, where its nREPL listens and whether it answers
   now. A route whose nREPL answers but that no section holds is :external?
   (somebody else runs it; it is shared all the same). Empty without a
   program runner."
  [ctx]
  (if-let [rn (ops/runner ctx)]
    (let [active (:active @(:state ctx))
          declared (for [[id proj] (sort-by key (get-in ctx [:settings :compose/projects]))
                         :let [cname (profile/project (target/project-profile proj))]
                         p (:project/programs proj)]
                     [id cname p])
          known (into #{} (map (fn [[_ cname p]] [cname (:program/id p)])) declared)
          held (for [[_ a] (sort-by key active)
                     [pid run] (sort-by key (:programs a))
                     :when (not (contains? known [(:project a) pid]))]
                 [(:project a) (:project a) {:program/id pid :program/dir (:dir run)}])]
      (vec (distinct (keep (fn [[id cname p]] (route rn active id cname p))
                           (concat declared held)))))
    []))

(defn- with-advice [rs]
  (mapv #(assoc % :advice (sw/route-advice %)) rs))

(defn nrepl-answer
  "{:nrepl [route] :nrepl-not-running n :nrepl-hint s}, merged into a claim
   answer: :nrepl is always a list, every route with its advice for a first
   claim or an adoption; for a renewal (`renewed?`) only the running ones,
   the others counted beside it with the hint to ask for one by path (card
   20261123125953-1956fd03: a renewal repeated ~1k tokens of advice)."
  [rs renewed?]
  (if-not renewed?
    {:nrepl (with-advice rs)}
    (let [{running true others false} (group-by (comp boolean :running?) rs)]
      (cond-> {:nrepl (with-advice (vec running))}
        (seq others) (assoc :nrepl-not-running (count others)
                            :nrepl-hint "compose nrepl path=<file> answers the route of a module that does not run")))))

(defn section-routes
  "The routes of active entry `a`'s compose project: the shared nREPLs of the
   app it runs."
  [rs a]
  (filterv #(= (:project a) (:compose-project %)) rs))

(defn- section-dirs
  "Directories whose agents count as working on active entry `a`."
  [ctx rs a]
  (vec (distinct (concat (mapcat sw/project-dirs (projects-named ctx (:project a)))
                         (keep :dir rs)
                         (keep (comp sw/clean-dir :dir) (vals (:programs a)))))))

(defn- published-ports
  "Host ports the running services of active entry `a` publish; #{} when the
   engine cannot tell."
  [ctx a]
  (let [engine (:engine ctx) p (:profile a)]
    (if (and p (satisfies? port/IPublishedPorts engine))
      (let [res (port/-published engine p)]
        (if (r/ok? res)
          (into #{} (mapcat val) (select-keys (:ok res) (plan/footprint a)))
          #{}))
      #{})))

(defn- container-addresses
  "The addresses the running containers of active entry `a`'s project hold:
   peers from there are the project talking to itself, not use. #{} when the
   engine cannot tell."
  [ctx a]
  (let [engine (:engine ctx) p (:profile a)]
    (if (and p (satisfies? port/IContainerAddresses engine))
      (let [res (port/-addresses engine p)]
        (if (r/ok? res) (set (:ok res)) #{}))
      #{})))

(defn- section-profile
  "The Profile of active entry `sid`/`a`: as configured now, else as it ran."
  [ctx sid a]
  (or (get-in ctx [:settings :compose/profiles sid]) (:profile a)))

(defn section-activity
  "Which new connections count as use of active section `sid`/`a`: its
   project's :project/activity (a preset's :profile/activity), else the
   global :compose/activity."
  [ctx sid a]
  (profile/activity (merge config/defaults (:settings ctx)) (section-profile ctx sid a)))

(defn- watched-ports
  "{port source} the pass watches for active entry `a` under activity
   `kinds`: the nREPLs of its running routes `srs` (:nrepl) and the host ports
   its containers publish (:published). A port that is both is :nrepl."
  [ctx a srs kinds]
  (merge (when (contains? kinds :published)
           (zipmap (published-ports ctx a) (repeat :published)))
         (when (contains? kinds :nrepl)
           (zipmap (keep #(when (:running? %) (:nrepl-port %)) srs) (repeat :nrepl)))))

(defn- busy
  "{service percent} of active entry `a`'s services busier than
   :compose/cpu-busy-percent, when activity `kinds` holds :cpu; {} otherwise
   and when the engine cannot tell."
  [ctx a kinds]
  (let [engine (:engine ctx) p (:profile a)]
    (if (and p (contains? kinds :cpu) (satisfies? port/IContainerLoad engine))
      (let [res (try (port/-cpu engine p) (catch Throwable _probe-threw nil))]
        (if (and res (r/ok? res))
          (sw/busy-services (:ok res) (setting ctx :compose/cpu-busy-percent) (plan/footprint a))
          {}))
      {})))

;; ---------------------------------------------------------------------------
;; observe

(defn- notice-data
  "The :nrepl/route notice for one agent, addressed to it (:to)."
  [{:keys [agent section routes]}]
  {:message (str agent " works in " section "'s project: "
                 (str/join "; " (map sw/route-advice routes)))
   :to agent
   :agent agent
   :section section
   :nrepl (mapv #(select-keys % [:dir :program/id :nrepl-port]) routes)})

(defn- notice-channel
  "The channel a delivered notice result `res` went out on: :direct or
   :broadcast (an ISwarm answering a bare true counts as :broadcast)."
  [res]
  (let [v (:ok res)]
    (or (when (map? v) (:channel v)) :broadcast)))

(defn observe!
  "One usage pass over the active sections (see the ns doc). Answers
   {:swarm :ok|:absent|:off :agents n :in-use [id] :expired [{:section :agent
   :reason}] :orphaned [id] :notified [{:agent :section :channel}]}, where
   :channel says how the agent's route notice went out: :direct to it, or
   :broadcast when no directed channel resolved. A route notice goes to the
   agents a section concerns: those holding a claim on it and those whose
   cwd lies in its project's directories."
  [ctx]
  (let [now ((:now ctx))
        wired (swarm ctx)
        found (agents ctx)
        live (when found (into #{} (map :agent/id) found))
        claim-ttl (* 60000 (setting ctx :compose/claim-ttl-minutes))
        rs (routes ctx)
        active (:active @(:state ctx))
        seen (into (sorted-map)
                   (for [[sid a] active
                         :let [srs (section-routes rs a)
                               kinds (section-activity ctx sid a)]]
                     [sid {:routes srs
                           :agents (if found (sw/agents-in found (section-dirs ctx srs a)) [])
                           :ports (watched-ports ctx a srs kinds)
                           :busy (busy ctx a kinds)
                           :expired (sw/expired-claims (:claims a) live now claim-ttl)}]))
        all-ports (into #{} (mapcat (comp keys :ports)) (vals seen))
        peers-now (if (and (:activity ctx) (seq all-ports))
                    (try (port/-peers (:activity ctx) all-ports) (catch Throwable _probe-threw {}))
                    {})
        fresh (sw/fresh-peers (or (:peers @(:state ctx)) {}) peers-now)
        verdicts (into (sorted-map)
                       (for [[sid {:keys [agents ports busy expired]}] seen
                             :let [claims (apply dissoc (get-in active [sid :claims]) (keys expired))
                                   own (if (sw/published-peers? fresh ports)
                                         (container-addresses ctx (get active sid))
                                         #{})]]
                         [sid {:claims claims
                               :expired expired
                               :usage (sw/usage {:claims claims :agents agents
                                                 :fresh (sw/section-fresh fresh ports own)
                                                 :sources ports
                                                 :busy busy})}]))
        orphaned (vec (for [[sid {:keys [claims expired usage]}] verdicts
                            :when (and (seq expired) (empty? claims) (not (:in-use? usage)))]
                        sid))]
    (swap! (:state ctx)
           (fn [st]
             (-> st
                 (assoc :peers peers-now)
                 (update :active
                         (fn [m]
                           (reduce (fn [m [sid {:keys [claims usage]}]]
                                     (if-not (contains? m sid)
                                       m
                                       (update m sid
                                               (fn [a]
                                                 (cond-> (assoc a :usage (assoc usage :at now))
                                                   (seq claims) (assoc :claims claims)
                                                   (empty? claims) (dissoc :claims)
                                                   (some #{sid} orphaned) (assoc :orphaned-at now)
                                                   (:in-use? usage) (-> (assoc :last-touch now)
                                                                        (dissoc :orphaned-at)))))))
                                   m verdicts))))))
    (let [told (or (:told @(:state ctx)) #{})
          working (into {} (for [[sid {:keys [agents]}] seen
                                 :let [concerned (sw/concerned agents (keys (get-in verdicts [sid :claims])))]
                                 :when (seq concerned)]
                             [sid concerned]))
          due (when (and wired (setting ctx :compose/notify?))
                (sw/notices working (into {} (map (fn [[sid v]] [sid (:routes v)])) seen) told))
          sent (vec (for [n due
                          :let [res (notify! ctx :nrepl/route (notice-data n))]
                          :when (and res (r/ok? res))]
                      {:agent (:agent n) :section (:section n) :channel (notice-channel res)}))
          active-now (:active @(:state ctx))
          keep? (fn [[agent sid]]
                  (and (contains? active-now sid)
                       (or (nil? live) (contains? live agent)
                           (contains? (get-in active-now [sid :claims]) agent))))]
      (swap! (:state ctx) assoc :told (into #{} (filter keep?)
                                            (into told (map (juxt :agent :section)) sent)))
      (persist! ctx)
      {:swarm (cond (nil? wired) :off found :ok :else :absent)
       :agents (count found)
       :in-use (vec (for [[sid v] verdicts :when (:in-use? (:usage v))] sid))
       :expired (vec (for [[sid v] verdicts [agent reason] (:expired v)]
                       {:section sid :agent agent :reason reason}))
       :orphaned orphaned
       :notified sent})))

;; ---------------------------------------------------------------------------
;; claims

(defn- reap-in-seconds [ctx p a]
  (max 0 (quot (- (+ (:last-touch a) (plan/effective-ttl-ms (:settings ctx) p a)) ((:now ctx))) 1000)))

(defn- synced-active
  "The active entry `sid` after merging the state file in (`:sync!`, when the
   context has one): another addon instance sharing the file (a hot remount,
   the control API) may have brought the section up since this one last read."
  [ctx sid]
  (when-let [sync! (:sync! ctx)]
    (try (sync!) (catch Throwable _sync-failed nil)))
  (get-in @(:state ctx) [:active sid]))

(defn- adopt-running!
  "Result<entry-or-nil>: when the addon does not track section `p` but every
   service it would start already runs (a section reconcile forgot while its
   containers were being recreated, or one brought up outside this
   instance), track it as active WITHOUT calling the engine to start
   anything, and answer the new entry. nil when something it needs is down."
  [ctx p0]
  (let [engine (:engine ctx)
        ;; what the running containers were started from, as far as a holder says
        {p :profile assumed? :assumed?} (ops/adopt-profile ctx p0)]
    (r/let-ok [closure (if (program/programs-only? p) (r/ok []) (port/-closure engine p))
               running (if (program/programs-only? p) (r/ok #{}) (port/-running engine p))]
      (let [services (profile/up-services p closure)
            now ((:now ctx))]
        (if (and (seq services) (every? (set running) services))
          ;; the containers run: start only the host programs they pair with
          ;; (no engine call), as `up` would have, so the claimer gets its nREPL
          (let [{:keys [held report]} (ops/start-section-programs! ctx p services)
                not-started (filterv #(or (:error %) (:exited? %)) report)]
            (swap! (:state ctx) update-in [:active (:profile/id p)]
                   (fn [a] (or a (cond-> {:profile/id (:profile/id p) :project (profile/project p)
                                          :services (vec services) :profile p
                                          :started-at now :last-touch now}
                                   (seq (remove (set services) closure)) (assoc :needs (vec closure))
                                   (seq held) (assoc :programs held)
                                   assumed? (assoc :inputs :assumed)))))
            (r/ok {:entry (get-in @(:state ctx) [:active (:profile/id p)])
                   :programs report
                   :not-started not-started}))
          (r/ok nil))))))

(defn- claim-ttl-ms [ctx] (* 60000 (setting ctx :compose/claim-ttl-minutes)))

(defonce ^:private section-locks
  ;; One monitor per section id, for the life of the JVM: a claim's sync,
  ;; adopt, start and write run under it (see `claim!`). A dedicated Object per
  ;; id from this registry, never an interned string, which any code could
  ;; lock. Bounded by the number of section ids ever claimed.
  (java.util.concurrent.ConcurrentHashMap.))

(defn- section-lock
  "The monitor `claim!` holds for section `sid`."
  [sid]
  (.computeIfAbsent ^java.util.concurrent.ConcurrentHashMap section-locks sid
                    (reify java.util.function.Function (apply [_ _] (Object.)))))

(declare claim-locked!)

(defn claim!
  "Result: `agent` claims section `p` (see `claim-locked!`), serialized per
   section id: concurrent claims on one section run their state-file sync,
   adopt, start and claim write one after another, so the second sees the
   section the first adopted or started and renews it instead of starting
   its host programs or containers again. Claims on other sections are not
   held up."
  [ctx p agent opts]
  (locking (section-lock (:profile/id p))
    (claim-locked! ctx p agent opts)))

(defn- claim-locked!
  "Result: `agent` claims section `p`. The answer carries the section's shared
   nREPL routes with advice, so the agent drives them instead of starting its
   own JVM; `note` says what the agent works on.

   A claim on an ACTIVE section never calls the engine to start, recreate or
   restart anything: it writes the claim, touches the section and answers
   :renewed (the services as they run). A section the addon does not track
   whose services all run is taken in the same way (:adopted), its host
   programs started as `up` would (host only; one that cannot start is
   named under :programs-not-started and the claim still holds). A section
   not (fully) running is brought up (:started) by `start!`, a thunk the
   caller passes so the start goes through the same ownership and orphan
   guards as `up` (the addon does; a refusal is the claim's error), else by
   a plain `ops/up-profile!`.

   `ttl-minutes` makes it a WAVE claim (see `promote.swarm/claim-end`): live
   until now + ttl-minutes whether or not `agent` (an agent or a
   coordinator) stays in the swarm; it needs a `note` and is bounded by
   :compose/claim-max-ttl-minutes. Claiming again moves its end."
  [ctx p agent {:keys [note ttl-minutes start!]}]
  (if (str/blank? agent)
    (r/err :compose/missing-param {:param "agent" :hint "the claiming agent's id"})
    ;; validated up front, so a refused wave claim starts nothing; its end is
    ;; taken again from the clock read after a start (`until` below)
    (r/let-ok [_valid (sw/wave-until ((:now ctx)) ttl-minutes
                                     (setting ctx :compose/claim-max-ttl-minutes) note)]
      (let [sid (:profile/id p)
            known (synced-active ctx sid)
            adopted (when-not known (adopt-running! ctx p))
            taken (when (and adopted (r/ok? adopted)) (:ok adopted))
            ;; the start goes through the caller's guards (the addon passes the
            ;; ownership and orphan guards `up` runs), else a plain up
            up (when-not (or known taken)
                 (if start! (start!) (ops/up-profile! ctx p)))]
        (cond
          (and up (r/err? up)) up
          (nil? (get-in @(:state ctx) [:active sid]))
          ;; a reaper pass forgot the section between the check and the write:
          ;; never resurrect a partial entry
          (r/err :compose/not-active {:profile sid :hint "the section was released meanwhile; claim again"})
          :else
          (let [now ((:now ctx))
                until (:ok (sw/wave-until now ttl-minutes (setting ctx :compose/claim-max-ttl-minutes) note))
                live (some->> (agents ctx) (into #{} (map :agent/id)))
                swarm? (when live (contains? live agent))
                before (get-in @(:state ctx) [:active sid :claims agent])
                lapsed? (sw/lapsed-wave before now until)
                [old _] (swap-vals! (:state ctx)
                                    (fn [st] (if (get-in st [:active sid])
                                               (update-in st [:active sid]
                                                          #(-> %
                                                               (update :claims sw/claim agent now {:note note :swarm? swarm? :until until})
                                                               (assoc :last-touch now)
                                                               (dissoc :orphaned-at)))
                                               st)))
                a (get-in @(:state ctx) [:active sid])
                c (get-in a [:claims agent])]
            (persist! ctx)
            (if-not (get-in old [:active sid])
              ;; the reaper forgot it between the check and the write: no claim was written
              (r/err :compose/not-active {:profile sid :hint "the section was released meanwhile; claim again"})
              (r/ok (cond-> (merge {:profile sid
                                    :agent agent
                                    :claims (vec (sort (keys (:claims a))))
                                    :claim (sw/claim-view c now (claim-ttl-ms ctx))
                                    :claim-ttl-minutes (setting ctx :compose/claim-ttl-minutes)
                                    :renew (if (sw/wave? c)
                                             "a wave claim: it holds until :until whether or not its owner stays in the swarm; claim again with ttl_minutes to move it, release when done"
                                             "claim again (or touch) before the claim TTL runs out; release when done")}
                                   (nrepl-answer (section-routes (routes ctx) a) (and known (not taken) (not up))))
                      known (assoc :renewed {:profile sid :services (:services a)})
                      taken (assoc :adopted (cond-> {:profile sid :services (:services a)}
                                              (seq (:programs taken)) (assoc :programs (:programs taken))
                                              (seq (:not-started taken)) (assoc :programs-not-started (:not-started taken))))
                      lapsed? (assoc :wave-ended
                                     (str "your wave claim had ended (:until " (:until before)
                                          "); this renewal without ttl_minutes holds it as a plain claim, renewed within the claim TTL"))
                      up (assoc :started (dissoc (:ok up) :out)))))))))))

(defn release-claim!
  "Result: `agent` gives up its claim on section `p`. When no claim is left the
   section is orphaned: the reaper takes it after the orphan TTL unless
   something else uses it, or at once with `stop?`, which stops what no other
   section needs."
  [ctx p agent {:keys [stop?]}]
  (let [sid (:profile/id p)
        a (get-in @(:state ctx) [:active sid])]
    (cond
      (str/blank? agent) (r/err :compose/missing-param {:param "agent"})
      (nil? a) (r/err :compose/not-active {:profile sid})
      (not (contains? (:claims a) agent))
      (r/err :compose/not-claimed {:profile sid :agent agent :claims (vec (sort (keys (:claims a))))})
      :else
      (let [now ((:now ctx))
            left (dissoc (:claims a) agent)]
        (swap! (:state ctx) update-in [:active sid]
               #(if (seq left)
                  (assoc % :claims left)
                  (-> % (dissoc :claims) (assoc :orphaned-at now))))
        (persist! ctx)
        (if (and stop? (empty? left))
          (r/map-ok (ops/down-profile! ctx p :stop)
                    (fn [d] {:profile sid :agent agent :claims [] :stopped d}))
          (let [a' (get-in @(:state ctx) [:active sid])]
            (r/ok (cond-> {:profile sid :agent agent :claims (vec (sort (keys left)))}
                    (empty? left) (assoc :orphaned? true :reap-in-seconds (reap-in-seconds ctx p a'))
                    (and stop? (seq left)) (assoc :note "not stopped: other agents still claim it")))))))))

;; ---------------------------------------------------------------------------
;; reads

(defn who
  "Result<{:sections [..] :agents [..]}>: per active section its claims, what
   the last pass saw using it and when the reaper takes it; per live agent
   (when a swarm answers) the sections whose project it works in."
  [ctx]
  (let [settings (:settings ctx)
        now ((:now ctx))
        active (:active @(:state ctx))
        rs (routes ctx)
        found (agents ctx)
        sections (vec (for [[sid a] (sort-by key active)
                            :let [p (or (get-in settings [:compose/profiles sid]) (:profile a))]]
                        (cond-> {:id sid
                                 :project (:project a)
                                 :claims (sw/claims-view (:claims a) now (claim-ttl-ms ctx))
                                 :in-use? (boolean (get-in a [:usage :in-use?]))
                                 :used-by (get-in a [:usage :by] [])
                                 :in-use-via (get-in a [:usage :sources] [])
                                 :activity (vec (sort (section-activity ctx sid a)))
                                 :idle-seconds (quot (- now (:last-touch a)) 1000)
                                 :reap-in-seconds (reap-in-seconds ctx p a)}
                          (:orphaned-at a) (assoc :orphaned? true)
                          (= :none (profile/idle-action settings p)) (assoc :exempt? true))))]
    (r/ok (cond-> {:sections sections}
            found (assoc :agents
                         (vec (for [ag found]
                                (assoc ag :sections
                                       (vec (for [[sid a] (sort-by key active)
                                                  :when (some #(sw/under? % (:agent/cwd ag))
                                                              (section-dirs ctx (section-routes rs a) a))]
                                              sid))))))
            (and (swarm ctx) (nil? found)) (assoc :swarm :absent)))))

(defn nrepl
  "Result: the shared nREPL to drive `path` with (the program whose directory
   holds it most closely), or with no path every route (of `project` when
   given), each with one line of advice."
  [ctx {:keys [path project]}]
  (let [rs (with-advice (routes ctx))]
    (cond
      (seq path) (r/ok (if-let [best (sw/best-route rs path)]
                         (assoc best :path path)
                         {:path path :route nil
                          :advice (str "no configured program holds " path
                                       "; declare it under :project/programs to share its nREPL")}))
      (seq project) (r/ok {:routes (filterv #(or (= project (:project %)) (= project (:compose-project %))) rs)})
      :else (r/ok {:routes rs}))))

;; ---------------------------------------------------------------------------
;; announcements

(defn announce-up!
  "Tell the swarm section `sid` is up and which shared nREPLs it serves.
   Answers the notice result, nil when there was nothing to tell or notices
   are off."
  [ctx sid]
  (when-let [a (get-in @(:state ctx) [:active sid])]
    (let [ready (filterv :nrepl-ready? (section-routes (routes ctx) a))]
      (when (seq ready)
        (notify! ctx :section/up
                 {:message (str "compose section " sid " is up: " (str/join "; " (map sw/route-advice ready)))
                  :section sid
                  :nrepl (mapv #(select-keys % [:dir :program/id :nrepl-port]) ready)})))))

(defn announce-reaped!
  "Tell the swarm what a reaper pass released, nil when nothing was."
  [ctx reaped]
  (let [done (filterv :ok? reaped)]
    (when (seq done)
      (notify! ctx :section/reaped
               {:message (str "compose reaped idle section(s) "
                              (str/join ", " (map :profile/id done))
                              ": nobody claimed, worked in or connected to them; resources freed")
                :sections (mapv #(select-keys % [:profile/id :action :services]) done)}))))
