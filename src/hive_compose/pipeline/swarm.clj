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
     the connections newly opened to its nREPL (and, opt-in, published) ports,
     touches what is in use, marks a section whose last claim ended orphaned
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
        kinds (set (setting ctx :compose/activity))
        rs (routes ctx)
        active (:active @(:state ctx))
        seen (into (sorted-map)
                   (for [[sid a] active
                         :let [srs (section-routes rs a)]]
                     [sid {:routes srs
                           :agents (if found (sw/agents-in found (section-dirs ctx srs a)) [])
                           :ports (into (set (when (contains? kinds :nrepl)
                                               (keep #(when (:running? %) (:nrepl-port %)) srs)))
                                        (when (contains? kinds :published) (published-ports ctx a)))
                           :expired (sw/expired-claims (:claims a) live now claim-ttl)}]))
        all-ports (into #{} (mapcat :ports) (vals seen))
        peers-now (if (and (:activity ctx) (seq all-ports))
                    (try (port/-peers (:activity ctx) all-ports) (catch Throwable _probe-threw {}))
                    {})
        fresh (sw/fresh-peers (or (:peers @(:state ctx)) {}) peers-now)
        verdicts (into (sorted-map)
                       (for [[sid {:keys [agents ports expired]}] seen
                             :let [claims (apply dissoc (get-in active [sid :claims]) (keys expired))]]
                         [sid {:claims claims
                               :expired expired
                               :usage (sw/usage {:claims claims :agents agents
                                                 :fresh (select-keys fresh ports)})}]))
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

(defn claim!
  "Result: `agent` claims section `p`, bringing it up first when it is not
   active. The answer carries the section's shared nREPL routes with advice,
   so the agent drives them instead of starting its own JVM. A renewed claim
   resets its TTL; `note` says what the agent works on."
  [ctx p agent {:keys [note]}]
  (if (str/blank? agent)
    (r/err :compose/missing-param {:param "agent" :hint "the claiming agent's id"})
    (let [sid (:profile/id p)
          up (when-not (get-in @(:state ctx) [:active sid]) (ops/up-profile! ctx p))]
      (if (and up (r/err? up))
        up
        (let [now ((:now ctx))
              live (some->> (agents ctx) (into #{} (map :agent/id)))
              swarm? (when live (contains? live agent))]
          (swap! (:state ctx) update-in [:active sid]
                 (fn [a] (-> a
                             (update :claims sw/claim agent now {:note note :swarm? swarm?})
                             (assoc :last-touch now)
                             (dissoc :orphaned-at))))
          (persist! ctx)
          (let [a (get-in @(:state ctx) [:active sid])]
            (r/ok (cond-> {:profile sid
                           :agent agent
                           :claims (vec (sort (keys (:claims a))))
                           :claim-ttl-minutes (setting ctx :compose/claim-ttl-minutes)
                           :renew "claim again (or touch) before the claim TTL runs out; release when done"
                           :nrepl (with-advice (section-routes (routes ctx) a))}
                    up (assoc :started (dissoc (:ok up) :out))))))))))

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
                                 :claims (into (sorted-map)
                                               (map (fn [[k c]] [k (select-keys c [:note :since :seen])]))
                                               (:claims a))
                                 :in-use? (boolean (get-in a [:usage :in-use?]))
                                 :used-by (get-in a [:usage :by] [])
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
