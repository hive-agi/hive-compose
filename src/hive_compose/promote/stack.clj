(ns hive-compose.promote.stack
  "Pure stack planning (hc-4). A Resolution (what the capability resolver
   answers) becomes a StackPlan for one run: every instance under a compose
   project (the provider's own name when shared, `<provider>-<consumer>-<run>`
   when private), its services and docker labels; every consumer on its own
   view network `hive-<run>-<consumer>`, joined by the instances it is bound to
   under their original service names (network aliases, never an env
   rewrite); and a start order. Then: a terraform-plan style diff against what
   already runs, the image lockfile value, and a refcounted release of
   instances across stacks, generalising promote.plan to services spanning
   compose projects. Decision 20261122141106-2d375c4f. No IO here."
  (:require [clojure.string :as str]
            [hive-compose.promote.names :as names]
            [hive-compose.promote.plan :as plan]
            [hive-compose.promote.profile :as profile]
            [hive-compose.schema :as schema]
            [hive-dsl.result :as r]
            [malli.core :as m]))

;; SPDX-License-Identifier: MIT

(def default-stack-id "default")

(defn project-compose-name
  "The compose project name a Project runs under: `:project/name`, else the
   name compose derives from `:project/dir`."
  [p]
  (or (:project/name p) (profile/default-project (:project/dir p))))

(defn labels
  "Docker labels of what run `run` of stack `stack-id` creates; with
   `instance-id`, also the instance label."
  ([run stack-id]
   {(:run schema/docker-labels) run (:stack schema/docker-labels) stack-id})
  ([run stack-id instance-id]
   (assoc (labels run stack-id) (:instance schema/docker-labels) instance-id)))

(defn- binders
  "{instance-id #{consumer}}: who binds each instance."
  [bindings]
  (reduce-kv (fn [acc consumer caps]
               (reduce (fn [acc iid] (update acc iid (fnil conj (sorted-set)) consumer))
                       acc (vals caps)))
             {} bindings))

(defn- resolution-problems
  "Inconsistencies between bindings and instances, as [{:consumer :capability
   :instance :problem}]: an unknown instance, a capability mismatch, a private
   instance bound by someone other than its one consumer, and an instance
   nobody binds ({:instance :problem :unbound-instance})."
  [{:resolution/keys [instances bindings]}]
  (let [bound (set (mapcat vals (vals bindings)))]
    (into
     (vec (for [[consumer caps] (sort-by key bindings)
                [cap iid] (sort-by key caps)
                :let [i (get instances iid)
                      problem (cond
                                (nil? i) :unknown-instance
                                (not= cap (:instance/capability i)) :capability-mismatch
                                (and (not (:instance/share? i))
                                     (not= [consumer] (:instance/consumers i))) :private-instance-shared)]
                :when problem]
            {:consumer consumer :capability cap :instance iid :problem problem}))
     (for [iid (sort (keys instances)) :when (not (bound iid))]
       {:instance iid :problem :unbound-instance}))))

(defn- provider-compose-name
  "The compose project name of `provider`: its Project's, else its slug (an
   unknown provider is refused later, by instance-project)."
  [projects provider]
  (if-let [p (get projects provider)]
    (project-compose-name p)
    (names/slug provider)))

(defn- fill-private-projects
  "`resolution` with every private instance under its compose project for
   `run`: `<provider compose name>-<slug consumer>-<run>`. Always recomputed,
   so a resolver that leaves it out (or set it for another run) is planned
   right."
  [projects run resolution]
  (update resolution :resolution/instances
          (fn [is]
            (into {} (map (fn [[iid i]]
                            [iid (if (:instance/share? i)
                                   i
                                   (assoc i :instance/compose-project
                                          (names/private-project (provider-compose-name projects (:instance/provider i))
                                                                 (first (:instance/consumers i)) run)))]))
                  is))))

(defn- instance-project
  "Result<compose project name> of instance `i`: the provider's compose
   project name when shared (as `up` of that project runs it), the filled
   private name otherwise. The provider must be a configured project."
  [projects i]
  (let [provider (:instance/provider i)]
    (cond
      (not (contains? projects provider))
      (r/err :compose/unknown-provider {:instance (:instance/id i) :provider provider
                                        :projects (vec (sort (keys projects)))})
      (:instance/share? i) (r/ok (project-compose-name (get projects provider)))
      :else (r/ok (:instance/compose-project i)))))

(defn- instance-deps
  "Instances other than `iid` that `i`'s provider project is bound to: they
   must run before it."
  [bindings iid i]
  (vec (sort (disj (set (vals (get bindings (:instance/provider i)))) iid))))

(defn topo-order
  "Result<[node]>: `deps` ({node [node]}, every dep a key) in dependency order,
   ties broken by sort order so the answer is deterministic. A cycle is an
   error naming the nodes on it."
  [deps]
  (loop [order [] done #{} left (into (sorted-map) deps)]
    (if (empty? left)
      (r/ok order)
      (let [ready (filter (fn [[_ ds]] (every? done ds)) left)]
        (if (empty? ready)
          (r/err :compose/dependency-cycle {:instances (vec (keys left))})
          (let [n (key (first ready))]
            (recur (conj order n) (conj done n) (dissoc left n))))))))

(defn- collisions
  "{name [ids]} where several ids map to one name."
  [id->name]
  (into (sorted-map)
        (comp (filter (fn [[_ ids]] (< 1 (count ids))))
              (map (fn [[n ids]] [n (vec (sort ids))])))
        (group-by id->name (keys id->name))))

(defn- clashing-aliases
  "[{:consumer :alias :instances}]: one alias answered by two instances on one
   consumer's view network, which would make Docker DNS ambiguous."
  [consumers]
  (vec (for [[c cp] (sort-by key consumers)
             :let [by-alias (reduce (fn [acc {:keys [instance aliases]}]
                                      (reduce #(update %1 %2 (fnil conj (sorted-set)) instance) acc aliases))
                                    {} (:consumer/attach cp))]
             [a ids] (sort-by key by-alias)
             :when (< 1 (count ids))]
         {:consumer c :alias a :instances (vec ids)})))

(defn instance-labels
  "Labels on an instance's containers. A shared instance carries only
   hive.instance: a per-run label on a service shared across runs would change
   its compose config hash and recreate it on every new run's up, so per-run
   ownership of a shared instance lives on the view networks (`labels`). A
   private one carries hive.run, hive.stack, hive.instance and hive.consumer."
  [run stack-id iid i]
  (if (:instance/share? i)
    {(:instance schema/docker-labels) iid}
    (assoc (labels run stack-id iid) (:consumer schema/docker-labels) (first (:instance/consumers i)))))

(defn- shadowed-services
  "[{:consumer :alias :instance}]: an instance alias on a consumer's view
   network equal to one of that consumer's own service names
   (`services` {consumer [service]}); Docker DNS would answer either."
  [consumers services]
  (vec (for [[c cp] (sort-by key consumers)
             :let [own (set (get services c))]
             {:keys [instance aliases]} (:consumer/attach cp)
             a aliases
             :when (own a)]
         {:consumer c :alias a :instance instance})))

(defn- plan-instance [projects run stack-id bindings by iid i]
  (r/map-ok (instance-project projects i)
            (fn [cp]
              (assoc i
                     :instance/compose-project cp
                     :instance/services [(:instance/service i)]
                     :instance/labels (instance-labels run stack-id iid i)
                     ;; every binder's view network, and its provider's own when the
                     ;; provider binds instances itself, so it reaches them there
                     :instance/networks (vec (sort (distinct (map #(names/view-network run %)
                                                                  (cond-> (get by iid)
                                                                    (contains? bindings (:instance/provider i))
                                                                    (conj (:instance/provider i)))))))
                     :instance/depends-on (instance-deps bindings iid i)))))

(defn- plan-consumer [run stack-id instances consumer caps]
  {:consumer/id consumer
   :consumer/network (names/view-network run consumer)
   :consumer/labels (labels run stack-id)
   :consumer/attach (vec (for [iid (sort (distinct (vals caps)))]
                           {:instance iid :aliases (:instance/aliases (get instances iid))}))
   :consumer/bindings (into (sorted-map) caps)
   :consumer/provider-of (vec (sort (keep (fn [[iid ip]] (when (= consumer (:instance/provider ip)) iid)) instances)))})

(defn stack-plan
  "Result<StackPlan> for `resolution` in run `run`. `settings` gives the
   provider projects (`:compose/projects`); `opts` {:stack id} names the stack
   (default \"default\"). Errors: :compose/invalid-run, :compose/invalid-resolution,
   :compose/inconsistent-resolution (a binding to an unknown instance, a
   capability mismatch, a private instance bound by another consumer),
   :compose/unknown-provider, :compose/name-collision (two consumers slugging
   to one view network, two private instances to one compose project),
   :compose/alias-clash (two instances answering one alias on one consumer's
   network) and :compose/alias-shadows-service (an instance alias equal to one
   of the consumer's own service names, given in `opts` as
   {:services {consumer [service]}}, which Docker DNS would shadow) and
   :compose/dependency-cycle. A private instance's compose project is always
   computed here (`<provider compose name>-<slug consumer>-<run>`), whatever
   the resolution carried."
  [settings resolution run opts]
  (let [stack-id (or (:stack opts) default-stack-id)
        projects (:compose/projects settings)
        input resolution
        ;; fill only a well-formed input: a malformed one is answered by the
        ;; ResolvedResolution check below, never by an exception in the fill
        resolution (if (and (m/validate schema/ResolvedResolution input)
                            (m/validate schema/RunId run))
                     (fill-private-projects projects run resolution)
                     resolution)
        {:resolution/keys [instances bindings]} resolution]
    (cond
      (not (m/validate schema/RunId run))
      (r/err :compose/invalid-run {:run run :problems (schema/explain schema/RunId run)})

      (not (m/validate schema/StackId stack-id))
      (r/err :compose/invalid-stack {:stack stack-id :problems (schema/explain schema/StackId stack-id)})

      ;; input: run-free (the resolver leaves private compose projects out);
      ;; after fill-private-projects every instance must be a strict Instance
      (not (m/validate schema/ResolvedResolution input))
      (r/err :compose/invalid-resolution {:problems (schema/explain schema/ResolvedResolution input)})

      (not (m/validate schema/Resolution resolution))
      (r/err :compose/invalid-resolution {:problems (schema/explain schema/Resolution resolution)})

      (seq (resolution-problems resolution))
      (r/err :compose/inconsistent-resolution {:problems (resolution-problems resolution)})

      :else
      (let [by (binders bindings)
            planned (reduce-kv (fn [acc iid i]
                                 (let [x (plan-instance projects run stack-id bindings by iid i)]
                                   (if (r/err? x) (reduced x) (assoc acc iid (:ok x)))))
                               {} instances)]
        ;; reduce into a plain map: r/err? on a string-keyed sorted-map throws
        (if (r/err? planned)
          planned
          (let [planned (into (sorted-map) planned)
                consumers (into (sorted-map)
                                (map (fn [[c caps]] [c (plan-consumer run stack-id planned c caps)]))
                                bindings)
                net-clash (collisions (into {} (map (fn [[c cp]] [c (:consumer/network cp)])) consumers))
                proj-clash (collisions (into {} (comp (remove (comp :instance/share? val))
                                                      (map (fn [[_ ip]] [[(:instance/provider ip) (first (:instance/consumers ip))]
                                                                         (:instance/compose-project ip)])))
                                             planned))
                aliases (clashing-aliases consumers)
                shadows (shadowed-services consumers (:services opts))]
            (cond
              (or (seq net-clash) (seq proj-clash))
              (r/err :compose/name-collision {:networks net-clash :compose-projects proj-clash})

              (seq aliases)
              (r/err :compose/alias-clash {:clashes aliases})

              (seq shadows)
              (r/err :compose/alias-shadows-service {:shadows shadows})

              :else
              (r/map-ok (topo-order (into {} (map (fn [[iid ip]] [iid (:instance/depends-on ip)])) planned))
                        (fn [order]
                          {:stack/id stack-id
                           :stack/run run
                           :stack/instances planned
                           :stack/consumers consumers
                           :stack/networks (vec (sort (distinct (map :consumer/network (vals consumers)))))
                           :stack/start-order (into (mapv (fn [iid] {:step/kind :instance :step/id iid}) order)
                                                    (map (fn [c] {:step/kind :consumer :step/id c}))
                                                    (keys consumers))})))))))))

;; ---------------------------------------------------------------------------
;; plan diff: what is shared, private, running and to start

(defn- service-key [project service] {:project project :service service})

(defn plan-diff
  "Terraform-plan style summary of `sp` against `running`, a set of
   {:project <compose project> :service <service>} already up. Per instance in
   start order: :mode :shared or :private, :state :running (every service up),
   :to-start (none) or :partial, and which services are up and which start.
   `:summary` counts them."
  [sp running]
  (let [running (set running)
        rows (vec (for [{:step/keys [kind id]} (:stack/start-order sp)
                        :when (= :instance kind)
                        :let [ip (get-in sp [:stack/instances id])
                              cp (:instance/compose-project ip)
                              svcs (vec (distinct (concat (:instance/services ip) (:instance/needs ip))))
                              up (filterv #(running (service-key cp %)) svcs)
                              start (filterv #(not (running (service-key cp %))) svcs)]]
                    {:instance id
                     :capability (:instance/capability ip)
                     :mode (if (:instance/share? ip) :shared :private)
                     :compose-project cp
                     :state (cond (empty? start) :running (empty? up) :to-start :else :partial)
                     :running up
                     :to-start start
                     :consumers (:instance/consumers ip)}))]
    {:stack (:stack/id sp)
     :run (:stack/run sp)
     :instances rows
     :consumers (vec (for [[c cp] (:stack/consumers sp)]
                       {:consumer c :network (:consumer/network cp)
                        :attach (:consumer/attach cp)}))
     :summary {:instances (count rows)
               :shared (count (filter #(= :shared (:mode %)) rows))
               :private (count (filter #(= :private (:mode %)) rows))
               :running (count (filter #(= :running (:state %)) rows))
               :partial (count (filter #(= :partial (:state %)) rows))
               :to-start (count (filter #(= :to-start (:state %)) rows))}}))

(def ^:private state-marks {:running "=" :to-start "+" :partial "~"})

(defn- pad [s n] (let [s (str s)] (str s (apply str (repeat (max 1 (- n (count s))) " ")))))

(defn render-diff
  "`diff` (plan-diff) as printable text: one line per instance (= running,
   + to start, ~ partly running), then each consumer's view network and the
   instances joining it."
  [{:keys [stack run instances consumers summary]}]
  (let [w (inc (reduce max 8 (map (comp count :instance) instances)))
        pw (inc (reduce max 8 (map (comp count :compose-project) instances)))]
    (str/join
     "\n"
     (concat
      [(str "Stack " stack ", run " run ": " (:instances summary) " instances ("
            (:shared summary) " shared, " (:private summary) " private), "
            (:running summary) " running, " (:partial summary) " partial, "
            (:to-start summary) " to start.")]
      (for [{:keys [instance mode compose-project state running to-start]} instances]
        (str "  " (state-marks state) " " (pad instance w) (pad (name mode) 8) (pad compose-project pw)
             (pad (name state) 9)
             (str/join " " (concat (map #(str "=" %) running) (map #(str "+" %) to-start)))))
      (when (seq consumers) ["Consumers:"])
      (for [{:keys [consumer network attach]} consumers]
        (str "  " consumer " -> " network
             (when (seq attach)
               (str " <- " (str/join ", " (map (fn [{:keys [instance aliases]}]
                                                  (str instance " as " (str/join "," aliases)))
                                                attach))))))))))

;; ---------------------------------------------------------------------------
;; lockfile

(defn lock
  "The lockfile value of `sp`: {instance-id {:image :digest}} from `digests`,
   {[compose-project service] {:image :digest}} as the engine reports it, read
   at each instance's own service. Instances with no reported digest are left
   out."
  [sp digests]
  (into (sorted-map)
        (for [[iid ip] (:stack/instances sp)
              :let [d (get digests [(:instance/compose-project ip) (:instance/service ip)])]
              :when (and (:image d) (:digest d))]
          [iid (select-keys d [:image :digest])])))

(defn lock-path
  "Where the lockfile of stack `stack-id` lives: `locks/<stack>.lock.edn`
   beside the state file `state-file`."
  [state-file stack-id]
  (let [dir (or (second (re-find #"^(.*)/[^/]*$" (str state-file))) ".")]
    (str dir "/locks/" stack-id ".lock.edn")))

;; ---------------------------------------------------------------------------
;; release across stacks: a refcount over instances spanning compose projects

(defn instance-footprint
  "What instance plan `ip` keeps alive, as #{[compose-project service]}: its
   services and the depends_on closure they need (`:instance/needs`)."
  [ip]
  (let [cp (:instance/compose-project ip)]
    (into #{} (map #(vector cp %)) (concat (:instance/services ip) (:instance/needs ip)))))

(defn holders
  "{[compose-project service] [holder]}: every service live stacks other than
   instance `instance-id` of run `run` keep alive, with who keeps it: [run
   instance-id] for a stack instance, [:section profile-id] for an active
   section (promote.plan's active map) of that compose project."
  ([active-stacks run instance-id] (holders active-stacks run instance-id {}))
  ([active-stacks run instance-id active-sections]
   (let [pairs (concat
                (for [[r sp] active-stacks
                      [iid ip] (:stack/instances sp)
                      :when (not (and (= r run) (= iid instance-id)))
                      k (instance-footprint ip)]
                  [k [r iid]])
                (for [[pid a] active-sections
                      s (plan/footprint a)]
                  [[(:project a) s] [:section pid]]))]
     (update-vals (reduce (fn [acc [k h]] (update acc k (fnil conj #{}) h)) {} pairs)
                  #(vec (sort-by pr-str %))))))

(defn instance-release
  "The release step for instance `instance-id` of run `run` among
   `active-stacks` ({run StackPlan}, the run itself included): the services of
   its footprint that no other live stack instance (and, given
   `active-sections`, no active section) keeps alive. An instance another live
   stack still binds releases nothing. `:kept` names, per spared service, who
   keeps it. nil when the run has no such instance."
  ([active-stacks run instance-id] (instance-release active-stacks run instance-id {}))
  ([active-stacks run instance-id active-sections]
   (when-let [ip (get-in active-stacks [run :stack/instances instance-id])]
     (let [held (holders active-stacks run instance-id active-sections)
           fp (sort (instance-footprint ip))
           released (vec (for [[_ s :as k] fp :when (not (contains? held k))] s))
           kept (into (sorted-map) (for [[_ s :as k] fp :when (contains? held k)] [s (vec (get held k))]))]
       (cond-> {:run run
                :instance instance-id
                :project (:instance/compose-project ip)
                :services released}
         (seq kept) (assoc :kept kept))))))

(defn stack-releases
  "Release steps for taking run `run` down, instances in reverse start order
   against a shrinking set of live stacks, so a service two instances of the
   run share goes with the last of them and whatever another stack or
   section keeps stays."
  ([active-stacks run] (stack-releases active-stacks run {}))
  ([active-stacks run active-sections]
   (let [sp (get active-stacks run)
         order (reverse (keep (fn [{:step/keys [kind id]}] (when (= :instance kind) id))
                              (:stack/start-order sp)))]
     (first
      (reduce (fn [[steps live] iid]
                [(conj steps (instance-release live run iid active-sections))
                 (update-in live [run :stack/instances] dissoc iid)])
              [[] active-stacks]
              order)))))

(defn with-needs
  "`sp` with each instance's `:instance/needs` filled from `closures`
   ({[compose-project service] [service]}, the engine's depends_on closure of
   the instance's service), so plan-diff and the refcount cover dependencies
   too. hc-6 calls it before it persists the plan."
  [sp closures]
  (update sp :stack/instances
          (fn [is]
            (into (sorted-map)
                  (map (fn [[iid ip]]
                         [iid (if-let [c (get closures [(:instance/compose-project ip) (:instance/service ip)])]
                                (assoc ip :instance/needs (vec (sort (distinct c))))
                                ip)]))
                  is))))

(defn stack-held
  "{compose-project #{service}} every live stack instance keeps alive: what a
   section release (promote.plan, which cannot see stacks) must also spare."
  [active-stacks]
  (reduce (fn [acc [cp s]] (update acc cp (fnil conj #{}) s))
          {}
          (for [[_ sp] active-stacks ip (vals (:stack/instances sp)) k (instance-footprint ip)] k)))
