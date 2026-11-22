(ns hive-compose.pipeline.stacks
  "Stack runs over a context (the one `hive-compose.pipeline.ops` takes; its
   engine must satisfy `hive-compose.port/IStackEngine`):

     stack-up!     resolve (hc-3) -> plan (hc-4) -> wiring (hc-5) -> view
                   networks -> instances in start order, each `up --wait`ed
                   and health-gated -> each instance joins its consumers'
                   view networks under its aliases -> consumers, each only
                   once every instance it binds is healthy, on its view
                   network alone, with its wiring env (secrets read at spawn).
     stack-down!   consumers, then the run's view networks (which detaches
                   shared instances from them), then instances in reverse
                   start order releasing only what no other live run, active
                   section, or the world before this run keeps (hc-4
                   instance-release). Never volumes.
     stack-status  what each run's containers are doing now.
     plan          hc-4 plan-diff of a stack against what runs.
     reap!         takes down idle, unclaimed runs, as the reaper does sections.
     claim!/release-claim!  agents hold a run (`target=stack:<run>`).

   Runs persist under `:stacks {run entry}` in the state, beside `:active`.
   An entry: {:stack/id :run :started-at :last-touch :plan StackPlan
   :networks [name] :consumers {consumer {:compose-project :services}}
   :status :up|:partial :claims {agent Claim} :orphaned-at ms}. What ran
   before the runs is global, not per run: `:preexisting {provider-project
   #{service}}` beside `:stacks` (`run/mark-preexisting`,
   `run/reconcile-preexisting`).

   stack-up!, stack-down!, reap!, claim! and release-claim! are serialized
   on one lock per state: the monitor of the context's state atom (a
   reloaded context shares the atom, so it shares the lock). Each holds it
   across its plan, its engine IO and its state writes, so no down plans a
   release an up then relies on, and no probe reads a world another stack
   op is changing. Reads (stack-status, plan) take no lock."
  (:require [clojure.string :as str]
            [hive-compose.port :as port]
            [hive-compose.promote.capability :as capability]
            [hive-compose.promote.plan :as plan]
            [hive-compose.promote.resources :as resources]
            [hive-compose.promote.stack :as stack]
            [hive-compose.promote.stack-run :as run]
            [hive-compose.promote.target :as target]
            [hive-compose.promote.wiring :as wiring]
            [hive-compose.schema :as schema]
            [hive-dsl.result :as r]
            [malli.core :as m])
  (:import (java.security SecureRandom)))

;; SPDX-License-Identifier: MIT

(def claim-prefix "stack:")

(defn- persist! [{:keys [state save!]}]
  (when save! (save! (select-keys @state [:active :current :stacks :preexisting]))))

(defn- now [ctx] ((:now ctx)))

(defn- stacks [ctx] (:stacks @(:state ctx) {}))

(defn- plans-of
  "{run StackPlan} of every run recorded in state value `st`."
  [st]
  (into {} (map (fn [[k e]] [k (:plan e)])) (:stacks st)))

(defn- live-plans
  "{run StackPlan} of every recorded run."
  [ctx]
  (plans-of @(:state ctx)))

(defmacro ^:private serialized
  "`body` holding the stack lock of `ctx`: the monitor of its state atom, one
   per state, reentrant (reap! and release-claim! call stack-down!)."
  [ctx & body]
  `(locking (:state ~ctx) ~@body))

(defn- random-hex []
  (let [b (byte-array 3)]
    (.nextBytes (SecureRandom.) b)
    (apply str (map #(format "%02x" (bit-and % 0xff)) b))))

(defn- stack-engine
  "Result<engine> when the context's engine can run stacks."
  [ctx]
  (let [e (:engine ctx)]
    (if (satisfies? port/IStackEngine e)
      (r/ok e)
      (r/err :compose/stacks-unsupported {:hint "the engine has no IStackEngine (networks, labelled projects)"}))))

(defn- all-ok
  "Result<[v]>: `f` over `xs` in order, stopping at the first error."
  [f xs]
  (reduce (fn [acc x] (let [y (f x)] (if (r/err? y) (reduced y) (r/map-ok acc #(conj % (:ok y))))))
          (r/ok []) xs))

(defn- stack-config [ctx stack-id]
  (if-let [s (get-in ctx [:settings :compose/stacks stack-id])]
    (r/ok s)
    (r/err :compose/unknown-stack {:stack stack-id
                                   :known (vec (sort (keys (get-in ctx [:settings :compose/stacks]))))})))

(defn- project-of [ctx project-id]
  (get-in ctx [:settings :compose/projects project-id]))

;; ---------------------------------------------------------------------------
;; health

(defn project-health
  "Result<{service {:state :health}}> of the containers of compose project
   `cp`, found by compose's own label (no compose file loaded)."
  [engine cp]
  (r/map-ok (port/-running-labelled engine {"com.docker.compose.project" cp})
            (fn [rows] (into (sorted-map) (map (fn [c] [(:service c) (select-keys c [:state :health])])) rows))))

(defn- gate
  "Result<ip>: ok when every service of instance plan `ip` is ready now, else
   :compose/instance-not-ready naming the instance (and `consumer`, waiting)."
  [engine ip consumer]
  (r/let-ok [h (project-health engine (:instance/compose-project ip))]
    (let [bad (run/not-ready h (:instance/services ip))]
      (if (empty? bad)
        (r/ok ip)
        (r/err :compose/instance-not-ready
               (cond-> {:instance (:instance/id ip) :compose-project (:instance/compose-project ip)
                        :not-ready bad}
                 consumer (assoc :consumer consumer)))))))

;; ---------------------------------------------------------------------------
;; planning against the engine

(defn- with-needs
  "Result<StackPlan> with every instance's depends_on closure
   (`:instance/needs`, hc-4 `stack/with-needs`), read from its provider
   project's own files, whatever -p the instance runs under."
  [engine settings sp]
  (r/map-ok
   (all-ok (fn [[_ ip]]
             (let [proj (get-in settings [:compose/projects (:instance/provider ip)])
                   p (assoc (target/project-profile proj) :profile/services [(:instance/service ip)])]
               (r/map-ok (port/-closure engine p)
                         (fn [c] [[(:instance/compose-project ip) (:instance/service ip)] (vec c)]))))
           (:stack/instances sp))
   (fn [rows] (stack/with-needs sp (into {} rows)))))

(defn- running-footprints
  "Result<{compose-project #{service}}>: what of each shared instance's
   footprint runs now, by the compose project it runs under (its provider's).
   Only the probe; what counts as preexisting is decided in
   `run/mark-preexisting`, against the state current at the swap."
  [engine settings sp]
  (r/map-ok
   (all-ok (fn [[_ ip]]
             (if (:instance/share? ip)
               (let [proj (get-in settings [:compose/projects (:instance/provider ip)])]
                 (r/map-ok (port/-running engine (target/project-profile proj))
                           (fn [up] [(:instance/compose-project ip)
                                     (into (sorted-set) (filter (set up))
                                           (map second (stack/instance-footprint ip)))])))
               (r/ok nil)))
           (:stack/instances sp))
   #(apply merge-with into {} (keep (fn [[k v]] (when (seq v) {k v})) %))))

(defn- section-held
  "{compose-project #{service}} the active sections hold (promote.plan footprint)."
  [ctx]
  (reduce (fn [m [_ a]] (update m (:project a) (fnil into #{}) (plan/footprint a)))
          {} (:active @(:state ctx))))

(defn resolve-plan
  "Result<{:resolution :plan :wirings :budget}> of stack `stack-id` for run
   `run-id`: resolution (hc-3), StackPlan (hc-4), Wirings (hc-5) and the
   capped-memory Budget (hc-16a). Pure, no engine."
  [ctx stack-id run-id]
  (let [settings (:settings ctx)
        projects (:compose/projects settings)]
    (r/let-ok [s (stack-config ctx stack-id)
               ;; a stack's own needs (otel, ...) are needs of every container
               ;; consumer in it, shared: one instance joins each view network.
               ;; Built as capability/resolve-stack builds it, so the budget
               ;; (hc-16a) sees the same Need :resources.
               needs (capability/consumer-needs projects (:stack/consumers s)
                                                (cond-> (run/stack-needs-per-consumer s)
                                                  (seq (:stack/needs s))
                                                  (update (capability/stack-consumer s) merge (:stack/needs s))))
               res (capability/resolve-needs projects needs)
               sp (stack/stack-plan settings res run-id {:stack stack-id})
               ws (wiring/wirings settings res run-id (run/otlp-envs res run-id))]
      (r/ok {:resolution res :plan sp :wirings ws
             :budget (resources/budget settings s needs res)}))))

;; ---------------------------------------------------------------------------
;; resource caps (hc-16a)

(defn- mem-available
  "MemAvailable in bytes from the context's IHostMemory, nil when there is
   none or it cannot be read (admission then checks the budgets alone)."
  [ctx]
  (when-let [m (:host-memory ctx)]
    (when (satisfies? port/IHostMemory m)
      (let [x (port/-mem-available m)] (when (r/ok? x) (:ok x))))))

(defn- running-instances
  "Ids of the shared instances of `sp` already up: held by another live run,
   or whose answering service runs now in its compose project (`running`,
   {compose-project #{service}} from `running-footprints`). They take no new
   host memory."
  [ctx sp running]
  (let [held (set (for [[_ other] (live-plans ctx)
                        [iid ip] (:stack/instances other)
                        :when (:instance/share? ip)]
                    iid))]
    (set (for [[iid ip] (:stack/instances sp)
               :when (:instance/share? ip)
               :when (or (held iid)
                         (contains? (get running (:instance/compose-project ip) #{})
                                    (:instance/service ip)))]
           iid))))

(defn others-mem
  "What :compose/budget is already spent on besides `budget` (this stack's):
   the capped memory (closures included) of every live run's instances.

   A SHARED instance is one container whoever binds it, so it is keyed by
   its id, counted once at the largest cap any run gives it, and for one
   this stack binds too only what the live runs give it beyond this stack's
   own count. A PRIVATE instance is a container per run: its id
   (`<provider>/<cap>@<consumer>`) carries no run, so it is keyed by its
   compose project and always adds; two runs of one stack sum, they never
   collapse into one. A run recorded before runs carried :budget counts 0."
  [ctx budget]
  (let [mine   (:budget/instances budget)
        key-of (fn [iid ip] (if (:instance/share? ip) iid [:private (:instance/compose-project ip) iid]))
        live   (reduce (fn [m [k c]] (update m k (fnil max 0) (resources/inst-mem c)))
                       {}
                       (for [[_ e] (stacks ctx)
                             [iid ip] (get-in e [:plan :stack/instances])]
                         [(key-of iid ip) (get-in e [:budget :budget/instances iid] {})]))]
    (reduce + 0 (for [[k m] live]
                  (if-let [c (when (string? k) (get mine k))]
                    (max 0 (- m (resources/inst-mem c)))
                    m)))))

(defn admit
  "Result<Budget>: `budget` admitted against :stack/budget, :compose/budget
   (host-wide: every live run's capped memory plus this stack's) and the
   host's MemAvailable minus :compose/host-reserve, the instances in
   `running` (ids) counting as already paid; else :compose/over-budget."
  [ctx stack-id budget running]
  (resources/admit budget
                   (resources/limits (:settings ctx)
                                     (get-in ctx [:settings :compose/stacks stack-id])
                                     (mem-available ctx))
                   running
                   (others-mem ctx budget)))

(defn- service-facts
  "{service {:image :java-tool-options :mem-limit}} of `services` of the
   provider of `ip`, {} when the engine cannot tell."
  [ctx engine ip services]
  (if (and (seq services) (satisfies? port/IServiceImages engine))
    (let [proj (project-of ctx (:instance/provider ip))
          f (port/-service-facts engine (target/project-profile proj) (vec services))]
      (if (r/ok? f) (:ok f) {}))
    {}))

(defn- private-facts
  "{instance-id {service facts}} of every private instance's closure."
  [ctx engine sp]
  (into {} (for [[iid ip] (:stack/instances sp)
                 :when (not (:instance/share? ip))]
             [iid (service-facts ctx engine ip (distinct (concat (:instance/services ip) (:instance/needs ip))))])))

(defn- instance-caps-overrides
  "{service override} capping planned instance `ip`: a PRIVATE instance gets
   its caps and JVM flag in its stack override; a shared one runs from its
   provider's own files with no override (hc-6), so its caps are summed and
   admitted but not written (writing them would recreate it under the
   consumers already using it)."
  [ctx engine budget ip]
  (if (:instance/share? ip)
    {}
    (let [caps (get-in budget [:budget/instances (:instance/id ip)] {})
          facts (service-facts ctx engine ip (distinct (concat (:instance/services ip) (:instance/needs ip))))]
      (run/instance-overrides (:settings ctx) ip caps facts))))

(defn- consumer-services
  "Result<[service]>: what consumer `c` runs: the services of its section
   target, else every service its project enables, closed over depends_on."
  [engine proj c]
  (let [[_ tgt] (str/split c #"/" 2)
        whole (target/project-profile proj)]
    (r/let-ok [names (if tgt
                       (r/ok (:names (target/parse tgt)))
                       (port/-listed engine whole))]
      (port/-closure engine (assoc whole :profile/services (vec names))))))

;; ---------------------------------------------------------------------------
;; up

(defn- wait-timeout-s [ctx]
  (some-> (get-in ctx [:settings :compose/up-timeout-ms]) (quot 1000) (max 1)))

(defn- record! [ctx run-id f]
  (swap! (:state ctx) update-in [:stacks run-id] f)
  (persist! ctx))

(defn- up-instance!
  "Result<row>: instance `ip` up (waited and gated when `wait?`) and joined
   to each of its consumers' view networks under its aliases."
  [ctx engine ip wait? budget]
  (let [spec (cond-> (run/instance-spec (:settings ctx) ip wait?)
               (and wait? (wait-timeout-s ctx)) (assoc :wait-timeout-s (wait-timeout-s ctx))
               true (run/with-overrides (instance-caps-overrides ctx engine budget ip)))
        iid (:instance/id ip)]
    (r/let-ok [_ (let [u (port/-up-project! engine spec)]
                   (if (r/err? u)
                     (r/err :compose/instance-unhealthy
                            {:instance iid :compose-project (:project spec)
                             :cause (select-keys u [:error :exit :stderr])})
                     u))
               _ (if wait? (gate engine ip nil) (r/ok ip))
               _ (all-ok (fn [net]
                           (port/-network-connect! engine net
                                                   {:project (:instance/compose-project ip)
                                                    :service (:instance/service ip)}
                                                   (:instance/aliases ip)))
                         (:instance/networks ip))]
      (r/ok {:id iid :compose-project (:instance/compose-project ip)
             :shared? (boolean (:instance/share? ip)) :services (:instance/services ip)
             :networks (:instance/networks ip)}))))

(defn- injected-vars
  "The variables of `w`'s env the stack adds (not the consumer's own env, but
   always HIVE_RUN_ID and the resource attributes): passed into containers."
  [settings consumer w]
  (let [own (set (keys (wiring/consumer-env settings consumer)))]
    (-> (into (sorted-set) (remove own) (keys (:wiring/env w)))
        (conj wiring/run-id-var wiring/resource-attributes-var))))

(defn- up-consumer!
  "Result<row>: consumer `c` up once every instance it binds is ready."
  [ctx engine sp ws c wait?]
  (let [settings (:settings ctx)
        cp (get-in sp [:stack/consumers c])
        w (get ws c)
        proj (project-of ctx (run/consumer-project-id c))]
    (r/let-ok [_ (if wait?
                   (all-ok #(gate engine (get-in sp [:stack/instances (:instance %)]) c) (:consumer/attach cp))
                   (r/ok nil))
               all (consumer-services engine proj c)
               ;; hc-4 :consumer/provider-of: what this consumer provides runs
               ;; as its instance (shared or private), never again as the consumer
               services (r/ok (run/without-provided all (map #(get-in sp [:stack/instances %])
                                                            (:consumer/provider-of cp))))
               _ (port/-up-project! engine (run/consumer-spec settings (:stack/run sp) (:stack/id sp) c services w
                                                              (injected-vars settings c w) wait?))]
      (r/ok {:consumer c :network (:wiring/network w)
             :compose-project (run/consumer-compose-project settings c (:stack/run sp))
             :services services}))))

(defn- pick-run-id
  "Result<rid>: `run-id`, else a fresh `<stack><6 hex>`, when it is a RunId.
   Whether it is free is decided only by `claim-run!`, inside its swap."
  [ctx stack-id run-id]
  (let [rid (or run-id (run/run-id stack-id ((or (:rand-hex ctx) random-hex))))]
    (if (m/validate schema/RunId rid)
      (r/ok rid)
      (r/err :compose/invalid-run {:run rid :problems (schema/explain schema/RunId rid)}))))

(defn- run-exists [rid]
  (r/err :compose/run-exists {:run rid :hint "stack-down run=<id> first, or omit run for a fresh id"}))

(defn- probed-footprints
  "#{[compose-project service]}: the footprints of `sp`'s shared instances,
   what `running-footprints` probed."
  [sp]
  (into #{} (comp (map val) (filter :instance/share?) (mapcat stack/instance-footprint))
        (:stack/instances sp)))

(defn- claim-run!
  "Result<rid>: record run `rid` (`entry`) in ONE swap that first checks the
   id is free (else :compose/run-exists, nothing changed), then reconciles
   the preexistence marks against this run's probe (`running` of the
   `probed` footprints): a mark the probe disproves is dropped
   (`run/reconcile-preexisting`), a world service found running is marked
   (`run/mark-preexisting`), both against the runs recorded at that moment."
  [ctx rid entry running probed held]
  (let [[old _] (swap-vals! (:state ctx)
                            (fn [st]
                              (if (contains? (:stacks st) rid)
                                st
                                (let [runs (plans-of st)]
                                  (-> st
                                      (update :preexisting run/reconcile-preexisting probed running runs rid)
                                      (update :preexisting run/mark-preexisting running runs rid held)
                                      (assoc-in [:stacks rid] entry))))))]
    (if (contains? (:stacks old) rid)
      (run-exists rid)
      (do (persist! ctx) (r/ok rid)))))

(defn stack-up!
  "Result<{:run :stack :instances [..] :consumers [..] :networks [..]}>: run
   stack `stack` (`opts` {:stack id :run id? :wait? (default true)}). The run
   is recorded before anything starts, so a failure midway answers an error
   carrying `:run` that stack-down! releases. Holds the stack lock
   throughout; a run id already recorded answers :compose/run-exists."
  [ctx {stack-id :stack run-id :run wait? :wait? :or {wait? true}}]
  (serialized ctx
    (r/let-ok [engine (stack-engine ctx)
               rid (pick-run-id ctx stack-id run-id)
               {sp0 :plan ws :wirings budget0 :budget} (resolve-plan ctx stack-id rid)
               sp (with-needs engine (:settings ctx) sp0)
               budget (r/ok (resources/with-closure budget0 (:settings ctx) sp (private-facts ctx engine sp)))
               running (running-footprints engine (:settings ctx) sp)
               ;; hc-16a: refused before anything is recorded or started; under
               ;; the stack lock, so no other up is admitted against the same room
               _ (admit ctx stack-id budget (running-instances ctx sp running))
               t (r/ok (now ctx))
               _ (claim-run! ctx rid {:stack/id stack-id :run rid :started-at t :last-touch t
                                      :plan sp :budget budget :networks (:stack/networks sp)
                                      :consumers {} :status :partial}
                             running (probed-footprints sp) (section-held ctx))]
      (let [consumers (vec (filter run/container-consumer? (keys (:stack/consumers sp))))
            res (r/let-ok [nets (all-ok #(port/-network-create! engine % (stack/labels rid stack-id))
                                        (:stack/networks sp))
                           instances (all-ok (fn [{:step/keys [id]}]
                                               (up-instance! ctx engine (get-in sp [:stack/instances id]) wait? budget))
                                             (filter #(= :instance (:step/kind %)) (:stack/start-order sp)))
                           cs (all-ok (fn [c]
                                        ;; recorded BEFORE its up: a failed up leaves
                                        ;; containers that stack-down must still remove
                                        (record! ctx rid #(assoc-in % [:consumers c]
                                                                    {:compose-project (run/consumer-compose-project
                                                                                       (:settings ctx) c rid)}))
                                        (r/map-ok (up-consumer! ctx engine sp ws c wait?)
                                                  (fn [row] (record! ctx rid #(assoc-in % [:consumers c]
                                                                                        (select-keys row [:compose-project :services])))
                                                    row)))
                                      consumers)]
                  (r/ok {:run rid :stack stack-id :instances instances :consumers cs
                         :networks (mapv :network nets)}))]
        (if (r/ok? res)
          (do (record! ctx rid #(assoc % :status :up)) res)
          (assoc res :run rid :hint (str "the run is recorded as partial; stack-down run=" rid " releases it")))))))

;; ---------------------------------------------------------------------------
;; down

(defn- runs-of
  "Result<[run]>: `run` when given (it must exist), else every run of `stack-id`."
  [ctx {stack-id :stack run-id :run}]
  (let [all (stacks ctx)]
    (cond
      run-id (if (contains? all run-id) (r/ok [run-id]) (r/err :compose/unknown-run {:run run-id :known (vec (sort (keys all)))}))
      stack-id (let [rs (vec (sort (for [[k e] all :when (= stack-id (:stack/id e))] k)))]
                 (if (seq rs) (r/ok rs) (r/err :compose/no-runs {:stack stack-id})))
      :else (r/err :compose/missing-param {:param "stack or run"}))))


(defn- down-consumer! [ctx engine [c {:keys [compose-project]}]]
  (let [proj (project-of ctx (run/consumer-project-id c))]
    (r/map-ok (port/-down-project! engine (cond-> {:project compose-project :all? true}
                                           (:project/dir proj) (assoc :dir (:project/dir proj))
                                           (seq (:project/files proj)) (assoc :files (:project/files proj))))
              (constantly c))))

(defn- release-instance! [ctx engine sp step]
  (let [ip (get-in sp [:stack/instances (:instance step)])
        spec (run/instance-spec (:settings ctx) ip false)]
    (cond
      (run/whole-project? ip step)
      (r/map-ok (port/-down-project! engine (assoc spec :all? true)) (constantly step))
      (seq (:services step))
      (r/map-ok (port/-down-project! engine (assoc (dissoc spec :labels) :services (:services step)))
                (constantly step))
      :else (r/ok step))))

(defn- down-run!
  "Result<report>: run `rid` taken down: its release planned against the
   state, the IO done and the entry dropped. Called under the stack lock
   (stack-down!), so no up records itself between the plan and the IO."
  [ctx engine rid]
  (let [e (get (stacks ctx) rid)
        sp (:plan e)
        steps (mapv #(run/sparing-preexisting % (run/preexisting-of (:preexisting @(:state ctx)) %))
                    (stack/stack-releases (live-plans ctx) rid (:active @(:state ctx))))
        consumers (mapv #(down-consumer! ctx engine %) (sort-by key (:consumers e)))
        nets (mapv #(port/-network-rm! engine %) (:networks e))
        released (mapv #(release-instance! ctx engine sp %) steps)
        errors (vec (filter r/err? (concat consumers nets released)))]
    (if (empty? errors)
      (do (swap! (:state ctx) update :stacks dissoc rid)
          (persist! ctx)
          (r/ok {:run rid :stack (:stack/id e)
                 :consumers (mapv :ok consumers)
                 :released (vec (for [s steps :when (seq (:services s))]
                                  (select-keys s [:instance :project :services])))
                 :kept (vec (for [s steps :when (:kept s)] (select-keys s [:instance :project :kept])))
                 :networks-removed (vec (for [n nets :when (get-in n [:ok :removed?])] (get-in n [:ok :network])))}))
      (r/err :compose/stack-down-incomplete {:run rid :errors errors
                                             :hint "the run stays recorded; stack-down again once fixed"}))))

(defn stack-down!
  "Result<{:runs [report]}>: take down `opts` {:run id} or every run of
   {:stack id}. Volumes are never removed. Holds the stack lock throughout."
  [ctx opts]
  (serialized ctx
    (r/let-ok [engine (stack-engine ctx)
               rids (runs-of ctx opts)
               reports (all-ok #(down-run! ctx engine %) rids)]
      (r/ok {:runs reports}))))

;; ---------------------------------------------------------------------------
;; reads

(defn stack-status
  "Result<{:stacks [row]}>: every run (narrowed by `opts` {:stack :run}), its
   instances' and consumers' containers as they are now."
  [ctx {stack-id :stack run-id :run}]
  (r/let-ok [engine (stack-engine ctx)]
    (let [t (now ctx)
          rows (for [[rid e] (sort-by key (stacks ctx))
                     :when (and (or (nil? run-id) (= run-id rid))
                                (or (nil? stack-id) (= stack-id (:stack/id e))))]
                 (let [health #(let [h (project-health engine %)] (if (r/ok? h) (:ok h) {:error (:error h)}))]
                   (cond-> {:run rid :stack (:stack/id e) :status (:status e)
                            :started-at (:started-at e)
                            :idle-seconds (quot (- t (:last-touch e t)) 1000)
                            :networks (:networks e)
                            :instances (into (sorted-map)
                                             (for [[iid ip] (get-in e [:plan :stack/instances])]
                                               [iid {:compose-project (:instance/compose-project ip)
                                                     :shared? (boolean (:instance/share? ip))
                                                     :services (select-keys (health (:instance/compose-project ip))
                                                                            (:instance/services ip))}]))
                            :consumers (into (sorted-map)
                                             (for [[c {:keys [compose-project]}] (:consumers e)]
                                               [c {:compose-project compose-project :services (health compose-project)}]))}
                     (seq (:claims e)) (assoc :claims (:claims e))
                     (:orphaned-at e) (assoc :orphaned? true))))]
      (r/ok {:stacks (vec rows)}))))

(defn plan
  "Result<{:diff :text :budget :admission}>: hc-4 plan-diff of stack
   `stack-id` against what runs now (by compose project label), the capped
   memory of every instance with the total and what sharing saves (hc-16a),
   and the rendering of both. `:admission` is :ok, or the
   :compose/over-budget error stack-up would answer (then the text says so)."
  [ctx {stack-id :stack run-id :run}]
  (r/let-ok [engine (stack-engine ctx)
             {sp0 :plan budget0 :budget} (resolve-plan ctx stack-id (or run-id (run/run-id stack-id "000000")))
             sp (with-needs engine (:settings ctx) sp0)
             running (all-ok (fn [cp] (r/map-ok (project-health engine cp)
                                                (fn [h] (for [[s {:keys [state]}] h :when (= "running" state)]
                                                          {:project cp :service s}))))
                             (distinct (map :instance/compose-project (vals (:stack/instances sp)))))
             footprints (running-footprints engine (:settings ctx) sp)]
    (let [facts (private-facts ctx engine sp)
          budget (resources/with-closure budget0 (:settings ctx) sp facts)
          d (stack/plan-diff sp (apply concat running))
          ;; the same basis stack-up! admits on: held by a live run or running
          verdict (admit ctx stack-id budget (running-instances ctx sp footprints))
          admission (if (r/ok? verdict) :ok (dissoc verdict :ok))
          warns (resources/xmx-warnings budget facts)]
      (r/ok {:diff d
             :budget budget
             :admission admission
             :warnings warns
             :text (str (stack/render-diff d) "\n\n" (resources/summary budget)
                        (when (seq warns) (str "\n" (str/join "\n" warns)))
                        (when (not= :ok admission) (str "\nREFUSED: " (:message admission))))}))))

;; ---------------------------------------------------------------------------
;; claims and the reaper

(defn claim-run
  "The run id named by a claim target `stack:<run>`, nil for any other target."
  [target]
  (when (and (string? target) (str/starts-with? target claim-prefix))
    (subs target (count claim-prefix))))

(defn claim!
  "Result<{:run :claims}>: `agent` holds run `rid`; renews an existing claim."
  [ctx rid agent {:keys [note]}]
  (serialized ctx
    (cond
      (str/blank? agent) (r/err :compose/missing-param {:param "agent"})
      (not (contains? (stacks ctx) rid)) (r/err :compose/unknown-run {:run rid :known (vec (sort (keys (stacks ctx))))})
      :else (let [t (now ctx)]
              (record! ctx rid (fn [e] (-> e
                                           (update-in [:claims agent] #(cond-> (assoc (or % {:since t}) :seen t)
                                                                          note (assoc :note note)))
                                           (assoc :last-touch t)
                                           (dissoc :orphaned-at))))
              (r/ok {:run rid :claims (get-in (stacks ctx) [rid :claims])})))))

(defn release-claim!
  "Result<{:run :claims ..}>: `agent` gives up run `rid`. With no claim left the
   run is orphaned (reaped after the orphan TTL), or taken down now with `stop?`."
  [ctx rid agent {:keys [stop?]}]
  (serialized ctx
    (if-not (contains? (stacks ctx) rid)
      (r/err :compose/unknown-run {:run rid})
      (do (record! ctx rid (fn [e] (let [e (-> e (update :claims dissoc agent) (assoc :last-touch (now ctx)))]
                                     (if (empty? (:claims e)) (assoc e :orphaned-at (now ctx)) e))))
          (let [left (get-in (stacks ctx) [rid :claims])]
            (if (and stop? (empty? left))
              (r/map-ok (stack-down! ctx {:run rid}) #(assoc % :run rid :claims {}))
              (r/ok {:run rid :claims (or left {})})))))))

(defn- ttls [settings]
  {:ttl-ms (* 60000 (or (:compose/default-ttl-minutes settings) 60))
   :orphan-ttl-ms (* 60000 (or (:compose/orphan-ttl-minutes settings) (:compose/default-ttl-minutes settings) 60))
   :claim-ttl-ms (* 60000 (or (:compose/claim-ttl-minutes settings) 30))})

(defn- down-row [ctx rid extra]
  (let [x (try (stack-down! ctx {:run rid})
               (catch Throwable t (r/err :compose/release-threw {:message (str t)})))]
    (merge (if (r/ok? x) {:run rid :ok? true} {:run rid :ok? false :error x}) extra)))

(defn- pressure-idle-ms
  "How long a run must be untouched before the pressure guard may stop it:
   the orphan TTL (the reaper's shortest idle threshold), so a run in use
   just now is never taken."
  [settings]
  (:orphan-ttl-ms (ttls settings)))

(defn pressure-step!
  "The reaper's memory pressure guard (hc-16a): when the host's MemAvailable
   is below :compose/host-reserve, stop ONE run, the idle unclaimed run of
   the lowest :stack/priority (resources/pressure-victim). Answers its row
   (with :reason :memory-pressure, :available, :reserve), or nil."
  [ctx]
  (let [settings (:settings ctx)
        available (mem-available ctx)]
    (when (resources/under-pressure? settings available)
      (when-let [rid (resources/pressure-victim settings (stacks ctx) (now ctx)
                                                {:claim-ttl-ms (:claim-ttl-ms (ttls settings))
                                                 :idle-ms (pressure-idle-ms settings)})]
        (down-row ctx rid {:reason :memory-pressure
                           :available available
                           :reserve (resources/parse-mem (:compose/host-reserve settings))})))))

(defn reap!
  "{:reaped [report]}: take down every idle, unclaimed run, then, while the
   host is under memory pressure, the lowest-priority unclaimed run (one per
   pass, so the next pass sees what it freed). A run that cannot be taken
   down answers its error row and stays. Holds the stack lock from choosing
   the idle runs and the pressure victim to the last down, so no claim, up
   or down lands between a choice and its release."
  [ctx]
  (if-not (satisfies? port/IStackEngine (:engine ctx))
    {:reaped []}
    (serialized ctx
      (let [rids (run/idle-runs (stacks ctx) (now ctx) (ttls (:settings ctx)))
            idle (mapv #(down-row ctx % nil) rids)
            pressure (pressure-step! ctx)]
        {:reaped (cond-> idle pressure (conj pressure))}))))
