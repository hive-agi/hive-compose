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
   :preexisting {instance-id [service]} :networks [name] :consumers {consumer
   {:compose-project :services}} :status :up|:partial :claims {agent Claim}
   :orphaned-at ms}."
  (:require [clojure.string :as str]
            [hive-compose.port :as port]
            [hive-compose.promote.capability :as capability]
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

(defn- live-plans
  "{run StackPlan} of every recorded run."
  [ctx]
  (into {} (map (fn [[k e]] [k (:plan e)])) (stacks ctx)))

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

(defn- preexisting
  "Result<{instance-id [service]}>: what of each shared instance's closure ran
   before this run and no other live run (in `runs`, {run StackPlan}) holds:
   the world before stacks, never removed by this run. What another live run
   holds is left to the refcount, so the last run releases it."
  [engine settings sp runs]
  (let [others (run/held-by-others runs (:stack/run sp))]
    (r/map-ok
     (all-ok (fn [[iid ip]]
               (if (:instance/share? ip)
                 (let [proj (get-in settings [:compose/projects (:instance/provider ip)])
                       cp (:instance/compose-project ip)]
                   (r/map-ok (port/-running engine (target/project-profile proj))
                             (fn [up] [iid (vec (sort (for [[_ s :as k] (stack/instance-footprint ip)
                                                            :when (and (contains? (set up) s) (not (others k)))]
                                                        s)))])))
                 (r/ok [iid []])))
             (:stack/instances sp))
     #(into (sorted-map) (remove (comp empty? second)) %))))

(defn resolve-plan
  "Result<{:resolution :plan :wirings}> of stack `stack-id` for run `run-id`:
   resolution (hc-3), StackPlan (hc-4) and Wirings (hc-5). Pure, no engine."
  [ctx stack-id run-id]
  (let [settings (:settings ctx)]
    (r/let-ok [s (stack-config ctx stack-id)
               ;; a stack's own needs (otel, ...) are needs of every container
               ;; consumer in it, shared: one instance joins each view network
               res (capability/resolve-stack (:compose/projects settings) s
                                             {:extra (run/stack-needs-per-consumer s)})
               sp (stack/stack-plan settings res run-id {:stack stack-id})
               ws (wiring/wirings settings res run-id (run/otlp-envs res run-id))]
      (r/ok {:resolution res :plan sp :wirings ws}))))

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
  [ctx engine ip wait?]
  (let [spec (cond-> (run/instance-spec (:settings ctx) ip wait?)
               (and wait? (wait-timeout-s ctx)) (assoc :wait-timeout-s (wait-timeout-s ctx)))
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

(defn- pick-run-id [ctx stack-id run-id]
  (let [rid (or run-id (run/run-id stack-id ((or (:rand-hex ctx) random-hex))))]
    (cond
      (not (m/validate schema/RunId rid))
      (r/err :compose/invalid-run {:run rid :problems (schema/explain schema/RunId rid)})
      (contains? (stacks ctx) rid)
      (r/err :compose/run-exists {:run rid :hint "stack-down run=<id> first, or omit run for a fresh id"})
      :else (r/ok rid))))

(defn stack-up!
  "Result<{:run :stack :instances [..] :consumers [..] :networks [..]}>: run
   stack `stack` (`opts` {:stack id :run id? :wait? (default true)}). The run
   is recorded before anything starts, so a failure midway answers an error
   carrying `:run` that stack-down! releases."
  [ctx {stack-id :stack run-id :run wait? :wait? :or {wait? true}}]
  (r/let-ok [engine (stack-engine ctx)
             rid (pick-run-id ctx stack-id run-id)
             {sp0 :plan ws :wirings} (resolve-plan ctx stack-id rid)
             sp (with-needs engine (:settings ctx) sp0)
             pre (preexisting engine (:settings ctx) sp (live-plans ctx))]
    (let [t (now ctx)
          consumers (vec (filter run/container-consumer? (keys (:stack/consumers sp))))]
      ;; preexistence is global and permanent: set by the first run that finds
      ;; an instance's services running outside every run, never cleared
      (swap! (:state ctx) update :preexisting
             #(run/mark-preexisting % (:stack/instances sp) pre))
      (record! ctx rid (constantly {:stack/id stack-id :run rid :started-at t :last-touch t
                                    :plan sp :networks (:stack/networks sp)
                                    :consumers {} :status :partial}))
      (let [res (r/let-ok [nets (all-ok #(port/-network-create! engine % (stack/labels rid stack-id))
                                        (:stack/networks sp))
                           instances (all-ok (fn [{:step/keys [id]}]
                                               (up-instance! ctx engine (get-in sp [:stack/instances id]) wait?))
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

(defn- down-run! [ctx engine rid]
  (let [e (get (stacks ctx) rid)
        sp (:plan e)
        steps (mapv #(run/sparing-preexisting
                      % (run/preexisting-of (:preexisting @(:state ctx))
                                            (get-in sp [:stack/instances (:instance %)])))
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
   {:stack id}. Volumes are never removed."
  [ctx opts]
  (r/let-ok [engine (stack-engine ctx)
             rids (runs-of ctx opts)
             reports (all-ok #(down-run! ctx engine %) rids)]
    (r/ok {:runs reports})))

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
  "Result<{:diff :text}>: hc-4 plan-diff of stack `stack-id` against what
   runs now (by compose project label), and its rendering."
  [ctx {stack-id :stack run-id :run}]
  (r/let-ok [engine (stack-engine ctx)
             {sp :plan} (resolve-plan ctx stack-id (or run-id (run/run-id stack-id "000000")))
             running (all-ok (fn [cp] (r/map-ok (project-health engine cp)
                                                (fn [h] (for [[s {:keys [state]}] h :when (= "running" state)]
                                                          {:project cp :service s}))))
                             (distinct (map :instance/compose-project (vals (:stack/instances sp)))))]
    (let [d (stack/plan-diff sp (apply concat running))]
      (r/ok {:diff d :text (stack/render-diff d)}))))

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
  (cond
    (str/blank? agent) (r/err :compose/missing-param {:param "agent"})
    (not (contains? (stacks ctx) rid)) (r/err :compose/unknown-run {:run rid :known (vec (sort (keys (stacks ctx))))})
    :else (let [t (now ctx)]
            (record! ctx rid (fn [e] (-> e
                                         (update-in [:claims agent] #(cond-> (assoc (or % {:since t}) :seen t)
                                                                        note (assoc :note note)))
                                         (assoc :last-touch t)
                                         (dissoc :orphaned-at))))
            (r/ok {:run rid :claims (get-in (stacks ctx) [rid :claims])}))))

(defn release-claim!
  "Result<{:run :claims ..}>: `agent` gives up run `rid`. With no claim left the
   run is orphaned (reaped after the orphan TTL), or taken down now with `stop?`."
  [ctx rid agent {:keys [stop?]}]
  (if-not (contains? (stacks ctx) rid)
    (r/err :compose/unknown-run {:run rid})
    (do (record! ctx rid (fn [e] (let [e (-> e (update :claims dissoc agent) (assoc :last-touch (now ctx)))]
                                   (if (empty? (:claims e)) (assoc e :orphaned-at (now ctx)) e))))
        (let [left (get-in (stacks ctx) [rid :claims])]
          (if (and stop? (empty? left))
            (r/map-ok (stack-down! ctx {:run rid}) #(assoc % :run rid :claims {}))
            (r/ok {:run rid :claims (or left {})}))))))

(defn- ttls [settings]
  {:ttl-ms (* 60000 (or (:compose/default-ttl-minutes settings) 60))
   :orphan-ttl-ms (* 60000 (or (:compose/orphan-ttl-minutes settings) (:compose/default-ttl-minutes settings) 60))
   :claim-ttl-ms (* 60000 (or (:compose/claim-ttl-minutes settings) 30))})

(defn reap!
  "{:reaped [report]}: take down every idle, unclaimed run. A run that cannot
   be taken down answers its error row and stays."
  [ctx]
  (if-not (satisfies? port/IStackEngine (:engine ctx))
    {:reaped []}
    (let [rids (run/idle-runs (stacks ctx) (now ctx) (ttls (:settings ctx)))]
      {:reaped (mapv (fn [rid]
                       (let [x (try (stack-down! ctx {:run rid})
                                    (catch Throwable t (r/err :compose/release-threw {:message (str t)})))]
                         (if (r/ok? x) {:run rid :ok? true} {:run rid :ok? false :error x})))
                     rids)})))
