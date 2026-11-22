(ns hive-compose.promote.stack-run
  "Pure parts of running a stack (hc-6): run ids, the ProjectSpec each
   instance and consumer is brought up with (see hive-compose.port/IStackEngine),
   readiness, what a release spares, and which runs the reaper takes.

   Placement rules (decision 20261122141106-2d375c4f):
   - a SHARED instance runs under its provider's own compose project with the
     project's own files and NO stack override: it is the same container a
     section `up` of that project runs, so neither a second stack nor a section
     ever recreates it. Its stack membership lives in the state file
     (`:stacks`), not in labels;
   - a PRIVATE instance runs under `<provider-project>-<consumer>-<run>` with
     an override putting hive.run / hive.stack / hive.instance on every
     service of its closure;
   - a CONSUMER (a project or a section with containers) runs under
     `<consumer-project>-<run>`, labelled hive.run / hive.stack /
     hive.consumer, attached only to its view network, its wiring env passed
     into its containers by name (values stay in memory until spawn).
   Instances join view networks with `docker network connect --alias`, so
   nothing is recreated for a new consumer.

   Every created network and every container a stack run creates under a
   run-scoped compose project carries hive.run. No IO."
  (:require [clojure.string :as str]
            [hive-compose.promote.otel :as otel]
            [hive-compose.promote.stack :as stack]
            [hive-compose.schema :as schema]))

;; SPDX-License-Identifier: MIT

(def consumer-label (:consumer schema/docker-labels "hive.consumer"))

(defn run-id
  "The RunId of a new run of stack `stack-id`: its id lower-cased with every
   char outside [a-z0-9] dropped, cut to 30, then `hex` (6 lowercase hex
   chars): \"shop\" -> \"shop3f9a1c\". A stack id with no such char yields
   \"run<hex>\"."
  [stack-id hex]
  (let [a (str/replace (str/lower-case (str stack-id)) #"[^a-z0-9]" "")
        a (if (str/blank? a) "run" a)]
    (str (subs a 0 (min 30 (count a))) hex)))

(defn container-consumer?
  "True when `consumer` runs containers of its own: a project or a section,
   not a stack's own needs (`stack:`) nor a suite (`suite:`, run by hc-8)."
  [consumer]
  (not (re-find #"^(stack|suite):" (str consumer))))

(defn consumer-project-id
  "The project id of a project or section consumer `<project>[/<target>]`."
  [consumer]
  (first (str/split (str consumer) #"/" 2)))

(defn consumer-compose-project
  "The compose project consumer `consumer` runs under in `run`:
   `<its project's compose name>-<run>` for a project, `<compose name>-<slug
   target>-<run>` for a section `<project>/<target>`, so two sections of one
   project in one run never share a compose project (nor its override)."
  [settings consumer run]
  (let [proj (get-in settings [:compose/projects (consumer-project-id consumer)])
        [_ tgt] (str/split (str consumer) #"/" 2)]
    (str (stack/project-compose-name proj)
         (when (seq tgt) (str "-" (schema/slug tgt)))
         "-" run)))

(defn- project-base [proj]
  (cond-> {:dir (:project/dir proj)}
    (seq (:project/files proj)) (assoc :files (vec (:project/files proj)))))

(defn instance-spec
  "ProjectSpec of planned instance `ip` (a StackPlan instance, with
   `:instance/needs` its depends_on closure when known): a shared one as its
   provider project runs it, a private one labelled over its whole closure."
  [settings ip wait?]
  (let [proj (get-in settings [:compose/projects (:instance/provider ip)])
        closure (vec (distinct (concat (:instance/services ip) (:instance/needs ip))))]
    (cond-> (merge (project-base proj)
                   {:project (:instance/compose-project ip)
                    :services (vec (:instance/services ip))
                    :env (or (:project/env proj) {})
                    :wait? (boolean wait?)})
      (not (:instance/share? ip)) (assoc :services closure
                                         :labels (:instance/labels ip)))))

(defn consumer-labels
  "hive.run, hive.stack and hive.consumer of `consumer` in `run` of `stack-id`."
  [run stack-id consumer]
  (assoc (stack/labels run stack-id) consumer-label consumer))

(defn consumer-spec
  "ProjectSpec of `consumer` in `run`: `services` (its closure), the whole
   Wiring env for compose (`wiring`), only the `injected` variable names (what
   the stack adds: endpoints, tenancy, HIVE_RUN_ID, OTLP) passed into its
   containers, its labels, and its view network as its only network."
  [settings run stack-id consumer services wiring injected wait?]
  (let [proj (get-in settings [:compose/projects (consumer-project-id consumer)])]
    (merge (project-base proj)
           {:project (consumer-compose-project settings consumer run)
            :services (vec services)
            :env (:wiring/env wiring)
            :container-env (vec (sort injected))
            :labels (consumer-labels run stack-id consumer)
            :networks [(:wiring/network wiring)]
            :wait? (boolean wait?)})))

(defn ready?
  "True when every one of `services` has a container in `health`
   ({service {:state :health}}) that runs and is :healthy or has no
   healthcheck (:none)."
  [health services]
  (every? (fn [s] (let [{:keys [state] h :health} (get health s)]
                    (and (= "running" state) (contains? #{:healthy :none} h))))
          services))

(defn not-ready
  "{service {:state :health}} of `services` that are not ready in `health`
   (absent ones as {:state \"missing\"})."
  [health services]
  (into (sorted-map)
        (for [s services
              :let [row (get health s {:state "missing" :health :none})]
              :when (not (ready? {s row} [s]))]
          [s row])))

(defn held-by-others
  "#{[compose-project service]} the instances of every live run but `run`
   keep alive (`runs` {run StackPlan})."
  [runs run]
  (into #{} (for [[r sp] runs :when (not= r run)
                  [_ ip] (:stack/instances sp)
                  k (stack/instance-footprint ip)]
              k)))

(defn sparing-preexisting
  "Release step `step` (hc-4 instance-release) with the services that ran
   before this run brought them up (`preexisting`, [service]) moved from
   :services to :kept as :preexisting: a run never removes what it did not
   start."
  [step preexisting]
  (let [pre (set preexisting)
        spared (filter pre (:services step))]
    (cond-> (update step :services #(vec (remove pre %)))
      (seq spared) (update :kept (fnil into (sorted-map)) (map (fn [s] [s [[:preexisting]]])) spared))))

(defn stack-needs-per-consumer
  "{consumer Needs}: the stack's own `:stack/needs` as extra needs of each of
   its container consumers, so a capability the stack needs (otel) is bound
   by every consumer and joins every view network. Empty when the stack
   needs nothing."
  [stack]
  (if-let [needs (not-empty (:stack/needs stack))]
    (into {} (for [c (:stack/consumers stack) :when (container-consumer? c)] [c needs]))
    {}))

(def otel-capability "otel")

(defn otlp-envs
  "The extra-envs-fn for `promote.wiring/wirings`: a consumer bound to an
   `otel` instance in `resolution` gets the OTLP env (endpoint on its view
   network: `http://<alias>:4317`, the bound instance's first alias, which
   is the name it joins that network under; with its protocol as a pair,
   service name and hive.run.id). A consumer bound to none gets nothing: no
   endpoint nobody answers."
  [resolution run]
  (fn [consumer]
    (when-let [iid (get-in resolution [:resolution/bindings consumer otel-capability])]
      (let [alias (or (first (get-in resolution [:resolution/instances iid :instance/aliases])) otel/service)]
        [(otel/otlp-env {:run-id run :service (schema/slug consumer)
                         :endpoint (str "http://" alias ":" (:otlp-grpc otel/ports))})]))))

(defn without-provided
  "`services` of a consumer minus what its own provided instances (`ips`,
   hc-4 `:consumer/provider-of`) run, their depends_on closures included: a
   consumer step never re-ups an instance it provides."
  [services ips]
  (let [provided (into #{} (mapcat #(concat (:instance/services %) (:instance/needs %))) ips)]
    (vec (remove provided services))))

(defn spare-stack-held
  "Section release step `step` (promote.plan: {:project :services ..}) with
   the services live stack instances keep alive (`held`, hc-4
   `stack/stack-held`: {compose-project #{service}}) moved from :services to
   :kept as [:stack]: a section down or reap never stops a stack's instance."
  [step held]
  (let [h (get held (:project step) #{})
        spared (filter h (:services step))]
    (cond-> step
      (seq spared) (-> (update :services #(vec (remove h %)))
                       (update :kept (fnil into (sorted-map)) (map (fn [s] [s [[:stack]]])) spared)))))

(defn mark-preexisting
  "Preexistence marks ({provider-project #{service}}) after a run found
   `running` ({provider-project #{service}}: what of its shared instances'
   footprints ran before it started). A running service is the world's, and
   marked, unless another live run's instance holds it (`runs`, {run
   StackPlan}, this run excluded; the last of them releases it) or an active
   section holds it (`section-held`, {compose-project #{service}}, which
   keeps and releases it). Shared instances run under their provider's
   compose project, so `running` keys are both provider and compose project.
   Marks only grow: a mark is never cleared, so whichever run leaves last
   never removes what ran before the runs. Pure; computed against the state
   current inside the swap, so concurrent runs cannot race it."
  [marks running runs run section-held]
  (let [others (held-by-others runs run)]
    (reduce-kv (fn [m provider svcs]
                 (let [fresh (remove #(or (others [provider %])
                                          (contains? (get section-held provider #{}) %))
                                     svcs)]
                   (cond-> m (seq fresh) (update provider (fnil into (sorted-set)) fresh))))
               (or marks {})
               running)))

(defn preexisting-of
  "[service] marked preexisting for the provider project of release `step`
   ({:project ..}): every down step spares them, whichever instance or run."
  [marks step]
  (seq (get marks (:project step))))

(defn whole-project?
  "True when release `step` of a private instance takes its whole compose
   project down: it is private and nothing of it is kept."
  [ip step]
  (and (not (:instance/share? ip)) (empty? (:kept step))))

(defn claim-live?
  "True when `claim` was renewed within `claim-ttl-ms` of `now`."
  [claim now claim-ttl-ms]
  (< (- now (:seen claim 0)) claim-ttl-ms))

(defn idle-runs
  "Runs of `stacks` ({run entry}) the reaper takes down at `now`: no live
   claim, and idle past the TTL since `:last-touch` (the orphan TTL once its
   last claim ended, `:orphaned-at`). `ttls` {:ttl-ms :orphan-ttl-ms
   :claim-ttl-ms}. An entry with `:idle-action :none` is never reaped."
  [stacks now {:keys [ttl-ms orphan-ttl-ms claim-ttl-ms]}]
  (vec (sort (for [[run e] stacks
                   :when (not= :none (:idle-action e))
                   :when (not-any? #(claim-live? % now claim-ttl-ms) (vals (:claims e)))
                   :let [ttl (if (:orphaned-at e) (min ttl-ms orphan-ttl-ms) ttl-ms)]
                   :when (>= (- now (:last-touch e 0)) ttl)]
               run))))
