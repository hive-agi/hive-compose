(ns hive-compose.promote.faults
  "Pure: fault injection through Toxiproxy as an interposed capability (hc-12).

   A Need `{:via :toxiproxy}` puts a proxy between its consumer and the
   instance that answers it. One toxiproxy container serves a whole run:

   - it joins the consumer's view network under the ORIGINAL aliases of the
     interposed instance (so `pg:5432` in the consumer's config is right as
     written) plus one internal alias, `listen-alias`, unique per consumer;
   - the real instance leaves that consumer's view network and is reached by
     toxiproxy alone, on the run's faults network, under `upstream-alias`
     (unique per instance, so a shared and a private instance of one
     capability never collide);
   - each proxy listens on `<listen-alias>:<port>` (the provider's `:port`)
     and forwards to `<upstream-alias>:<port>`. Toxiproxy resolves the listen
     host through Docker DNS inside its own container when the proxy is
     created, to its own address on that view network, so one port serves
     different consumers on different view networks. Toxiproxy must
     therefore be connected to a view network before its proxies are made.

   Consumers with a proxy, and every suite consumer `suite:<id>` of a run
   holding one, get TOXIPROXY_URL (the REST API, port 8474, under the
   consumer's listen alias) and TOXIPROXY_PROXIES (the proxy names they may
   drive, comma-separated; a suite may drive all of them).

   `plan` answers the interposition; `interpose` rewrites the hc-5 Wirings
   with it: a consumer's attach keeps exactly the aliases it had, only the
   instance answering them changes. `project-spec` and `joins` are what the
   stack ops (hc-6, IStackEngine) run; `hive-compose.pipeline.faults`
   creates the proxies through IToxiproxyClient."
  (:refer-clojure :exclude [interpose proxy-name])
  (:require [clojure.string :as str]
            [hive-compose.schema :as schema]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

;; ---------------------------------------------------------------------------
;; Provider data

(def toxiproxy-version
  "The ghcr.io/shopify/toxiproxy tag the compose fragment pins."
  "2.12.0")

(def image
  (str "ghcr.io/shopify/toxiproxy:" toxiproxy-version))

(def via
  "The `:via` value that interposes toxiproxy."
  :toxiproxy)

(def instance-id
  "The id the run's toxiproxy takes in attach lists and labels."
  "faults/toxiproxy")

(def service
  "The compose service of the bundled fragment."
  "toxiproxy")

(def api-port
  "Toxiproxy's REST API port inside its container."
  8474)

(def compose-resource
  "Classpath resource of the compose fragment running toxiproxy."
  "hive-compose/toxiproxy/compose.yml")

(def url-var "TOXIPROXY_URL")

(def proxies-var "TOXIPROXY_PROXIES")

;; ---------------------------------------------------------------------------
;; Names, built with the hc-2 name builders

(defn faults-network
  "The run's faults network, toxiproxy and the interposed instances only: the
   view network of the pseudo consumer `faults`."
  [run]
  (schema/view-network run "faults"))

(defn compose-project
  "The compose project the run's toxiproxy runs under."
  [run]
  (schema/private-project "hive-toxiproxy" "faults" run))

(defn listen-alias
  "Toxiproxy's internal alias on `consumer`'s view network: the host its
   proxies for that consumer listen on, and the TOXIPROXY_URL host."
  [consumer]
  (str "toxiproxy-" (schema/slug consumer)))

(defn upstream-alias
  "The alias instance `id` joins the faults network under."
  [id]
  (str "upstream-" (schema/slug id)))

(defn proxy-name
  "The toxiproxy proxy name of `consumer`'s capability `cap`."
  [consumer cap]
  (str (schema/slug consumer) "." cap))

(defn suite?
  [consumer]
  (str/starts-with? (str consumer) "suite:"))

;; ---------------------------------------------------------------------------
;; Needs

(defn consumer-needs
  "The Needs `consumer` declared: a suite's `:suite/needs`, a project's or a
   section's `:project/needs`, merged with `extra-needs` ({consumer Needs},
   e.g. a stack's own `:stack/needs`)."
  [settings extra-needs consumer]
  (merge (if (suite? consumer)
           (get-in settings [:compose/suites (subs consumer 6) :suite/needs] {})
           (get-in settings [:compose/projects (first (str/split consumer #"/" 2)) :project/needs] {}))
         (get extra-needs consumer)))

(defn- via-of [need]
  (let [v (:via need)]
    (cond (keyword? v) v
          (string? v) (keyword v)
          :else nil)))

(defn- provide
  [settings inst]
  (get-in settings [:compose/projects (:instance/provider inst)
                    :project/provides (:instance/capability inst)]))

;; ---------------------------------------------------------------------------
;; Plan

(defn- proxy-of
  "Result<Proxy> for `consumer`'s capability `cap` bound to instance `id`."
  [settings instances consumer cap id]
  (if-let [inst (get instances id)]
    (if-let [port (:port (provide settings inst))]
      (r/ok {:proxy/name (proxy-name consumer cap)
             :proxy/consumer consumer
             :proxy/capability cap
             :proxy/instance id
             :proxy/aliases (vec (:instance/aliases inst))
             :proxy/port port
             :proxy/listen (str (listen-alias consumer) ":" port)
             :proxy/upstream (str (upstream-alias id) ":" port)})
      (r/err :compose/faults-missing-port
             {:consumer consumer :capability cap :provider (:instance/provider inst)
              :hint (str "add :port to :project/provides \"" cap "\" of project "
                         (:instance/provider inst) ": toxiproxy forwards to <service>:<port>")}))
    (r/err :compose/unknown-instance {:consumer consumer :instances [id]})))

(defn- interposed
  "Result<[[consumer cap instance-id] ...]> of every binding whose Need goes
   via toxiproxy, sorted; an error for a `:via` nobody implements."
  [settings extra-needs resolution]
  (reduce (fn [acc [consumer binding]]
            (let [needs (consumer-needs settings extra-needs consumer)]
              (reduce (fn [acc [cap id]]
                        (let [v (via-of (get needs cap))]
                          (cond
                            (nil? v) acc
                            (= via v) (r/map-ok acc #(conj % [consumer cap id]))
                            :else (reduced (r/err :compose/unknown-via
                                                  {:consumer consumer :capability cap :via v
                                                   :known [via]})))))
                      acc
                      (sort-by key binding))))
          (r/ok [])
          (sort-by key (:resolution/bindings resolution))))

(defn- clash
  "A Result error when one consumer would listen twice on one port, or two
   interposed instances share an upstream alias; nil when none."
  [proxies]
  (or (some (fn [[[consumer port] ps]]
              (when (< 1 (count ps))
                (r/err :compose/faults-port-clash
                       {:consumer consumer :port port :capabilities (mapv :proxy/capability ps)
                        :hint "one consumer cannot interpose two capabilities answering on one port"})))
            (sort-by key (group-by (juxt :proxy/consumer :proxy/port) proxies)))
      (some (fn [[alias ids]]
              (when (< 1 (count ids))
                (r/err :compose/faults-alias-clash {:alias alias :instances (vec ids)})))
            (sort-by key (group-by upstream-alias (distinct (map :proxy/instance proxies)))))))

(defn- consumer-env
  "TOXIPROXY_URL and TOXIPROXY_PROXIES of `consumer`."
  [consumer proxies]
  {url-var (str "http://" (listen-alias consumer) ":" api-port)
   proxies-var (str/join "," (sort (map :proxy/name (if (suite? consumer)
                                                      proxies
                                                      (filter #(= consumer (:proxy/consumer %)) proxies)))))})

(def empty-plan
  {:faults/toxiproxy nil :faults/proxies [] :faults/consumers {} :faults/env {}})

(defn- full-plan
  [run resolution proxies]
  (let [suites (filter suite? (keys (:resolution/bindings resolution)))
        consumers (sort (into (set (map :proxy/consumer proxies)) suites))]
    {:faults/run run
     :faults/network (faults-network run)
     :faults/toxiproxy {:id instance-id
                        :image image
                        :service service
                        :compose-project (compose-project run)
                        :api-port api-port
                        :labels {(:run schema/docker-labels) run
                                 (:instance schema/docker-labels) instance-id}}
     :faults/proxies (vec (sort-by :proxy/name proxies))
     :faults/consumers (into (sorted-map)
                             (map (fn [c]
                                    (let [mine (filter #(= c (:proxy/consumer %)) proxies)]
                                      [c {:aliases (vec (distinct (mapcat :proxy/aliases mine)))
                                          :instances (vec (sort (distinct (map :proxy/instance mine))))
                                          :listen-alias (listen-alias c)}])))
                             consumers)
     :faults/env (into (sorted-map) (map (fn [c] [c (consumer-env c proxies)])) consumers)}))

(defn plan
  "Result<Plan> of the interposition in run `run` over `resolution`.
   `extra-needs` ({consumer Needs}) adds needs settings do not hold (a
   stack's `:stack/needs`). With no Need `:via :toxiproxy` the plan holds no
   proxy and `:faults/toxiproxy` is nil. Otherwise:

   {:faults/run :faults/network
    :faults/toxiproxy {:id :image :service :compose-project :api-port :labels}
    :faults/proxies   [{:proxy/name :proxy/consumer :proxy/capability
                        :proxy/instance :proxy/aliases :proxy/port
                        :proxy/listen :proxy/upstream}]
    :faults/consumers {consumer {:aliases :instances :listen-alias}}
    :faults/env       {consumer {TOXIPROXY_URL .. TOXIPROXY_PROXIES ..}}}

   Errors: :compose/invalid-run, :compose/unknown-via,
   :compose/unknown-instance, :compose/faults-missing-port,
   :compose/faults-port-clash, :compose/faults-alias-clash."
  ([settings resolution run] (plan settings resolution run {}))
  ([settings resolution run extra-needs]
   (if-let [problems (schema/explain schema/RunId run)]
     (r/err :compose/invalid-run {:run run :problems problems})
     (r/let-ok [triples (interposed settings extra-needs resolution)
                proxies (reduce (fn [acc [c cap id]]
                                  (let [p (proxy-of settings (:resolution/instances resolution) c cap id)]
                                    (if (r/err? p) (reduced p) (r/map-ok acc #(conj % (:ok p))))))
                                (r/ok [])
                                triples)]
       (cond
         (clash proxies) (clash proxies)
         (empty? proxies) (r/ok (assoc empty-plan :faults/run run))
         :else (r/ok (full-plan run resolution proxies)))))))

(defn active?
  "True when plan `p` interposes anything."
  [p]
  (some? (:faults/toxiproxy p)))

;; ---------------------------------------------------------------------------
;; Wirings

(defn- interpose-one
  "`w` with toxiproxy answering the aliases of the instances interposed for
   its consumer, those instances off its attach list, and the faults env
   merged under its own env keys (a consumer setting TOXIPROXY_URL wins)."
  [p w]
  (let [c (:wiring/consumer w)
        {:keys [instances listen-alias] :as entry} (get-in p [:faults/consumers c])]
    (if-not entry
      w
      (let [gone (set instances)
            [moved kept] ((juxt filter remove) #(gone (:instance %)) (:wiring/attach w))
            aliases (vec (distinct (mapcat :aliases moved)))
            attach (cond-> (vec kept)
                     (seq aliases) (conj {:instance instance-id :aliases aliases
                                          :internal-aliases [listen-alias]}))]
        (-> w
            (assoc :wiring/attach (vec (sort-by :instance attach)))
            (update :wiring/env #(merge (get-in p [:faults/env c]) %)))))))

(defn interpose
  "Result<{consumer Wiring}>: `wirings` (hc-5) rewritten by plan `p`. A
   consumer the plan does not name is returned unchanged; for one it names,
   the set of aliases on its attach list is unchanged, only the instance
   answering the interposed ones becomes toxiproxy (with `:internal-aliases`
   [listen-alias] beside them, for the stack ops to connect too). A suite
   with no proxy of its own keeps its attach list and gets the env alone;
   `joins` connects toxiproxy to its view network under its listen alias, so
   it reaches the API. Each Wiring still conforms to schema/Wiring."
  [p wirings]
  (if-not (active? p)
    (r/ok wirings)
    (reduce (fn [acc [c w]]
              (let [w' (interpose-one p w)]
                (if-let [problems (schema/explain schema/Wiring w')]
                  (reduced (r/err :compose/invalid-wiring {:consumer c :problems problems}))
                  (r/map-ok acc #(assoc % c w')))))
            (r/ok (sorted-map))
            (sort-by key wirings))))

;; ---------------------------------------------------------------------------
;; The hc-4 StackPlan

(defn interpose-plan
  "StackPlan `sp` (hive-compose.promote.stack/stack-plan, as the hc-6 stack
   ops consume it) rewritten by faults plan `p`, so that what the ops connect
   matches the interposition:
   - each interposed instance leaves the view network of every consumer it
     is interposed for (`:instance/networks`), so Docker DNS answers the
     original alias on that network with toxiproxy alone;
   - each such consumer's `:consumer/attach` names toxiproxy, not the
     instance, for the interposed aliases (aliases unchanged);
   - `:stack/networks` gains the faults network, so the ops create it
     (labelled like every stack network) before anything joins it.
   The instance reaches the faults network, and toxiproxy every view network,
   through `joins`. `sp` is returned as it is when `p` interposes nothing."
  [p sp]
  (if-not (active? p)
    sp
    (let [run (:faults/run p)
          drop-nets (reduce (fn [acc {:proxy/keys [instance consumer]}]
                              (update acc instance (fnil conj #{}) (schema/view-network run consumer)))
                            {} (:faults/proxies p))
          gone (into {} (map (fn [[c {:keys [instances]}]] [c (set instances)])) (:faults/consumers p))]
      (-> sp
          (update :stack/instances
                  (fn [is] (into (empty is)
                                 (map (fn [[iid ip]]
                                        [iid (if-let [nets (get drop-nets iid)]
                                               (update ip :instance/networks #(vec (remove nets %)))
                                               ip)]))
                                 is)))
          (update :stack/consumers
                  (fn [cs] (into (empty cs)
                                 (map (fn [[c cp]]
                                        (let [g (get gone c #{})
                                              [moved kept] ((juxt filter remove) #(g (:instance %)) (:consumer/attach cp))
                                              aliases (vec (distinct (mapcat :aliases moved)))]
                                          [c (cond-> cp
                                               (seq moved) (assoc :consumer/attach
                                                                  (vec (sort-by :instance (conj (vec kept) {:instance instance-id :aliases aliases})))))])))
                                 cs)))
          (update :stack/networks #(vec (sort (distinct (conj (vec %) (:faults/network p))))))))))

;; ---------------------------------------------------------------------------
;; What the stack ops run

(defn joins
  "[{:network :instance :aliases}]: the `-network-connect!` calls plan `p`
   needs, toxiproxy's first (it joins the faults network through its spec).
   Toxiproxy joins each consumer's view network under the interposed
   aliases and its listen alias; each interposed instance joins the faults
   network under its upstream alias. The stack ops map an instance id to
   its {:project :service} target."
  [p]
  (if-not (active? p)
    []
    (into (vec (for [[c {:keys [aliases listen-alias]}] (:faults/consumers p)]
                 {:network (schema/view-network (:faults/run p) c)
                  :instance instance-id
                  :aliases (vec (distinct (conj aliases listen-alias)))}))
          (for [id (sort (distinct (map :proxy/instance (:faults/proxies p))))]
            {:network (:faults/network p) :instance id :aliases [(upstream-alias id)]}))))

(defn project-spec
  "The IStackEngine ProjectSpec bringing the run's toxiproxy up from the
   compose fragment written into `dir` (`compose-resource`), on the faults
   network, labelled, with `--wait`."
  [p dir]
  (let [{:keys [compose-project labels]} (:faults/toxiproxy p)]
    {:project compose-project
     :dir dir
     :files [(str dir "/compose.yml")]
     :services [service]
     :env {"TOXIPROXY_IMAGE" image}
     :labels labels
     :networks [(:faults/network p)]
     :wait? true}))

(defn teardown
  "[step]: what stack-down of the run must undo for plan `p`, in order, as
   IStackEngine calls. 1. `-down-project!` of toxiproxy's compose project
   with :all? (removing its container disconnects it from every view
   network). 2. `-network-rm!` of the faults network (which disconnects the
   interposed instances still attached, shared ones included). [] when `p`
   interposes nothing."
  [p dir]
  (if-not (active? p)
    []
    [{:step :down-project :spec (assoc (project-spec p dir) :all? true)}
     {:step :network-rm :network (:faults/network p)}]))

(defn proxy-config
  "The toxiproxy REST body creating proxy `pr`."
  [pr]
  {"name" (:proxy/name pr) "listen" (:proxy/listen pr)
   "upstream" (:proxy/upstream pr) "enabled" true})

;; ---------------------------------------------------------------------------
;; Toxics

(def toxic-types
  "Toxic types toxiproxy 2.x implements, with the attributes each reads."
  {"latency" #{"latency" "jitter"}
   "timeout" #{"timeout"}
   "bandwidth" #{"rate"}
   "reset_peer" #{"timeout"}
   "slow_close" #{"delay"}
   "limit_data" #{"bytes"}
   "slicer" #{"average_size" "size_variation" "delay"}})

(defn word
  "A keyword, symbol or string as its name; anything else (a number, a map,
   nil) as nil. Total: never throws on what JSON can send."
  [x]
  (cond (string? x) x
        (ident? x) (name x)
        :else nil))

(defn toxic-name
  "The name of the toxic a remove names: its :name, else `<type>_<stream>`
   (stream downstream by default), nil when neither is usable."
  [spec]
  (when (map? spec)
    (or (word (:name spec))
        (when-let [t (word (:type spec))]
          (str t "_" (or (word (:stream spec)) "downstream"))))))

(defn toxic
  "Result<toxic body> of a toxic spec as the `faults` command takes it:
   {:type :attributes {name number} :stream upstream|downstream
    :toxicity 0..1 :name}. The name defaults to `<type>_<stream>`."
  [spec]
  (let [spec (if (map? spec) spec {})
        {:keys [type attributes stream toxicity]} spec
        type (word type)
        stream (if (nil? stream) "downstream" (word stream))
        attrs (when (or (nil? attributes) (map? attributes))
                (into {} (map (fn [[k v]] [(word k) v])) attributes))
        known (get toxic-types type)]
    (cond
      (nil? attrs)
      (r/err :compose/faults-invalid-toxic {:hint "attributes is an object {name number}"})

      (nil? known)
      (r/err :compose/faults-unknown-toxic {:type type :known (vec (sort (keys toxic-types)))})

      (not (#{"upstream" "downstream"} stream))
      (r/err :compose/faults-invalid-toxic {:stream stream :known ["upstream" "downstream"]})

      (seq (remove known (keys attrs)))
      (r/err :compose/faults-invalid-toxic {:type type :unknown (vec (sort (remove known (keys attrs))))
                                            :known (vec (sort known))})

      (not-every? #(and (number? %) (not (neg? %))) (vals attrs))
      (r/err :compose/faults-invalid-toxic {:type type :hint "attributes are non-negative numbers"})

      (and (some? toxicity) (not (and (number? toxicity) (<= 0 toxicity 1))))
      (r/err :compose/faults-invalid-toxic {:toxicity toxicity :hint "toxicity is a number in [0, 1]"})

      :else
      (r/ok (cond-> {"name" (or (some-> (:name spec) str) (str type "_" stream))
                     "type" type
                     "stream" stream
                     "attributes" attrs}
              (some? toxicity) (assoc "toxicity" (double toxicity)))))))
