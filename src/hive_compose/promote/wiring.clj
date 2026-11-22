(ns hive-compose.promote.wiring
  "Pure: how each consumer of a stack run is wired to the instances it
   resolved (hc-5, decision 20261122141106-2d375c4f).

   Network aliases model: the consumer attaches only to its view network
   `hive-<run>-<consumer>` and every bound instance joins it under its
   original service names, so a provider's `:env` (`KAFKA_BROKERS=kafka:9092`)
   is right as written and is never rewritten.

   The env a consumer receives, layered low to high:
   1. injected: each bound provider's `:env` (by capability; two instances
      setting one key to different values is a Result error naming both and
      the key), then for each provider with `:tenancy {:env VAR}` VAR = the
      consumer's tenancy prefix `<run>.<consumer>.`, then `extra-envs`, a seq
      of Env maps in order (the OTLP hook: `hive-compose.promote.otel`).
   2. the consumer's own env (`consumer-env`), per key.
   3. forced: HIVE_RUN_ID=<run> and hive.run.id=<run>.
   Throughout, OTEL_EXPORTER_OTLP_ENDPOINT and OTEL_EXPORTER_OTLP_PROTOCOL are
   one unit (a map naming either replaces both; a result with only one is an
   error), OTEL_RESOURCE_ATTRIBUTES is merged per key (comma-separated k=v,
   later wins), and finally hive.run.id=<run> and HIVE_RUN_ID=<run> are
   forced.

   Secret references (`{:pass path}`) stay references: they are read only at
   spawn (`hive-compose.adapter.secrets`); show a Wiring through
   `hive-compose.promote.env/rendered`."
  (:require [clojure.string :as str]
            [hive-compose.promote.env :as env]
            [hive-compose.promote.names :as names]
            [hive-compose.schema :as schema]
            [hive-dsl.result :as r]
            [malli.core :as m]))

;; SPDX-License-Identifier: MIT

(def run-id-var "HIVE_RUN_ID")

(def otlp-pair
  "Keys that only travel together: an endpoint without its protocol (or the
   reverse) makes an SDK guess, so they are set and replaced as one unit."
  ["OTEL_EXPORTER_OTLP_ENDPOINT" "OTEL_EXPORTER_OTLP_PROTOCOL"])

(def resource-attributes-var "OTEL_RESOURCE_ATTRIBUTES")

(def run-attribute "hive.run.id")

;; ---------------------------------------------------------------------------
;; OTEL_RESOURCE_ATTRIBUTES: comma-separated k=v, merged per key.

(defn parse-attributes
  "`s` (`k1=v1,k2=v2`) as a vector of [k v] in order. Blank entries and
   entries without `=` are dropped; a repeated key keeps its last value."
  [s]
  (reduce (fn [acc part]
            (let [part (str/trim part)
                  i (str/index-of part "=")]
              (if (or (nil? i) (zero? i))
                acc
                (let [k (str/trim (subs part 0 i))
                      v (str/trim (subs part (inc i)))
                      at (first (keep-indexed (fn [n [k' _]] (when (= k k') n)) acc))]
                  (if at (assoc acc at [k v]) (conj acc [k v]))))))
          []
          (str/split (or s "") #",")))

(defn merge-attributes
  "The attribute strings `ss` merged per key, later wins, first position of a
   key kept: (merge-attributes [\"a=1,b=2\" \"b=3\"]) => \"a=1,b=3\"."
  [ss]
  (->> ss
       (map parse-attributes)
       (reduce (fn [acc kvs]
                 (reduce (fn [acc [k v]]
                           (let [at (first (keep-indexed (fn [n [k' _]] (when (= k k') n)) acc))]
                             (if at (assoc acc at [k v]) (conj acc [k v]))))
                         acc kvs))
               [])
       (map (fn [[k v]] (str k "=" v)))
       (str/join ",")))

;; ---------------------------------------------------------------------------
;; Merge of Env maps with the OTLP rules.

(defn- merge-env
  "`acc` with `e` merged over it, `e` winning, except that the OTLP pair is
   replaced as a unit and resource attributes accumulate per key in
   `::attributes` (rendered at the end)."
  [acc e]
  (let [pair? (some #(contains? e %) otlp-pair)
        attrs (get e resource-attributes-var)
        plain (apply dissoc e resource-attributes-var otlp-pair)]
    (cond-> (merge acc plain)
      pair? (as-> a (merge (apply dissoc a otlp-pair) (select-keys e otlp-pair)))
      (some? attrs) (update ::attributes (fnil conj []) attrs))))

(defn- finish-env
  "Close `acc`: render the attributes with hive.run.id forced, force
   HIVE_RUN_ID, check the OTLP pair."
  [acc run]
  (let [attrs (::attributes acc)
        e (dissoc acc ::attributes)
        present (filter #(contains? e %) otlp-pair)]
    (cond
      (some env/ref? attrs)
      (r/err :compose/otlp-attributes-ref {:key resource-attributes-var})

      (= 1 (count present))
      (r/err :compose/otlp-pair-broken {:present (vec present) :pair otlp-pair})

      :else
      (r/ok (assoc e
                   resource-attributes-var (merge-attributes (conj (vec attrs) (str run-attribute "=" run)))
                   run-id-var run)))))

;; ---------------------------------------------------------------------------
;; Consumers, needs and bound instances.

(defn consumer-needs
  "The Needs a consumer declared: a project id or a section `<project>/<target>`
   reads its project's `:project/needs`, a suite `suite:<id>` its `:suite/needs`."
  [settings consumer]
  (if (str/starts-with? consumer "suite:")
    (get-in settings [:compose/suites (subs consumer 6) :suite/needs] {})
    (get-in settings [:compose/projects (first (str/split consumer #"/" 2)) :project/needs] {})))

(defn consumer-env
  "The consumer's own Env: `:suite/env` of a suite `suite:<id>`, else
   `:project/env` of its project. It overlays the injected env (layer 2)."
  [settings consumer]
  (if (str/starts-with? consumer "suite:")
    (get-in settings [:compose/suites (subs consumer 6) :suite/env] {})
    (get-in settings [:compose/projects (first (str/split consumer #"/" 2)) :project/env] {})))

(defn- serves?
  "True when `service` of `consumer` gets the wiring of capability `cap`:
   always when `service` is nil or the Need names no `:services`."
  [settings consumer cap service]
  (let [only (get-in (consumer-needs settings consumer) [cap :services])]
    (or (nil? service) (empty? only) (some #{service} only))))

(defn- bound
  "[[cap instance] ...] of `consumer`, by capability, or a Result error for an
   instance id the resolution does not hold."
  [resolution consumer]
  (let [instances (:resolution/instances resolution)
        binding (sort-by key (get-in resolution [:resolution/bindings consumer]))
        missing (remove #(contains? instances (val %)) binding)]
    (if (seq missing)
      (r/err :compose/unknown-instance {:consumer consumer
                                        :instances (mapv val missing)})
      (r/ok (mapv (fn [[cap id]] [cap (get instances id)]) binding)))))

(defn- provide
  "The Provide an instance answers with, {} when its provider is not a
   configured project (a built-in provider such as otel wires through
   extra-envs)."
  [settings inst]
  (get-in settings [:compose/projects (:instance/provider inst)
                    :project/provides (:instance/capability inst)] {}))

(defn- providers-env
  "The merged `:env` of `insts`, or an error on a key two instances set to
   different values. Keys owned by the OTLP rules are left to merge-env."
  [settings insts]
  (reduce (fn [acc inst]
            (let [e (:env (provide settings inst) {})
                  clash (first (for [[k v] (apply dissoc e resource-attributes-var otlp-pair)
                                     :let [[owner v'] (get (:owners acc) k)]
                                     :when (and owner (not= owner (:instance/id inst)) (not= v v'))]
                                 [k owner]))]
              (if clash
                (reduced (r/err :compose/env-collision
                                {:key (first clash)
                                 :instances [(second clash) (:instance/id inst)]}))
                (-> acc
                    (update :env merge-env e)
                    (update :owners into (map (fn [[k v]] [k [(:instance/id inst) v]])) e)))))
          {:env {} :owners {}}
          insts))

(defn- attach
  "One attach entry per distinct bound instance, by instance id."
  [insts]
  (->> insts
       (map (juxt :instance/id :instance/aliases))
       (into (sorted-map))
       (mapv (fn [[id aliases]] {:instance id :aliases (vec aliases)}))))

(defn- wire
  "The Wiring of `consumer`, its env restricted to the capabilities whose Need
   serves `service` (all when nil)."
  [settings resolution run consumer extra-envs service]
  (if-not (m/validate schema/RunId run)
    (r/err :compose/invalid-run {:run run})
    (r/let-ok [binding (bound resolution consumer)]
      (let [prefix (names/tenancy-prefix run consumer)
            served (->> binding
                        (filter (fn [[cap _]] (serves? settings consumer cap service)))
                        (map second))
            tenancy (into {} (keep (fn [inst]
                                     (when-let [var (get-in (provide settings inst) [:tenancy :env])]
                                       [var prefix])))
                          served)]
        (r/let-ok [provided (let [p (providers-env settings served)]
                              (if (r/err? p) p (r/ok (:env p))))
                   e (finish-env (reduce merge-env provided
                                         (concat [tenancy] extra-envs [(consumer-env settings consumer)]))
                                 run)]
          (let [w {:wiring/consumer consumer
                   :wiring/run run
                   :wiring/network (names/view-network run consumer)
                   :wiring/attach (attach (map second binding))
                   :wiring/env e
                   :wiring/prefix prefix}]
            (if-let [problems (schema/explain schema/Wiring w)]
              (r/err :compose/invalid-wiring {:consumer consumer :problems problems})
              (r/ok w))))))))

(defn wiring
  "Result<Wiring> of `consumer` in `run`: its view network, the instances it
   is bound to in `resolution` with their aliases, its env (see the ns doc)
   and its tenancy prefix. `extra-envs` is a seq of Env maps merged in
   order over the providers' env and under the consumer's own env."
  [settings resolution run consumer & [extra-envs]]
  (wire settings resolution run consumer extra-envs nil))

(defn service-env
  "Result<Env> `service` of `consumer` receives: the Wiring env restricted to
   the capabilities whose Need lists `service` in `:services` (or lists none).
   HIVE_RUN_ID, resource attributes and `extra-envs` reach every service."
  [settings resolution run consumer service & [extra-envs]]
  (r/let-ok [w (wire settings resolution run consumer extra-envs service)]
    (r/ok (:wiring/env w))))

(defn wirings
  "Result<{consumer Wiring}> for every consumer bound in `resolution`.
   `extra-envs-fn`, (fn [consumer] [Env ...]), supplies each one's extra envs."
  [settings resolution run & [extra-envs-fn]]
  (let [f (or extra-envs-fn (constantly nil))]
    (reduce (fn [acc consumer]
              (let [w (wiring settings resolution run consumer (f consumer))]
                (if (r/err? w) (reduced w) (r/ok (assoc (:ok acc) consumer (:ok w))))))
            (r/ok {})
            (sort (keys (:resolution/bindings resolution))))))
