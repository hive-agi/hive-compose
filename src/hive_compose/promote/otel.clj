(ns hive-compose.promote.otel
  "Pure: the built-in otel capability and the OTLP environment a consumer is
   given so its telemetry is correlated by run id (and test id).

   Services integrate through the OpenTelemetry protocol only: the standard
   OTEL_* environment variables every OTel SDK and Beyla read. No hive code
   ships into a service.

   OTEL_RESOURCE_ATTRIBUTES is `k1=v1,k2=v2` as the OTel SDK environment spec
   prescribes: keys are W3C baggage tokens and are written as they are (an SDK
   never decodes a key), values are percent-encoded, so a value holding `,`
   `=` `%` or a space survives the trip.

   The otel instance joins every consumer's view network `hive-<run>-<consumer>`
   under its service name `otel` (hc-5 :wiring/attach), so the container-side
   endpoint is `http://otel:4317`; a host process reaches the published port
   on localhost."
  (:require [clojure.string :as str])
  (:import (java.io ByteArrayOutputStream)
           (java.nio.charset StandardCharsets)))

;; SPDX-License-Identifier: MIT

;; ---------------------------------------------------------------------------
;; Provider data

(def lgtm-version
  "The grafana/otel-lgtm tag resources/hive-compose/otel/compose.yml pins."
  "0.41.1")

(def beyla-version
  "The grafana/beyla tag of the fragment's optional `beyla` service."
  "3.43.0")

(def service
  "The compose service answering the otel capability, and the alias it joins
   consumer view networks under."
  "otel")

(def ports
  "Container ports of the otel-lgtm image, by role."
  {:otlp-grpc 4317
   :otlp-http 4318
   :grafana 3000
   :tempo 3200
   :loki 3100
   :prometheus 9090})

(def compose-resource
  "Classpath resource of the compose fragment running the otel provider."
  "hive-compose/otel/compose.yml")

;; TODO(hc-2): conform to hive-compose.schema/Provide once feat/hc-2-stack-schema
;; lands on staging. Plain data in the plan's provides shape until then:
;; {capability {:service .. :version ..}} plus what this provider adds.
(def provider
  "The built-in otel capability: grafana/otel-lgtm as service `otel`."
  {"otel" {:service service
           :version lgtm-version
           :image (str "grafana/otel-lgtm:" lgtm-version)
           :ports ports
           :otel/aliases [service]
           :compose/resource compose-resource
           :compose/profiles {"beyla" {:service "beyla"
                                       :image (str "grafana/beyla:" beyla-version)}}}})

;; ---------------------------------------------------------------------------
;; Percent encoding of values

(defn- unreserved?
  "RFC 3986 unreserved: the bytes left as they are."
  [b]
  (or (<= (int \A) b (int \Z))
      (<= (int \a) b (int \z))
      (<= (int \0) b (int \9))
      (contains? #{(int \-) (int \.) (int \_) (int \~)} b)))

(defn percent-encode
  "`s` with every UTF-8 byte outside the RFC 3986 unreserved set as `%XX`
   (upper-case hex): a superset of what W3C baggage requires of a value, and
   deterministic. `,` `=` `%` and whitespace never survive raw."
  [s]
  (let [sb (StringBuilder.)]
    (doseq [b (.getBytes (str s) StandardCharsets/UTF_8)]
      (let [u (bit-and (int b) 0xff)]
        (if (unreserved? u)
          (.append sb (char u))
          (.append sb (format "%%%02X" u)))))
    (str sb)))

(defn- hex-at
  "The value of the hex digit at `i` of `s`, nil when there is none."
  [^String s i]
  (when (< i (count s))
    (let [d (Character/digit (.charAt s (int i)) 16)]
      (when-not (neg? d) d))))

(defn percent-decode
  "Inverse of `percent-encode`. Runs of `%XX` escapes are decoded as UTF-8;
   every other character (astral ones included) is copied as it is; a `%`
   not followed by two hex digits stays literal; `+` is a plus."
  [s]
  (let [s (str s)
        n (count s)
        sb (StringBuilder.)
        bytes (ByteArrayOutputStream.)
        flush! (fn []
                 (when (pos? (.size bytes))
                   (.append sb (String. (.toByteArray bytes) StandardCharsets/UTF_8))
                   (.reset bytes)))]
    (loop [i 0]
      (if (< i n)
        (let [hi (when (= \% (.charAt s i)) (hex-at s (inc i)))
              lo (when hi (hex-at s (+ i 2)))]
          (if lo
            (do (.write bytes (int (+ (* 16 hi) lo))) (recur (+ i 3)))
            (do (flush!) (.append sb (.charAt s i)) (recur (inc i)))))
        (do (flush!) (str sb))))))

;; ---------------------------------------------------------------------------
;; Resource attributes

(defn valid-key?
  "True when `k` can be an OTEL_RESOURCE_ATTRIBUTES key: a non-empty W3C
   baggage token (RFC 7230 tchar) without `%`, which a parser would read as
   an escape."
  [k]
  (boolean (and (string? k) (re-matches #"[A-Za-z0-9!#$&'*+\-.^_`|~]+" k))))

(defn correlation-attribute?
  "True for the resource attributes hive-compose correlates a run by, the
   `hive.*` keys (hive.run.id, hive.test.id): an injected value always wins
   for them, a run's telemetry must carry its own run id."
  [k]
  (str/starts-with? (str k) "hive."))

(def ^:private head-order
  "Keys rendered first, in this order; the rest follow in key order."
  ["hive.run.id" "service.name" "hive.test.id"])

(defn- ordered-pairs
  "[k v] pairs of map `attrs`: `head-order` keys first, then the rest by key."
  [attrs]
  (let [head (set head-order)]
    (concat (keep (fn [k] (when (contains? attrs k) [k (get attrs k)])) head-order)
            (sort-by key (remove (comp head key) attrs)))))

(defn render-resource-attributes
  "OTEL_RESOURCE_ATTRIBUTES of `attrs` {key value}: keys raw, values
   percent-encoded, hive.run.id / service.name / hive.test.id first and the
   rest in key order. Throws ex-info naming the keys that are not
   `valid-key?`."
  [attrs]
  (let [attrs (into {} (map (fn [[k v]] [(str k) (str v)])) attrs)
        bad (sort (remove valid-key? (keys attrs)))]
    (when (seq bad)
      (throw (ex-info (str "invalid OTEL_RESOURCE_ATTRIBUTES keys: " (str/join ", " bad))
                      {:compose/error :compose/otel-invalid-attribute-key :keys (vec bad)})))
    (str/join "," (map (fn [[k v]] (str k "=" (percent-encode v))) (ordered-pairs attrs)))))

(defn parse-resource-attributes
  "{key value} of an OTEL_RESOURCE_ATTRIBUTES string; inverse of
   `render-resource-attributes`. Entries are trimmed; an entry without `=`
   or with an empty key is dropped (as OTel SDKs discard it); values are
   percent-decoded; a later duplicate key wins."
  [s]
  (if (str/blank? s)
    {}
    (into {}
          (keep (fn [entry]
                  (let [entry (str/trim entry)
                        i (str/index-of entry "=")
                        k (when i (str/trim (subs entry 0 i)))]
                    (when (seq k)
                      [k (percent-decode (str/trim (subs entry (inc i))))]))))
          (str/split s #","))))

(defn- entries
  "[key entry] of each well-formed entry of an OTEL_RESOURCE_ATTRIBUTES
   string, the entry trimmed but otherwise verbatim (still encoded)."
  [s]
  (keep (fn [e]
          (let [e (str/trim e)
                j (str/index-of e "=")
                k (when j (str/trim (subs e 0 j)))]
            (when (seq k) [k e])))
        (str/split (str s) #",")))

(defn merge-resource-attributes
  "OTEL_RESOURCE_ATTRIBUTES of `base` with `over` merged in per key (both
   strings, nil for none): `over` wins for every key it sets, a key keeps
   the position it first had, new keys follow in `over`'s order, a later
   duplicate wins. Entries are moved verbatim, never re-encoded, so a user's
   attributes survive byte for byte and nothing throws. Malformed entries
   (no `=`, empty key) are dropped, as SDKs drop them.

   The hc-5 layering forces the correlation keys with it:
   (merge-resource-attributes consumer-ra (render-resource-attributes
     (select-keys injected-attrs (filter correlation-attribute? ...))))."
  [base over]
  (let [all (concat (entries base) (entries over))
        order (distinct (map first all))
        latest (into {} all)]
    (str/join "," (map latest order))))

;; ---------------------------------------------------------------------------
;; OTLP environment

(def protocols
  "OTEL_EXPORTER_OTLP_PROTOCOL by protocol keyword."
  {:grpc "grpc"
   :http "http/protobuf"})

(defn endpoint
  "The OTLP endpoint of the otel instance for `protocol` (:grpc or :http) seen
   from `side`: :container (the consumer's view network, alias `otel`) or
   :host (the published port on localhost)."
  [side protocol]
  (str "http://" (if (= side :host) "localhost" service) ":"
       (if (= protocol :http) (:otlp-http ports) (:otlp-grpc ports))))

(defn- attr-key
  "An extra attribute key as a string: a keyword's namespace joined by `.`
   (`:deployment/environment` is `deployment.environment`)."
  [k]
  (if (keyword? k)
    (if-let [n (namespace k)] (str n "." (name k)) (name k))
    (str k)))

(defn- fail [msg data]
  (throw (ex-info msg (assoc data :compose/error :compose/otel-invalid-options))))

(defn resource-attributes
  "{key value}: hive.run.id, service.name, hive.test.id (when given) and
   `extra-attrs`; an extra key never overrides those three. Throws ex-info
   when two extra keys name the same attribute once normalised."
  [{:keys [run-id service test-id extra-attrs]}]
  (let [extra (map (fn [[k v]] [(attr-key k) (str v)]) extra-attrs)
        dups (sort (keep (fn [[k n]] (when (< 1 n) k)) (frequencies (map first extra))))]
    (when (seq dups)
      (fail (str "extra attributes name the same key twice: " (str/join ", " dups))
            {:keys (vec dups)}))
    (merge (into {} extra)
           {"hive.run.id" (str run-id) "service.name" (str service)}
           (when (some? test-id) {"hive.test.id" (str test-id)}))))

(defn otlp-env
  "The OTLP environment variables for one consumer.

   `opts`: :run-id and :service (OTEL_SERVICE_NAME) required, optional
   :test-id, :protocol (:grpc, default, or :http), :side (:container,
   default, or :host) or an explicit :endpoint, and :extra-attrs {key value}.
   Throws ex-info on a blank run id or service or an unknown protocol."
  [{:keys [run-id service protocol side] :as opts}]
  (let [protocol (or protocol :grpc)
        side (or side :container)]
    (when (str/blank? (str run-id)) (fail "otlp-env needs a :run-id" {:opts opts}))
    (when (str/blank? (str service)) (fail "otlp-env needs a :service" {:opts opts}))
    (when-not (contains? protocols protocol)
      (fail (str "unknown OTLP protocol " protocol) {:protocol protocol :known (keys protocols)}))
    (when-not (#{:container :host} side)
      (fail (str "unknown side " side) {:side side}))
    {"OTEL_EXPORTER_OTLP_ENDPOINT" (or (:endpoint opts) (endpoint side protocol))
     "OTEL_EXPORTER_OTLP_PROTOCOL" (protocols protocol)
     "OTEL_SERVICE_NAME" (str service)
     "OTEL_RESOURCE_ATTRIBUTES" (render-resource-attributes (resource-attributes opts))}))

(def exporter-pair
  "Endpoint and protocol are one unit: an endpoint means nothing without the
   protocol it speaks."
  ["OTEL_EXPORTER_OTLP_ENDPOINT" "OTEL_EXPORTER_OTLP_PROTOCOL"])

(defn otlp-pair-merge
  "`over` merged onto `base` per key, with endpoint and protocol as one unit:
   when `over` names either, both of `base`'s are dropped first, so an
   endpoint never ends up beside a protocol it does not speak. The env
   layering itself (injected < consumer < forced) is hc-5 wiring's; this is
   the rule it applies at each layer."
  [base over]
  (merge (if (some #(contains? over %) exporter-pair)
           (apply dissoc base exporter-pair)
           base)
         over))

(defn otlp-pair-broken?
  "True when `env` names exactly one of endpoint and protocol."
  [env]
  (= 1 (count (filter #(contains? env %) exporter-pair))))
