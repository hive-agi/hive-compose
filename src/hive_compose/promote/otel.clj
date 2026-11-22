(ns hive-compose.promote.otel
  "Pure: the built-in otel capability and the OTLP environment a consumer is
   given so its telemetry is correlated by run id (and test id).

   Services integrate through the OpenTelemetry protocol only: the standard
   OTEL_* environment variables every OTel SDK and Beyla read. No hive code
   ships into a service.

   OTEL_RESOURCE_ATTRIBUTES is `k1=v1,k2=v2`, each key and value
   percent-encoded (W3C baggage encoding, as the OTel SDK environment spec
   prescribes), so a value holding `,` `=` `%` or a space survives the trip."
  (:require [clojure.string :as str])
  (:import (java.nio.charset StandardCharsets)))

;; SPDX-License-Identifier: MIT

;; ---------------------------------------------------------------------------
;; Provider data

(def lgtm-version
  "The pinned grafana/otel-lgtm tag of resources/hive-compose/otel/compose.yml."
  "0.41.1")

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

;; TODO(hc-2): conform to hive-compose.schema :project/provides once lane core
;; lands it. Until then this is plain data in the plan's provides shape:
;; {capability {:version .. :service ..}} plus what this provider adds.
(def provider
  "The built-in otel capability: grafana/otel-lgtm as service `lgtm`."
  {"otel" {:version lgtm-version
           :service "lgtm"
           :image (str "grafana/otel-lgtm:" lgtm-version)
           :ports ports
           :compose/resource compose-resource
           :compose/profiles {"beyla" {:service "beyla"}}}})

;; ---------------------------------------------------------------------------
;; Percent encoding

(defn- unreserved?
  "RFC 3986 unreserved: the bytes left as they are."
  [b]
  (let [c (char b)]
    (or (<= (int \A) b (int \Z))
        (<= (int \a) b (int \z))
        (<= (int \0) b (int \9))
        (#{\- \. \_ \~} c))))

(defn percent-encode
  "`s` with every UTF-8 byte outside the RFC 3986 unreserved set as `%XX`
   (upper-case hex). Deterministic; `,` `=` `%` and whitespace never survive
   raw, so the result is safe as an OTEL_RESOURCE_ATTRIBUTES key or value."
  [s]
  (let [sb (StringBuilder.)]
    (doseq [b (.getBytes (str s) StandardCharsets/UTF_8)]
      (let [u (bit-and (int b) 0xff)]
        (if (unreserved? u)
          (.append sb (char u))
          (.append sb (format "%%%02X" u)))))
    (str sb)))

(defn- hex-digit [c]
  (Character/digit (char c) 16))

(defn percent-decode
  "Inverse of `percent-encode`. A `%` not followed by two hex digits is kept
   literally; `+` is a plus, not a space."
  [s]
  (let [s (str s)
        n (count s)
        out (java.io.ByteArrayOutputStream.)]
    (loop [i 0]
      (if (< i n)
        (let [c (.charAt s i)
              hi (when (< (inc i) n) (hex-digit (.charAt s (inc i))))
              lo (when (< (+ i 2) n) (hex-digit (.charAt s (+ i 2))))]
          (if (and (= c \%) hi lo (not= -1 hi) (not= -1 lo))
            (do (.write out (int (+ (* 16 hi) lo))) (recur (+ i 3)))
            (let [bs (.getBytes (str c) StandardCharsets/UTF_8)]
              (.write out bs 0 (alength bs))
              (recur (inc i)))))
        (String. (.toByteArray out) StandardCharsets/UTF_8)))))

;; ---------------------------------------------------------------------------
;; Resource attributes

(defn render-resource-attributes
  "OTEL_RESOURCE_ATTRIBUTES for `attrs`: a map (rendered in key order) or a
   sequence of [key value] pairs (rendered in that order). Keys and values are
   percent-encoded."
  [attrs]
  (let [pairs (if (map? attrs) (sort-by key attrs) attrs)]
    (str/join "," (map (fn [[k v]] (str (percent-encode k) "=" (percent-encode v)))
                       pairs))))

(defn parse-resource-attributes
  "{key value} of an OTEL_RESOURCE_ATTRIBUTES string; inverse of
   `render-resource-attributes`. Entries are trimmed; an entry without `=` is
   dropped (as OTel SDKs discard it); a later duplicate key wins."
  [s]
  (if (str/blank? s)
    {}
    (into {}
          (keep (fn [entry]
                  (let [entry (str/trim entry)
                        i (str/index-of entry "=")]
                    (when i
                      [(percent-decode (str/trim (subs entry 0 i)))
                       (percent-decode (str/trim (subs entry (inc i))))]))))
          (str/split s #"," -1))))

;; ---------------------------------------------------------------------------
;; OTLP environment

(def protocols
  "OTEL_EXPORTER_OTLP_PROTOCOL by protocol keyword."
  {:grpc "grpc"
   :http "http/protobuf"})

(defn default-endpoint
  "The local otel-lgtm OTLP endpoint for `protocol` (:grpc or :http)."
  [protocol]
  (str "http://localhost:" (if (= protocol :http) (:otlp-http ports) (:otlp-grpc ports))))

(def reserved-attributes
  "Resource attributes hive-compose sets itself; extra attributes never
   override them."
  #{"hive.run.id" "service.name" "hive.test.id"})

(defn resource-attributes
  "Ordered [key value] pairs: hive.run.id, service.name, hive.test.id (when
   given), then `extra-attrs` in key order, minus any reserved key."
  [{:keys [run-id service test-id extra-attrs]}]
  (concat [["hive.run.id" (str run-id)]
           ["service.name" (str service)]]
          (when (some? test-id) [["hive.test.id" (str test-id)]])
          (->> extra-attrs
               (map (fn [[k v]] [(if (keyword? k) (name k) (str k)) (str v)]))
               (remove (comp reserved-attributes first))
               (sort-by first))))

(defn otlp-env
  "The OTLP environment variables for one consumer.

   `opts`: :run-id, :service (OTEL_SERVICE_NAME), optional :test-id,
   :endpoint (default the local otel-lgtm port of :protocol), :protocol
   (:grpc, default, or :http) and :extra-attrs {key value}."
  [{:keys [service endpoint protocol] :as opts}]
  (let [protocol (or protocol :grpc)]
    {"OTEL_EXPORTER_OTLP_ENDPOINT" (or endpoint (default-endpoint protocol))
     "OTEL_EXPORTER_OTLP_PROTOCOL" (get protocols protocol "grpc")
     "OTEL_SERVICE_NAME" (str service)
     "OTEL_RESOURCE_ATTRIBUTES" (render-resource-attributes (resource-attributes opts))}))

(defn merge-env
  "`injected` (an OTLP env) under `consumer`: additive, the consumer's value
   wins for every variable it sets, byte for byte (a secret reference
   included), so a user's own OTEL_* is never clobbered; injected variables
   the consumer does not set are added."
  [consumer injected]
  (merge injected consumer))
