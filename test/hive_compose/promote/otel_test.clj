(ns hive-compose.promote.otel-test
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.promote.otel :as otel]))

;; SPDX-License-Identifier: MIT

(def gen-astral
  "A supplementary-plane character: a surrogate pair in a Java string."
  (gen/fmap #(String. (Character/toChars (int %))) (gen/choose 0x10000 0x10FFFF)))

(def gen-piece
  "One character of text, as a string. The separators the encoding must
   survive are weighted in, BMP and astral characters both appear, and lone
   surrogates (not representable in UTF-8) never do."
  (gen/frequency [[3 (gen/fmap str (gen/elements [\, \= \% \space \tab \newline \+ \; \" \\]))]
                  [4 (gen/fmap str gen/char)]
                  [2 (gen/fmap (comp str char) (gen/one-of [(gen/choose 0x100 0xD7FF)
                                                            (gen/choose 0xE000 0xFFFD)]))]
                  [1 gen-astral]]))

(def gen-text (gen/fmap str/join (gen/vector gen-piece 0 12)))

(def gen-key
  "A valid OTEL_RESOURCE_ATTRIBUTES key (W3C baggage token)."
  (gen/fmap str/join
            (gen/vector (gen/elements (seq "abcXYZ019.-_!#$&'*+^`|~")) 1 10)))

(def gen-attrs (gen/map gen-key gen-text {:max-elements 8}))

;; --- otlp-env

(deftest otlp-env-shape
  (let [env (otel/otlp-env {:run-id "r-1" :service "shop api" :test-id "t,1"
                            :protocol :http
                            :extra-attrs {:deployment/environment "e2e" "z" "a=b"}})]
    (is (= #{"OTEL_EXPORTER_OTLP_ENDPOINT" "OTEL_EXPORTER_OTLP_PROTOCOL"
             "OTEL_SERVICE_NAME" "OTEL_RESOURCE_ATTRIBUTES"}
           (set (keys env))))
    (is (= "http://otel:4318" (env "OTEL_EXPORTER_OTLP_ENDPOINT"))
        "containers reach the otel alias on their view network")
    (is (= "http/protobuf" (env "OTEL_EXPORTER_OTLP_PROTOCOL")))
    (is (= "shop api" (env "OTEL_SERVICE_NAME")))
    (is (= (str "hive.run.id=r-1,service.name=shop%20api,hive.test.id=t%2C1,"
                "deployment.environment=e2e,z=a%3Db")
           (env "OTEL_RESOURCE_ATTRIBUTES"))
        "run id, service, test id first; extras in key order; values encoded")))

(deftest otlp-env-defaults-and-sides
  (let [env (otel/otlp-env {:run-id "r01" :service "s"})]
    (is (= "grpc" (env "OTEL_EXPORTER_OTLP_PROTOCOL")))
    (is (= "http://otel:4317" (env "OTEL_EXPORTER_OTLP_ENDPOINT")))
    (is (= "hive.run.id=r01,service.name=s" (env "OTEL_RESOURCE_ATTRIBUTES"))))
  (is (= "http://127.0.0.1:4318"
         ((otel/otlp-env {:run-id "r01" :service "s" :protocol :http :side :host})
          "OTEL_EXPORTER_OTLP_ENDPOINT")))
  (is (= "http://x:1"
         ((otel/otlp-env {:run-id "r01" :service "s" :endpoint "http://x:1"})
          "OTEL_EXPORTER_OTLP_ENDPOINT"))))

(deftest otlp-env-refuses-bad-options
  (doseq [opts [{:service "s"} {:run-id "r01"} {:run-id "" :service "s"}
                {:run-id "r01" :service "s" :protocol :https}
                {:run-id "r01" :service "s" :side :moon}
                {:run-id "r01" :service "s" :extra-attrs {:a "1" "a" "2"}}
                {:run-id "r01" :service "s" :extra-attrs {"bad key" "1"}}]]
    (is (thrown? clojure.lang.ExceptionInfo (otel/otlp-env opts)) (pr-str opts))))

(deftest extra-attributes-never-override-reserved
  (is (= {"hive.run.id" "r01" "service.name" "s"}
         (otel/parse-resource-attributes
          ((otel/otlp-env {:run-id "r01" :service "s"
                           :extra-attrs {"hive.run.id" "evil" "service.name" "x"}})
           "OTEL_RESOURCE_ATTRIBUTES")))))

(defspec otlp-env-carries-the-run-id 200
  (prop/for-all [run-id (gen/such-that (complement str/blank?) gen-text)
                 service (gen/such-that (complement str/blank?) gen-text)
                 test-id (gen/one-of [(gen/return nil) gen-text])
                 extra gen-attrs]
    (let [attrs (otel/parse-resource-attributes
                 ((otel/otlp-env {:run-id run-id :service service :test-id test-id
                                  :extra-attrs extra})
                  "OTEL_RESOURCE_ATTRIBUTES"))]
      (and (= run-id (attrs "hive.run.id"))
           (= service (attrs "service.name"))
           (= test-id (attrs "hive.test.id"))
           (every? (fn [[k v]] (or (#{"hive.run.id" "service.name" "hive.test.id"} k)
                                   (= v (attrs k))))
                   extra)))))

;; --- codec

(deftest percent-encoding
  (is (= "a%2Cb%3Dc%25d%20e" (otel/percent-encode "a,b=c%d e")))
  (is (= "%C3%A9" (otel/percent-encode "é")))
  (is (= "a,b=c%d e" (otel/percent-decode "a%2Cb%3Dc%25d%20e")))
  (is (= "100%" (otel/percent-decode "100%")) "a dangling % stays literal")
  (is (= "%zz+" (otel/percent-decode "%zz+")) "invalid escape stays, + is a plus")
  (is (= "a😀b" (otel/percent-decode "a😀b")) "raw astral characters survive")
  (is (= "😀é" (otel/percent-decode "%F0%9F%98%80%C3%A9"))))

(deftest render-validates-keys
  (is (thrown? clojure.lang.ExceptionInfo (otel/render-resource-attributes {"a b" "1"})))
  (is (thrown? clojure.lang.ExceptionInfo (otel/render-resource-attributes {"" "1"})))
  (is (thrown? clojure.lang.ExceptionInfo (otel/render-resource-attributes {"a%2F" "1"})))
  (is (thrown? clojure.lang.ExceptionInfo (otel/render-resource-attributes {"a/b" "1"}))
      "`/` is not a token char: refused, never encoded")
  (is (= "%٣٣" (otel/percent-decode "%٣٣")) "only ASCII hex digits form an escape"))

(deftest extra-hive-attributes-are-dropped
  (is (= {"hive.run.id" "r01" "service.name" "s" "x" "1"}
         (otel/parse-resource-attributes
          ((otel/otlp-env {:run-id "r01" :service "s"
                           :extra-attrs {"hive.test.id" "spoof" "hive.other" "y" "x" "1"}})
           "OTEL_RESOURCE_ATTRIBUTES")))))

(deftest parse-tolerates-sdk-style-input
  (is (= {} (otel/parse-resource-attributes nil)))
  (is (= {} (otel/parse-resource-attributes "")))
  (is (= {"a" "1" "b" "x y"}
         (otel/parse-resource-attributes " a = 1 ,junk,=v, b=x%20y,"))
      "entries trimmed; no = or an empty key dropped"))

(defspec parse-inverts-render 300
  (prop/for-all [attrs gen-attrs]
    (= attrs (otel/parse-resource-attributes (otel/render-resource-attributes attrs)))))

(defspec parse-reads-raw-safe-text 200
  (prop/for-all [k gen-key
                 ;; raw: no separators, nothing str/trim (Java whitespace) eats
                 v (gen/fmap #(str/join (remove (fn [c] (or (#{\, \%} c) (Character/isWhitespace (char c))))
                                                %))
                             gen-text)]
    (= {k v} (otel/parse-resource-attributes (str k "=" v)))))

(defspec render-is-deterministic-and-separator-safe 200
  (prop/for-all [pairs (gen/vector (gen/tuple gen-key gen-text) 0 8)]
    (let [a (into {} pairs)
          b (into (sorted-map-by #(compare %2 %1)) pairs)
          s (otel/render-resource-attributes a)]
      (and (= s (otel/render-resource-attributes b))
           (= (count a) (if (empty? a) 0 (count (str/split s #","))))
           (not (re-find #"\s" s))
           (not (re-find #"%(?![0-9A-F]{2})" s))))))

;; --- RA per-key merge and the OTLP pair rule (the env layering is hc-5's)

(defspec merge-resource-attributes-is-per-key-over-wins 300
  (prop/for-all [base gen-attrs over gen-attrs]
    (= (merge base over)
       (otel/parse-resource-attributes
        (otel/merge-resource-attributes (otel/render-resource-attributes base)
                                        (otel/render-resource-attributes over))))))

(defspec merge-resource-attributes-keeps-entries-verbatim 200
  (prop/for-all [base gen-attrs over gen-attrs]
    (let [entries (fn [s] (set (remove str/blank? (str/split (str s) #","))))
          b (otel/render-resource-attributes base)
          o (otel/render-resource-attributes over)
          out (entries (otel/merge-resource-attributes b o))]
      (and (every? out (entries o))
           (every? (fn [e] (or (out e) (contains? over (first (str/split e #"=" 2)))))
                   (entries b))))))

(defspec merge-resource-attributes-is-idempotent 200
  (prop/for-all [base gen-attrs over gen-attrs]
    (let [b (otel/render-resource-attributes base)
          o (otel/render-resource-attributes over)
          once (otel/merge-resource-attributes b o)]
      (= once (otel/merge-resource-attributes once o)))))

(deftest merge-resource-attributes-examples
  (is (= "team=a%2Cb,hive.run.id=run-1,x=1"
         (otel/merge-resource-attributes "team=a%2Cb,hive.run.id=fake,junk"
                                         "hive.run.id=run-1,x=1"))
      "over wins in place, new keys appended, malformed dropped, encoding kept")
  (is (= "a=1" (otel/merge-resource-attributes nil "a=1")))
  (is (= "a=1" (otel/merge-resource-attributes "a=1" nil)))
  (is (otel/correlation-attribute? "hive.run.id"))
  (is (otel/correlation-attribute? "hive.test.id"))
  (is (not (otel/correlation-attribute? "service.name"))))

(def gen-env-key
  (gen/one-of [(gen/elements ["OTEL_EXPORTER_OTLP_ENDPOINT" "OTEL_EXPORTER_OTLP_PROTOCOL"
                              "OTEL_SERVICE_NAME" "PATH"])
               (gen/fmap #(str "X_" %) gen/string-alphanumeric)]))

(def gen-env-val
  (gen/one-of [gen-text (gen/fmap (fn [p] {:pass p}) gen/string-alphanumeric)]))

(def gen-env (gen/map gen-env-key gen-env-val {:max-elements 6}))

(defspec otlp-pair-merge-keeps-the-pair-together 300
  (prop/for-all [base gen-env over gen-env]
    (let [m (otel/otlp-pair-merge base over)
          owner (if (some #(contains? over %) otel/exporter-pair) over base)]
      (and (every? (fn [[k v]] (= v (m k))) over)
           (every? (fn [k] (= (get owner k ::none) (get m k ::none))) otel/exporter-pair)
           (every? (fn [[k v]] (or (contains? over k) (some #{k} otel/exporter-pair)
                                   (= v (m k))))
                   base)
           (= (otel/otlp-pair-broken? owner) (otel/otlp-pair-broken? m))))))

(deftest otlp-pair-examples
  (let [inj (otel/otlp-env {:run-id "run-1" :service "svc"})
        m (otel/otlp-pair-merge inj {"OTEL_EXPORTER_OTLP_ENDPOINT" "http://mine:4318"})]
    (is (= "http://mine:4318" (m "OTEL_EXPORTER_OTLP_ENDPOINT")))
    (is (not (contains? m "OTEL_EXPORTER_OTLP_PROTOCOL"))
        "an endpoint never inherits a protocol it was not set with")
    (is (otel/otlp-pair-broken? m))
    (is (not (otel/otlp-pair-broken? inj)))
    (is (not (otel/otlp-pair-broken? {})))))

;; --- provider data and the compose fragment

(deftest provider-data
  (let [{:keys [version service ports image] :as p} (otel/provider "otel")]
    (is (= "otel" service))
    (is (= ["otel"] (:otel/aliases p)))
    (is (= otel/lgtm-version version))
    (is (= (str "grafana/otel-lgtm:" version) image))
    (is (= {:otlp-grpc 4317 :otlp-http 4318 :grafana 3000 :tempo 3200
            :loki 3100 :prometheus 9090}
           ports))))

(deftest compose-resource-names-otel-lgtm
  (let [res (io/resource otel/compose-resource)]
    (is (some? res) "the fragment is on the classpath")
    (let [text (slurp res)]
      (testing "images pinned to the versions the provider data names"
        (is (str/includes? text (str "image: grafana/otel-lgtm:" otel/lgtm-version)))
        (is (str/includes? text (str "image: grafana/beyla:" otel/beyla-version)))
        (is (not (re-find #"otel-lgtm:latest|beyla:latest" text))))
      (testing "service otel, capability label, healthcheck, beyla behind its profile"
        (is (re-find #"(?m)^  otel:$" text))
        (is (str/includes? text "hive.capability: otel"))
        (is (str/includes? text "healthcheck:"))
        (is (re-find #"profiles: \[\"beyla\"\]" text))
        (is (str/includes? text "BEYLA_OPEN_PORT")))
      (testing "every port of the provider data is published, loopback by default"
        (doseq [p (vals (:ports (otel/provider "otel")))]
          (is (str/includes? text (str "${HIVE_OTEL_BIND:-127.0.0.1}:")) (str "port " p))
          (is (str/includes? text (str ":" p "\"")) (str "port " p)))))))
