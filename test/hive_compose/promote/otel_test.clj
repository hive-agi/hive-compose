(ns hive-compose.promote.otel-test
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.promote.otel :as otel]))

;; SPDX-License-Identifier: MIT

(def gen-char
  "Any BMP character but a lone surrogate (not representable in UTF-8), with
   the separators the encoding must survive weighted in."
  (gen/frequency [[3 (gen/elements [\, \= \% \space \tab \newline \+ \; \" \\])]
                  [5 gen/char]
                  [2 (gen/fmap char (gen/one-of [(gen/choose 0x100 0xD7FF)
                                                 (gen/choose 0xE000 0xFFFD)]))]]))

(def gen-text (gen/fmap str/join (gen/vector gen-char 0 12)))

(def gen-attrs (gen/map gen-text gen-text {:max-elements 8}))

(deftest otlp-env-shape
  (let [env (otel/otlp-env {:run-id "r-1" :service "shop api" :test-id "t,1"
                            :protocol :http :endpoint "http://lgtm:4318"
                            :extra-attrs {:deployment.environment "e2e" "z" "a=b"}})]
    (is (= #{"OTEL_EXPORTER_OTLP_ENDPOINT" "OTEL_EXPORTER_OTLP_PROTOCOL"
             "OTEL_SERVICE_NAME" "OTEL_RESOURCE_ATTRIBUTES"}
           (set (keys env))))
    (is (= "http://lgtm:4318" (env "OTEL_EXPORTER_OTLP_ENDPOINT")))
    (is (= "http/protobuf" (env "OTEL_EXPORTER_OTLP_PROTOCOL")))
    (is (= "shop api" (env "OTEL_SERVICE_NAME")))
    (is (= (str "hive.run.id=r-1,service.name=shop%20api,hive.test.id=t%2C1,"
                "deployment.environment=e2e,z=a%3Db")
           (env "OTEL_RESOURCE_ATTRIBUTES"))
        "run id, service, test id first; extras in key order; values encoded")))

(deftest otlp-env-defaults
  (let [env (otel/otlp-env {:run-id "r" :service "s"})]
    (is (= "grpc" (env "OTEL_EXPORTER_OTLP_PROTOCOL")))
    (is (= "http://localhost:4317" (env "OTEL_EXPORTER_OTLP_ENDPOINT")))
    (is (= "hive.run.id=r,service.name=s" (env "OTEL_RESOURCE_ATTRIBUTES"))
        "no test id, no hive.test.id"))
  (is (= "http://localhost:4318"
         ((otel/otlp-env {:run-id "r" :service "s" :protocol :http})
          "OTEL_EXPORTER_OTLP_ENDPOINT"))))

(deftest extra-attributes-never-override-reserved
  (let [attrs (otel/parse-resource-attributes
               ((otel/otlp-env {:run-id "r" :service "s"
                                :extra-attrs {"hive.run.id" "evil" "service.name" "x"}})
                "OTEL_RESOURCE_ATTRIBUTES"))]
    (is (= {"hive.run.id" "r" "service.name" "s"} attrs))))

(deftest percent-encoding
  (is (= "a%2Cb%3Dc%25d%20e" (otel/percent-encode "a,b=c%d e")))
  (is (= "%C3%A9" (otel/percent-encode "é")))
  (is (= "a,b=c%d e" (otel/percent-decode "a%2Cb%3Dc%25d%20e")))
  (is (= "100%" (otel/percent-decode "100%")) "a dangling % stays literal")
  (is (= "%zz+" (otel/percent-decode "%zz+")) "invalid escape stays, + is a plus"))

(deftest parse-tolerates-sdk-style-input
  (is (= {} (otel/parse-resource-attributes nil)))
  (is (= {} (otel/parse-resource-attributes "")))
  (is (= {"a" "1" "b" "x y"}
         (otel/parse-resource-attributes " a = 1 ,junk, b=x%20y,"))
      "entries trimmed, entries without = dropped"))

(defspec parse-inverts-render 300
  (prop/for-all [attrs gen-attrs]
    (= attrs (otel/parse-resource-attributes (otel/render-resource-attributes attrs)))))

(defspec render-is-deterministic-and-separator-safe 200
  (prop/for-all [attrs gen-attrs]
    (let [s (otel/render-resource-attributes attrs)]
      (and (= s (otel/render-resource-attributes (into {} (reverse (seq attrs)))))
           (= (count attrs) (if (empty? attrs) 0 (count (str/split s #"," -1))))
           (not (re-find #"[\s%](?![0-9A-F]{2})" s))))))

(defspec otlp-env-carries-the-run-id 200
  (prop/for-all [run-id gen-text service gen-text
                 test-id (gen/one-of [(gen/return nil) gen-text])
                 extra gen-attrs]
    (let [attrs (otel/parse-resource-attributes
                 ((otel/otlp-env {:run-id run-id :service service :test-id test-id
                                  :extra-attrs extra})
                  "OTEL_RESOURCE_ATTRIBUTES"))]
      (and (= run-id (attrs "hive.run.id"))
           (= service (attrs "service.name"))
           (= test-id (attrs "hive.test.id"))
           (every? (fn [[k v]] (or (otel/reserved-attributes k) (= v (attrs k)))) extra)))))

(def gen-env-key
  (gen/one-of [(gen/elements ["OTEL_EXPORTER_OTLP_ENDPOINT" "OTEL_EXPORTER_OTLP_PROTOCOL"
                              "OTEL_SERVICE_NAME" "OTEL_RESOURCE_ATTRIBUTES"
                              "OTEL_TRACES_SAMPLER" "PATH"])
               (gen/fmap #(str "X_" %) gen/string-alphanumeric)]))

(def gen-env-val
  (gen/one-of [gen-text (gen/fmap (fn [p] {:pass p}) gen/string-alphanumeric)]))

(def gen-env (gen/map gen-env-key gen-env-val {:max-elements 6}))

(defspec merge-env-never-clobbers-the-consumer 300
  (prop/for-all [consumer gen-env
                 injected (gen/fmap #(otel/otlp-env {:run-id % :service "s"}) gen-text)]
    (let [merged (otel/merge-env consumer injected)]
      (and (every? (fn [[k v]] (= v (merged k))) consumer)
           (= (set (keys merged)) (set/union (set (keys consumer)) (set (keys injected))))
           (every? (fn [[k v]] (or (contains? consumer k) (= v (merged k)))) injected)))))

(defspec merge-env-is-idempotent 200
  (prop/for-all [consumer gen-env injected gen-env]
    (let [once (otel/merge-env consumer injected)]
      (= once (otel/merge-env once injected)))))

(deftest provider-data
  (let [{:keys [version service ports image]} (otel/provider "otel")]
    (is (= "lgtm" service))
    (is (= otel/lgtm-version version))
    (is (not= "latest" version))
    (is (= (str "grafana/otel-lgtm:" version) image))
    (is (= {:otlp-grpc 4317 :otlp-http 4318 :grafana 3000 :tempo 3200
            :loki 3100 :prometheus 9090}
           ports))))

(deftest compose-resource-names-otel-lgtm
  (let [res (io/resource otel/compose-resource)]
    (is (some? res) "the fragment is on the classpath")
    (let [text (slurp res)]
      (testing "the provider image, pinned to the version the provider data names"
        (is (str/includes? text "grafana/otel-lgtm:"))
        (is (str/includes? text otel/lgtm-version))
        (is (not (str/includes? text "otel-lgtm:latest"))))
      (testing "capability label, healthcheck, beyla behind its profile"
        (is (str/includes? text "hive.capability: otel"))
        (is (str/includes? text "healthcheck:"))
        (is (str/includes? text "grafana/beyla:"))
        (is (str/includes? text "profiles: [\"beyla\"]"))
        (is (str/includes? text "BEYLA_OPEN_PORT")))
      (testing "every port of the provider data is published"
        (doseq [p (vals (:ports (otel/provider "otel")))]
          (is (str/includes? text (str ":" p "\"")) (str "port " p)))))))
