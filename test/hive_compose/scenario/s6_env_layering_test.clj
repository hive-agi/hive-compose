(ns hive-compose.scenario.s6-env-layering-test
  "S6 env layering (pure wiring + the pipeline + the CLI engine over a
   recording exec; the secret store is a stub ISecretBackend)."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [hive-compose.adapter.cli :as cli]
            [hive-compose.port :as port]
            [hive-compose.pipeline.stacks :as stacks]
            [hive-compose.promote.env :as env]
            [hive-compose.promote.otel :as otel]
            [hive-compose.promote.stack-run :as run]
            [hive-compose.promote.wiring :as wiring]
            [hive-compose.scenario.world :as w]
            [hive-dsl.result :as r]
            [hive-system.secrets.core :as sec]
            [hive-system.secrets.protocols :as proto]))

;; SPDX-License-Identifier: MIT

(def secret-path "infra/kafka-sasl")
(def secret-value "stub-sasl-9d1e77")

(def projects
  (-> w/projects
      (assoc-in ["infra" :project/provides "kafka" :env]
                {"KAFKA_BROKERS" "kafka:9092" "KAFKA_SASL_PASSWORD" {:pass secret-path}
                 "KAFKA_CLIENT_ID" "injected"})
      (assoc-in ["a" :project/env]
                {"KAFKA_CLIENT_ID" "a-own"
                 "OTEL_EXPORTER_OTLP_ENDPOINT" "http://collector:4318"
                 "OTEL_EXPORTER_OTLP_PROTOCOL" "http/protobuf"
                 "OTEL_RESOURCE_ATTRIBUTES" "service.name=a-api,hive.run.id=spoofed,team=pay"
                 "HIVE_RUN_ID" "spoofed"})))

(defn- settings [] (w/settings projects))

(defn- resolution [c run] (get-in (stacks/resolve-plan c "ab" run) [:ok :resolution]))

(deftest s6-injected-under-consumer-under-forced
  "CLAIM: for consumer A, a provider's injected value loses to A's own env per
   key (KAFKA_CLIENT_ID), injected keys A does not set survive
   (KAFKA_BROKERS), the hive-forced keys win over A (HIVE_RUN_ID and
   hive.run.id inside OTEL_RESOURCE_ATTRIBUTES, merged per key so A's
   service.name and team stay), and A setting the OTLP endpoint owns the
   protocol too: the injected otel pair (grpc to otel:4317) is replaced as a
   unit, never mixed. B, with no env of its own, keeps the injected otel
   pair, and hive.run.id is present for every consumer."
  (let [c (w/ctx {:projects projects})
        rid "s6run"
        res (resolution c rid)
        otlp (fn [consumer] [(otel/otlp-env {:run-id rid :service consumer})])
        ws (:ok (wiring/wirings (settings) res rid otlp))
        ea (get-in ws ["a" :wiring/env])
        eb (get-in ws ["b" :wiring/env])]
    (is (some? ws))
    (is (= "a-own" (get ea "KAFKA_CLIENT_ID")) "consumer beats injected")
    (is (= "injected" (get eb "KAFKA_CLIENT_ID")))
    (is (= "kafka:9092" (get ea "KAFKA_BROKERS")))
    (is (= rid (get ea "HIVE_RUN_ID")) "forced beats consumer")
    (is (= {"service.name" "a-api" "hive.run.id" rid "team" "pay"}
           (otel/parse-resource-attributes (get ea "OTEL_RESOURCE_ATTRIBUTES"))))
    (is (= ["http://collector:4318" "http/protobuf"]
           [(get ea "OTEL_EXPORTER_OTLP_ENDPOINT") (get ea "OTEL_EXPORTER_OTLP_PROTOCOL")])
        "the consumer's endpoint came with its own protocol")
    (is (= ["http://otel:4317" "grpc"]
           [(get eb "OTEL_EXPORTER_OTLP_ENDPOINT") (get eb "OTEL_EXPORTER_OTLP_PROTOCOL")]))
    (is (every? #(= rid (get (otel/parse-resource-attributes (get-in % [:wiring/env "OTEL_RESOURCE_ATTRIBUTES"])) "hive.run.id"))
                (vals ws)))
    (testing "an endpoint alone is refused, never paired with the injected protocol"
      (let [p (assoc-in projects ["a" :project/env] {"OTEL_EXPORTER_OTLP_ENDPOINT" "http://collector:4317"})
            c2 (w/ctx {:projects p})]
        (is (= :compose/otlp-pair-broken (:error (wiring/wirings (w/settings p) res rid otlp))))
        (let [up (w/up! c2 "ab" rid)]
          (is (= :compose/otlp-pair-broken (:error up)) "stack-up refuses it too")
          (is (= "a" (:consumer up)))
          (is (empty? (w/mutating-calls c2)) "before anything starts"))))))

(deftest s6-stack-up-injects-the-otlp-env
  "CLAIM (pipeline): stack-up gives every container consumer the OTLP pair
   and hive.run.id, so its telemetry is correlated without any env of its
   own (plan hc-7: every consumer gets OTEL_EXPORTER_OTLP_ENDPOINT and
   OTEL_RESOURCE_ATTRIBUTES with hive.run.id)."
  (let [p (assoc-in w/projects ["infra" :project/provides "otel"] {:service "otel" :version "0.41.1"})
        c (-> (w/ctx {:projects p})
              (assoc-in [:settings :compose/stacks "abo"]
                        {:stack/id "abo" :stack/consumers ["a" "b"] :stack/needs {"otel" {}}}))
        rid "s6otel"
        ws (get-in (stacks/resolve-plan c "abo" rid) [:ok :wirings])]
    (is (= ["http://otel:4317" "grpc"]
           [(get-in ws ["b" :wiring/env "OTEL_EXPORTER_OTLP_ENDPOINT"])
            (get-in ws ["b" :wiring/env "OTEL_EXPORTER_OTLP_PROTOCOL"])])
        "a stack that needs otel wires every consumer to it on its view network")
    (is (= "b" (get-in ws ["b" :wiring/env "OTEL_SERVICE_NAME"])))
    (is (= rid (get (otel/parse-resource-attributes (get-in ws ["b" :wiring/env "OTEL_RESOURCE_ATTRIBUTES"])) "hive.run.id")))
    (testing "the otel instance joins each consumer's view network under its alias"
      (is (some #(= ["otel"] (:aliases %)) (get-in ws ["a" :wiring/attach]))))
    (testing "without otel in the stack nothing points at an unreachable collector"
      (let [ws0 (get-in (stacks/resolve-plan (w/ctx) "ab" rid) [:ok :wirings])]
        (is (not (contains? (get-in ws0 ["b" :wiring/env]) "OTEL_EXPORTER_OTLP_ENDPOINT")))
        (is (= (str "hive.run.id=" rid) (get-in ws0 ["b" :wiring/env "OTEL_RESOURCE_ATTRIBUTES"])))
        (is (= rid (get-in ws0 ["b" :wiring/env "HIVE_RUN_ID"])))))))

(defn- stub-resolver [fetches]
  {:pass (reify proto/ISecretBackend
           (backend-id [_] :pass)
           (fetch [_ k _]
             (swap! fetches conj k)
             (if (= k secret-path)
               (r/ok (sec/make-secret secret-value :pass k))
               (r/err :pass/not-found {:key k})))
           (available? [_] (r/ok true)))})

(defn- tmp-dir []
  (str (java.nio.file.Files/createTempDirectory "hc-s6" (make-array java.nio.file.attribute.FileAttribute 0))))

(deftest s6-pass-references-are-never-rendered-as-values
  "CLAIM: a provider env value {:pass path} stays a reference through the
   wiring, the run state the addon persists, and the generated compose
   override file; the stub ISecretBackend is asked only when the consumer's
   containers are created, the value reaches only the docker process env,
   and every rendering shows pass:<path>."
  (let [c (w/ctx {:projects projects})
        rid "s6sec"
        up (w/up! c "ab" rid)
        wa (w/wiring-of c "ab" rid "a")]
    (is (r/ok? up) (pr-str up))
    (is (= {:pass secret-path} (get-in wa [:wiring/env "KAFKA_SASL_PASSWORD"])))
    (is (= (str "pass:" secret-path) (get (env/rendered (:wiring/env wa)) "KAFKA_SASL_PASSWORD")))
    (is (not (str/includes? (pr-str @(:saved c)) secret-value)) "the persisted state holds no value")
    (is (not (str/includes? (pr-str up) secret-value)))
    (testing "the CLI engine reads it at spawn only"
      (let [fetches (atom []) calls (atom []) dir (tmp-dir)
            e (cli/make-engine {:exec (fn [argv opts] (swap! calls conj [argv opts]) (r/ok {:exit 0 :stdout "" :stderr ""}))
                                :compose/resolver (stub-resolver fetches)
                                :compose/stack-dir dir})
            proj (tmp-dir)
            _ (spit (io/file proj "compose.yaml") "services: {app: {image: busybox}}\n")
            spec (assoc (run/consumer-spec (settings) rid "ab" "a" ["app"] wa
                                           (keys (:wiring/env wa)) false)
                        :dir proj)]
        (is (= [] @fetches))
        (is (r/ok? (port/-up-project! e spec)))
        (is (= [secret-path] @fetches))
        (let [[_ opts] (last @calls)
              override (slurp (io/file (cli/override-path dir (:project spec))))]
          (is (= secret-value (get-in opts [:env "KAFKA_SASL_PASSWORD"])) "the docker process env")
          (is (not (str/includes? override secret-value)) "never the override file")
          (is (str/includes? override "KAFKA_SASL_PASSWORD")))
        (reset! fetches [])
        (is (r/ok? (port/-down-project! e (assoc spec :all? true))))
        (is (= [] @fetches) "down reads no store")))))
