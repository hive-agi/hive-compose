(ns hive-compose.stack-e2e-test
  "hc-11, the e2e proof on a real docker daemon, with the fixtures of
   test/resources/e2e and the built-in otel provider:

   - A = orders (Go) and B = billing (JS) both need kafka, shared: ONE kafka
     instance runs under the provider's compose project;
   - C = a second billing (billing-c) needs kafka with :share? false and gets
     a private one, `-p <provider>-billing-c-<run>`, on its own network;
   - otel (resources/hive-compose/otel) is wired to every one of them;
   - an order placed on A is invoiced by B and never seen by C.

   Integration only: it skips cleanly (a printed reason, one passing
   assertion) without a docker daemon. Afterwards the run is taken down with
   stack-down! (never volumes) and no network of the run is left."
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [hive-compose.adapter.cli :as cli]
            [hive-compose.pipeline.stacks :as stacks]
            [hive-compose.port :as port]
            [hive-dsl.result :as r])
  (:import (java.net URI)
           (java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers HttpResponse$BodyHandlers)
           (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)
           (java.time Duration)))

;; SPDX-License-Identifier: MIT

;; ---------------------------------------------------------------- fixtures

(def e2e-root
  "test/resources/e2e, from the classpath."
  (some-> (io/resource "e2e/README.md") io/file .getParentFile))

(def otel-dir
  "The built-in otel provider's compose directory, from the classpath."
  (some-> (io/resource "hive-compose/otel/compose.yml") io/file .getParentFile str))

(defn- fixture [d] (str (io/file e2e-root d)))

(def images
  [["orders" "hive-compose-e2e/orders:dev"]
   ["billing" "hive-compose-e2e/billing:dev"]
   ["suite" "hive-compose-e2e/suite:dev"]])

(defn- docker-available? []
  (try (zero? (:exit (sh/sh "docker" "info" "--format" "{{.ServerVersion}}")))
       (catch java.io.IOException _ false)))

(defn- build-images!
  "Builds the fixture images (bounded by `timeout`); answers the failures."
  []
  (vec (for [[d tag] images
             :let [{:keys [exit err]} (sh/sh "timeout" "900" "docker" "build" "-q" "-t" tag (fixture d))]
             :when (not (zero? exit))]
         {:dir d :exit exit :err err})))

(defn- temp-dir [prefix]
  (str (Files/createTempDirectory prefix (make-array FileAttribute 0))))

;; ---------------------------------------------------------------- config

(def otlp-http
  "Both fixtures export with an OTLP/HTTP SDK, so each consumer declares the
   protocol it speaks in its own env, the layer that replaces the injected
   endpoint and protocol as one pair. The host name is the alias otel joins
   every consumer's view network under."
  {"OTEL_EXPORTER_OTLP_ENDPOINT" "http://otel:4318"
   "OTEL_EXPORTER_OTLP_PROTOCOL" "http/protobuf"})

(defn settings
  "The projects and the stack of the scenario, every compose project name
   carrying `tag` so nothing on the host is touched."
  [tag]
  {:compose/default-ttl-minutes 60
   :compose/default-idle-action :stop
   :compose/up-timeout-ms 300000
   :compose/profiles {}
   :compose/projects
   {"kafka" {:project/id "kafka" :project/dir (fixture "kafka") :project/name (str tag "kafka")
             :project/provides {"kafka" {:service "kafka" :version "3.9"
                                         :env {"KAFKA_BOOTSTRAP" "kafka:9092"}
                                         ;; A produces and B consumes one topic: one prefix per run
                                         :tenancy {:env "TOPIC_PREFIX" :scope :run}}}}
    "otel" {:project/id "otel" :project/dir otel-dir :project/name (str tag "otel")
            ;; ephemeral host ports: never clash with an otel already on the host
            :project/env (into {} (map (fn [v] [v "0"]))
                               ["HIVE_OTEL_OTLP_GRPC_PORT" "HIVE_OTEL_OTLP_HTTP_PORT" "HIVE_OTEL_GRAFANA_PORT"
                                "HIVE_OTEL_TEMPO_PORT" "HIVE_OTEL_LOKI_PORT" "HIVE_OTEL_PROMETHEUS_PORT"])
            :project/provides {"otel" {:service "otel" :version "0.41.1"}}}
    "orders" {:project/id "orders" :project/dir (fixture "orders") :project/name (str tag "orders")
              :project/env otlp-http
              :project/needs {"kafka" {:version "^3"} "otel" {}}}
    "billing" {:project/id "billing" :project/dir (fixture "billing") :project/name (str tag "billing")
               :project/env otlp-http
               :project/needs {"kafka" {:version "^3"} "otel" {}}}
    "billing-c" {:project/id "billing-c" :project/dir (fixture "billing") :project/name (str tag "billingc")
                 :project/env otlp-http
                 :project/needs {"kafka" {:version "^3" :share? false} "otel" {}}}}
   :compose/stacks {"e2e" {:stack/id "e2e" :stack/consumers ["orders" "billing" "billing-c"]}}})

(defn- context [state-dir settings]
  {:engine (cli/make-engine {:compose/stack-dir (str state-dir "/stacks")
                             :compose/up-timeout-ms 600000})
   :settings settings
   :state (atom {:active {} :current nil})
   :now #(System/currentTimeMillis)})

;; ---------------------------------------------------------------- docker and http

(defn- containers [engine labels]
  (:ok (port/-running-labelled engine labels)))

(defn- container-of [engine compose-project service]
  (:name (first (containers engine {"com.docker.compose.project" compose-project
                                    "com.docker.compose.service" service}))))

(defn- host-port
  "The host port `container` publishes for `container-port`."
  [container container-port]
  (let [{:keys [out]} (sh/sh "docker" "port" container (str container-port))]
    (some->> (str/split-lines (str out)) first (re-find #":(\d+)\s*$") second)))

(def ^:private http (-> (HttpClient/newBuilder) (.connectTimeout (Duration/ofSeconds 3)) (.build)))

(defn- request [method url body]
  (try
    (let [b (-> (HttpRequest/newBuilder (URI/create url))
                (.timeout (Duration/ofSeconds 10))
                (.header "Content-Type" "application/json"))
          req (.build (if body
                        (.method b method (HttpRequest$BodyPublishers/ofString body))
                        (.GET b)))
          resp (.send http req (HttpResponse$BodyHandlers/ofString))]
      {:status (.statusCode resp) :body (.body resp)})
    (catch Exception e {:status nil :body (str e)})))

(defn- eventually
  "The first truthy (f) within `ms`, polled each second; nil when none."
  [ms f]
  (let [deadline (+ (System/currentTimeMillis) ms)]
    (loop []
      (or (f)
          (when (< (System/currentTimeMillis) deadline)
            (Thread/sleep 1000)
            (recur))))))

(defn- run-networks [engine run]
  (mapv :name (:ok (port/-networks-labelled engine {"hive.run" run}))))

(defn- cleanup!
  "Whatever a failed run may have left, by compose project and network name.
   Never volumes."
  [engine s run]
  (doseq [[pid p] (:compose/projects s)
          :let [n (:project/name p)]]
    (port/-down-project! engine {:project (str n "-" run) :dir (:project/dir p) :all? true})
    (when (#{"kafka" "otel"} pid)
      (port/-down-project! engine {:project n :dir (:project/dir p) :all? true})
      (port/-down-project! engine {:project (str n "-billing-c-" run) :dir (:project/dir p) :all? true})))
  (doseq [n (run-networks engine run)]
    (port/-network-rm! engine n)))

;; ---------------------------------------------------------------- the proof

(deftest ^:integration shared-kafka-private-kafka-otel-everywhere
  (is (some? e2e-root) "test/resources/e2e is on the classpath")
  (is (some? otel-dir) "the otel provider is on the classpath")
  (if-not (docker-available?)
    (do (println "SKIP stack-e2e-test: no docker daemon") (is true))
    (let [stamp (mod (System/currentTimeMillis) 1000000)
          ;; fixed provider project names: a rerun reuses otel's named volume
          ;; (volumes are never removed); consumers are per run
          tag "hc11e2e"
          run (str "e2e" stamp)
          s (settings tag)
          ctx (context (temp-dir "hc11-state") s)
          engine (:engine ctx)
          kafka-cp (str tag "kafka")
          private-cp (str tag "kafka-billing-c-" run)]
      (is (= [] (build-images!)) "the fixture images build")
      (try
        (let [up (stacks/stack-up! ctx {:stack "e2e" :run run})]
          (is (r/ok? up) (pr-str (select-keys up [:error :instance :cause :not-ready :consumer :run])))
          (when (r/ok? up)
            (testing "exactly one shared kafka and one private one"
              (let [st (:ok (stacks/stack-status ctx {:run run}))
                    instances (get-in st [:stacks 0 :instances])]
                (is (= {"kafka/kafka" true "kafka/kafka@billing-c" false "otel/otel" true}
                       (into {} (map (fn [[k v]] [k (:shared? v)])) instances)))
                (is (= kafka-cp (get-in instances ["kafka/kafka" :compose-project])))
                (is (= private-cp (get-in instances ["kafka/kafka@billing-c" :compose-project]))))
              (let [shared (containers engine {"com.docker.compose.project" kafka-cp
                                               "com.docker.compose.service" "kafka"})
                    private (containers engine {"hive.run" run "hive.instance" "kafka/kafka@billing-c"})]
                (is (= 1 (count shared)) "one shared kafka, under the provider's compose project")
                (is (not (contains? (:labels (first shared)) "hive.run"))
                    "a shared instance carries no per-run label, so another run never recreates it")
                (is (= 1 (count private)) "one private kafka, labelled with the run and its instance")
                (is (= private-cp (:project (first private))))
                (is (= "billing-c" (get-in (first private) [:labels "hive.consumer"])))
                (doseq [c ["orders" "billing" "billing-c"]]
                  (is (seq (containers engine {"hive.run" run "hive.consumer" c}))
                      (str c " runs as a consumer of the run, labelled with it")))))
            (testing "otel is wired to every consumer"
              (doseq [[cp svc] [[(str tag "orders-" run) "orders"]
                                [(str tag "billing-" run) "billing"]
                                [(str tag "billingc-" run) "billing"]]]
                (let [c (container-of engine cp svc)
                      ;; inspect, not exec: the orders image is distroless (no `env` binary)
                      {:keys [out]} (sh/sh "docker" "inspect" "--format" "{{range .Config.Env}}{{println .}}{{end}}" c)
                      env (into {} (keep #(let [[k v] (str/split % #"=" 2)] (when v [k v]))) (str/split-lines out))]
                  (is (= "http://otel:4318" (get env "OTEL_EXPORTER_OTLP_ENDPOINT")) cp)
                  (is (= run (get env "HIVE_RUN_ID")) cp)
                  (is (str/includes? (str (get env "OTEL_RESOURCE_ATTRIBUTES")) "service.name=") cp)
                  (is (str/includes? (str (get env "OTEL_RESOURCE_ATTRIBUTES")) (str "hive.run.id=" run)) cp)
                  (is (= "kafka:9092" (get env "KAFKA_BOOTSTRAP")) cp)
                  (is (= (str run ".") (get env "TOPIC_PREFIX")) cp))))
            (testing "A's order reaches B over the shared kafka, never C"
              (let [orders (host-port (container-of engine (str tag "orders-" run) "orders") 8080)
                    billing (host-port (container-of engine (str tag "billing-" run) "billing") 8080)
                    billing-c (host-port (container-of engine (str tag "billingc-" run) "billing") 8080)
                    oid (str "e2e-" stamp)
                    placed (request "POST" (str "http://127.0.0.1:" orders "/orders")
                                    (str "{\"id\":\"" oid "\",\"sku\":\"widget\",\"qty\":1}"))]
                (is (= 202 (:status placed)) (pr-str placed))
                (is (eventually 60000 #(str/includes? (str (:body (request "GET" (str "http://127.0.0.1:" billing "/invoices") nil))) oid))
                    "B invoices A's order")
                (let [c (request "GET" (str "http://127.0.0.1:" billing-c "/invoices") nil)]
                  (is (= 200 (:status c)))
                  (is (= "[]" (str/trim (str (:body c)))) "C, on its private kafka, sees no invoice of A's orders"))))))
        (let [down (stacks/stack-down! ctx {:run run})]
          (is (r/ok? down) (pr-str down))
          (testing "down leaves nothing of the run"
            (is (empty? (containers engine {"hive.run" run})))
            (is (empty? (run-networks engine run)))
            (is (empty? (filter #(= "running" (:state %))
                                (containers engine {"com.docker.compose.project" kafka-cp})))
                "the shared kafka is released: no other run holds it")
            (is (not-any? #(str/includes? % run)
                          (str/split-lines (:out (sh/sh "docker" "network" "ls" "--format" "{{.Name}}")))))))
        (finally
          (cleanup! engine s run))))))
