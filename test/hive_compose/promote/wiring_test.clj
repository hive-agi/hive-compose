(ns hive-compose.promote.wiring-test
  "Wiring and tenancy injection per consumer (hc-5)."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.promote.env :as env]
            [hive-compose.promote.wiring :as wiring]
            [hive-compose.schema :as schema]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def settings
  {:compose/projects
   {"infra" {:project/id "infra" :project/dir "/w/infra"
             :project/provides {"kafka" {:service "kafka" :version "3.7"
                                         :env {"KAFKA_BROKERS" "kafka:9092"}
                                         :tenancy {:env "KAFKA_TOPIC_PREFIX"}}
                                "postgres" {:service "pg" :version "16"
                                            :env {"PG_HOST" "pg" "PG_PASSWORD" {:pass "infra/pg"}}}}}
    "shop" {:project/id "shop" :project/dir "/w/shop"
            :project/needs {"kafka" {:version "^3"}
                            "postgres" {:version "16" :services ["api"]}}}
    "billing" {:project/id "billing" :project/dir "/w/billing"
               :project/needs {"kafka" {:version "^3"}}}}})

(def kafka
  {:instance/id "infra/kafka" :instance/capability "kafka" :instance/provider "infra"
   :instance/service "kafka" :instance/version "3.7" :instance/share? true
   :instance/consumers ["shop" "billing"] :instance/aliases ["kafka"]
   :instance/compose-project "infra"})

(def pg
  {:instance/id "infra/postgres@shop" :instance/capability "postgres" :instance/provider "infra"
   :instance/service "pg" :instance/share? false :instance/consumers ["shop"]
   :instance/aliases ["pg"] :instance/compose-project "infra-shop-r-01"})

(def resolution
  {:resolution/instances {"infra/kafka" kafka "infra/postgres@shop" pg}
   :resolution/bindings {"shop" {"kafka" "infra/kafka" "postgres" "infra/postgres@shop"}
                         "billing" {"kafka" "infra/kafka"}}})

(deftest wiring-of-one-consumer
  (let [w (wiring/wiring settings resolution "r-01" "shop")]
    (is (r/ok? w))
    (is (nil? (schema/explain schema/Wiring (:ok w))))
    (is (= {:wiring/consumer "shop" :wiring/run "r-01"
            :wiring/network "hive-r-01-shop"
            :wiring/attach [{:instance "infra/kafka" :aliases ["kafka"]}
                            {:instance "infra/postgres@shop" :aliases ["pg"]}]
            :wiring/env {"KAFKA_BROKERS" "kafka:9092" "KAFKA_TOPIC_PREFIX" "r-01.shop."
                         "PG_HOST" "pg" "PG_PASSWORD" {:pass "infra/pg"}
                         "HIVE_RUN_ID" "r-01" "OTEL_RESOURCE_ATTRIBUTES" "hive.run.id=r-01"}
            :wiring/prefix "r-01.shop."}
           (:ok w))
        "provider hostnames stay as written: the network aliases make them right")))

(deftest secret-refs-stay-references
  (let [e (:wiring/env (:ok (wiring/wiring settings resolution "r-01" "shop")))]
    (is (= {:pass "infra/pg"} (get e "PG_PASSWORD")))
    (is (= "pass:infra/pg" (get (env/rendered e) "PG_PASSWORD")))))

(deftest a-section-consumer-reads-its-project-needs
  (let [res (assoc-in resolution [:resolution/bindings "billing/api"] {"kafka" "infra/kafka"})
        w (:ok (wiring/wiring settings res "r-01" "billing/api"))]
    (is (= "hive-r-01-billing-api" (:wiring/network w)))
    (is (= "r-01.billing-api." (get-in w [:wiring/env "KAFKA_TOPIC_PREFIX"])))))

(deftest per-service-restriction
  (testing "postgres is needed by shop's api only"
    (is (= "pg" (get (:ok (wiring/service-env settings resolution "r-01" "shop" "api")) "PG_HOST")))
    (let [worker (:ok (wiring/service-env settings resolution "r-01" "shop" "worker"))]
      (is (not (contains? worker "PG_HOST")))
      (is (= "kafka:9092" (get worker "KAFKA_BROKERS")) "kafka names no :services: every service")
      (is (= "r-01" (get worker "HIVE_RUN_ID"))))))

(deftest env-collision-names-both-instances-and-the-key
  (let [s (assoc-in settings [:compose/projects "infra" :project/provides "postgres" :env "KAFKA_BROKERS"]
                    "elsewhere:9092")
        w (wiring/wiring s resolution "r-01" "shop")]
    (is (= :compose/env-collision (:error w)))
    (is (= "KAFKA_BROKERS" (:key w)))
    (is (= #{"infra/kafka" "infra/postgres@shop"} (set (:instances w)))))
  (testing "the same value from two instances is no collision"
    (let [s (assoc-in settings [:compose/projects "infra" :project/provides "postgres" :env "KAFKA_BROKERS"]
                      "kafka:9092")]
      (is (r/ok? (wiring/wiring s resolution "r-01" "shop"))))))

(deftest unknown-instance-and-bad-run
  (is (= :compose/unknown-instance
         (:error (wiring/wiring settings (assoc-in resolution [:resolution/bindings "x"] {"kafka" "nope"})
                                "r-01" "x"))))
  (is (= :compose/invalid-run (:error (wiring/wiring settings resolution "R 01" "shop")))))

(def otlp
  {"OTEL_EXPORTER_OTLP_ENDPOINT" "http://otel:4317" "OTEL_EXPORTER_OTLP_PROTOCOL" "grpc"
   "OTEL_RESOURCE_ATTRIBUTES" "service.name=shop,hive.run.id=wrong"})

(deftest extra-envs-hook
  (let [e (:wiring/env (:ok (wiring/wiring settings resolution "r-01" "shop" [otlp {"EXTRA" "1"}])))]
    (is (= "http://otel:4317" (get e "OTEL_EXPORTER_OTLP_ENDPOINT")))
    (is (= "grpc" (get e "OTEL_EXPORTER_OTLP_PROTOCOL")))
    (is (= "service.name=shop,hive.run.id=r-01" (get e "OTEL_RESOURCE_ATTRIBUTES"))
        "hive.run.id is forced to the run")
    (is (= "1" (get e "EXTRA"))))
  (testing "a later map naming either pair key replaces the pair"
    (let [e (:wiring/env (:ok (wiring/wiring settings resolution "r-01" "shop"
                                             [otlp {"OTEL_EXPORTER_OTLP_ENDPOINT" "http://otel:4318"
                                                    "OTEL_EXPORTER_OTLP_PROTOCOL" "http/protobuf"}])))]
      (is (= ["http://otel:4318" "http/protobuf"]
             [(get e "OTEL_EXPORTER_OTLP_ENDPOINT") (get e "OTEL_EXPORTER_OTLP_PROTOCOL")]))))
  (testing "an endpoint without its protocol is refused"
    (let [w (wiring/wiring settings resolution "r-01" "shop" [otlp {"OTEL_EXPORTER_OTLP_ENDPOINT" "http://x:4317"}])]
      (is (= :compose/otlp-pair-broken (:error w)))))
  (testing "HIVE_RUN_ID cannot be overridden"
    (is (= "r-01" (get-in (:ok (wiring/wiring settings resolution "r-01" "shop" [{"HIVE_RUN_ID" "other"}]))
                          [:wiring/env "HIVE_RUN_ID"])))))

(deftest consumer-env-overlays-the-injected-env
  (let [s (assoc-in settings [:compose/projects "shop" :project/env]
                    {"KAFKA_BROKERS" "mine:9092"
                     "OTEL_EXPORTER_OTLP_ENDPOINT" "http://collector:4318"
                     "OTEL_EXPORTER_OTLP_PROTOCOL" "http/protobuf"
                     "OTEL_RESOURCE_ATTRIBUTES" "service.name=shop-api,hive.run.id=mine"
                     "HIVE_RUN_ID" "mine"})
        e (:wiring/env (:ok (wiring/wiring s resolution "r-01" "shop" [otlp])))]
    (is (= "mine:9092" (get e "KAFKA_BROKERS")) "the consumer wins per key")
    (is (= ["http://collector:4318" "http/protobuf"]
           [(get e "OTEL_EXPORTER_OTLP_ENDPOINT") (get e "OTEL_EXPORTER_OTLP_PROTOCOL")])
        "and per pair")
    (is (= "service.name=shop-api,hive.run.id=r-01" (get e "OTEL_RESOURCE_ATTRIBUTES")))
    (is (= "r-01" (get e "HIVE_RUN_ID")) "forced keys stay forced"))
  (testing "a consumer naming only the endpoint owns the pair: the protocol goes with it"
    (let [s (assoc-in settings [:compose/projects "shop" :project/env]
                      {"OTEL_EXPORTER_OTLP_ENDPOINT" "http://collector:4317"})]
      (is (= :compose/otlp-pair-broken (:error (wiring/wiring s resolution "r-01" "shop" [otlp]))))))
  (testing "a suite consumer reads :suite/env"
    (let [s (assoc settings :compose/suites {"smoke" {:suite/id "smoke" :suite/image "x" :suite/env {"S" "1"}}})
          res (assoc-in resolution [:resolution/bindings "suite:smoke"] {"kafka" "infra/kafka"})]
      (is (= "1" (get-in (:ok (wiring/wiring s res "r-01" "suite:smoke")) [:wiring/env "S"]))))))

(deftest attributes-merge
  (is (= "a=1,b=3,c=4" (wiring/merge-attributes ["a=1,b=2" "b=3,c=4"])))
  (is (= "a=x%3Dy" (wiring/merge-attributes ["a=x%3Dy"])) "values are kept as encoded")
  (is (= "k=v=w" (wiring/merge-attributes ["k=v=w"])) "split on the first =")
  (is (= "a=1" (wiring/merge-attributes [" a = 1 ,, =bad,nokey"]))))

(deftest all-consumers
  (let [ws (wiring/wirings settings resolution "r-01" (fn [c] [{"WHO" c}]))]
    (is (r/ok? ws))
    (is (= #{"shop" "billing"} (set (keys (:ok ws)))))
    (is (= "billing" (get-in (:ok ws) ["billing" :wiring/env "WHO"])))
    (is (every? #(nil? (schema/explain schema/Wiring %)) (vals (:ok ws))))))

;; ---------------------------------------------------------------------------
;; Properties

(def gen-run
  (gen/fmap (fn [[c cs]] (apply str c cs))
            (gen/tuple (gen/elements (seq "abcdefghijklmnopqrstuvwxyz0123456789"))
                       (gen/vector (gen/elements (seq "abcdefghijklmnopqrstuvwxyz0123456789-")) 2 20))))

(def caps ["kafka" "postgres" "redis" "s3" "otel"])

(def consumers ["shop" "billing" "billing/api" "suite:smoke" "legacy"])

(def gen-value
  (gen/one-of [(gen/fmap #(str "v-" %) gen/nat)
               (gen/fmap (fn [n] {:pass (str "store/" n)}) gen/nat)]))

(def gen-world
  "Settings with one provider project offering a random subset of caps (env
   keys namespaced by cap so no collision), and a resolution binding random
   consumers to shared or private instances."
  (gen/let [provided (gen/fmap (comp vec distinct) (gen/vector (gen/elements caps) 1 5))
            envs (gen/vector (gen/map (gen/elements ["HOST" "PORT" "PASSWORD"]) gen-value) (count provided))
            tenancy (gen/vector gen/boolean (count provided))
            bound (gen/vector (gen/tuple (gen/elements consumers)
                                         (gen/vector (gen/tuple (gen/elements provided) gen/boolean)))
                              1 5)]
    (let [provides (into {} (map (fn [cap e t]
                                   [cap (cond-> {:service cap
                                                 :env (into {} (map (fn [[k v]] [(str (str/upper-case cap) "_" k) v])) e)}
                                          t (assoc :tenancy {:env (str (str/upper-case cap) "_PREFIX")}))])
                                 provided envs tenancy))
          bindings (into {} (map (fn [[c bs]]
                                   [c (into {} (map (fn [[cap shared?]]
                                                      [cap (if shared? (str "infra/" cap) (str "infra/" cap "@" c))]))
                                            bs)]))
                         bound)
          ids (distinct (mapcat vals (vals bindings)))
          instances (into {} (map (fn [id]
                                    (let [[_ cap _] (re-matches #"infra/([^@]+)(@.*)?" id)]
                                      [id {:instance/id id :instance/capability cap :instance/provider "infra"
                                           :instance/service cap :instance/share? (not (str/includes? id "@"))
                                           :instance/consumers ["shop"] :instance/aliases [cap]}])))
                          ids)]
      {:settings {:compose/projects {"infra" {:project/id "infra" :project/dir "/w" :project/provides provides}}}
       :resolution {:resolution/instances instances :resolution/bindings bindings}})))

(defn- all-wirings [{:keys [settings resolution]} run]
  (:ok (wiring/wirings settings resolution run)))

(defspec hive-run-id-is-always-the-run 100
  (prop/for-all [world gen-world run gen-run]
    (let [ws (all-wirings world run)]
      (and (some? ws)
           (every? (fn [w] (and (= run (get-in w [:wiring/env "HIVE_RUN_ID"]))
                                (= run (:wiring/run w))
                                (nil? (schema/explain schema/Wiring w))))
                   (vals ws))))))

(defspec consumers-sharing-an-instance-get-different-prefixes 100
  (prop/for-all [world gen-world run gen-run]
    (let [ws (all-wirings world run)
          by-instance (group-by first (for [[c w] ws a (:wiring/attach w)] [(:instance a) c]))]
      (every? (fn [[_ pairs]]
                (let [cs (distinct (map second pairs))]
                  (= (count cs) (count (distinct (map #(get-in ws [% :wiring/prefix]) cs))))))
              by-instance))))

(defspec secret-refs-never-become-strings 100
  (prop/for-all [world gen-world run gen-run]
    (let [ws (all-wirings world run)
          provides (get-in world [:settings :compose/projects "infra" :project/provides])]
      (every? (fn [[c w]]
                (every? (fn [cap]
                          (every? (fn [[k v]] (or (not (env/ref? v)) (= v (get-in w [:wiring/env k]))))
                                  (get-in provides [cap :env])))
                        (keys (get-in world [:resolution :resolution/bindings c]))))
              ws))))

(defspec every-bound-instance-is-attached-exactly-once 100
  (prop/for-all [world gen-world run gen-run]
    (let [ws (all-wirings world run)]
      (every? (fn [[c w]]
                (let [ids (map :instance (:wiring/attach w))]
                  (and (= (count ids) (count (distinct ids)))
                       (= (set ids) (set (vals (get-in world [:resolution :resolution/bindings c])))))))
              ws))))

(def gen-attrs
  (gen/vector (gen/vector (gen/tuple (gen/elements ["service.name" "a" "b" "hive.run.id" "deployment.environment"])
                                     (gen/fmap #(str "v" %) gen/nat))
                          0 4)
              0 4))

(defspec resource-attributes-merge-per-key-later-wins-run-forced 100
  (prop/for-all [attr-sets gen-attrs run gen-run]
    (let [extra (map (fn [kvs] {"OTEL_RESOURCE_ATTRIBUTES" (str/join "," (map (fn [[k v]] (str k "=" v)) kvs))})
                     attr-sets)
          e (:wiring/env (:ok (wiring/wiring settings resolution run "shop" extra)))
          got (into {} (wiring/parse-attributes (get e "OTEL_RESOURCE_ATTRIBUTES")))
          want (assoc (into {} (apply concat attr-sets)) "hive.run.id" run)]
      (= want got))))

(defspec otlp-pair-travels-together 100
  (prop/for-all [maps (gen/vector (gen/elements [{} {"X" "1"}
                                                 {"OTEL_EXPORTER_OTLP_ENDPOINT" "http://otel:4317"
                                                  "OTEL_EXPORTER_OTLP_PROTOCOL" "grpc"}
                                                 {"OTEL_EXPORTER_OTLP_ENDPOINT" "http://otel:4318"
                                                  "OTEL_EXPORTER_OTLP_PROTOCOL" "http/protobuf"}])
                                  0 5)]
    (let [e (:wiring/env (:ok (wiring/wiring settings resolution "r-01" "shop" maps)))
          last-pair (last (filter #(contains? % "OTEL_EXPORTER_OTLP_ENDPOINT") maps))]
      (= (select-keys last-pair wiring/otlp-pair) (select-keys e wiring/otlp-pair)))))
