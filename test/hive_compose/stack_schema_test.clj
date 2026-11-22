(ns hive-compose.stack-schema-test
  "Stack value objects (hc-2): provides, needs, Stack, Instance, Resolution,
   Wiring, Suite, RunId; and that they are additive to the config."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.promote.config :as config]
            [hive-compose.schema :as schema]
            [malli.core :as m]))

;; SPDX-License-Identifier: MIT

(def home "/fake-home")

(deftest provides-and-needs-on-a-project
  (let [s (:ok (config/settings
                {:compose/projects
                 {:infra {:project/dir "~/infra"
                          :project/provides {:kafka {:service "kafka" :version "3.7"
                                                     :env {"KAFKA_BROKERS" "kafka:9092"}
                                                     :tenancy {:env "KAFKA_TOPIC_PREFIX"}}}}
                  :shop {:project/dir "/w/shop"
                         :project/needs {:kafka {:version "^3"}
                                         "postgres" {:version ">=15 <17" :share? false}}}}}
                nil home))]
    (is (some? s))
    (is (= "kafka" (get-in s [:compose/projects "infra" :project/provides "kafka" :service]))
        "keyword capability names become strings")
    (is (= #{"kafka" "postgres"} (set (keys (get-in s [:compose/projects "shop" :project/needs])))))
    (is (false? (get-in s [:compose/projects "shop" :project/needs "postgres" :share?])))))

(deftest existing-configs-stay-valid
  (testing "no stack keys: the Settings are exactly what they were"
    (let [s (:ok (config/settings {:compose/projects {:shop {:project/dir "/w/shop"}}} nil home))]
      (is (some? s))
      (is (not (contains? s :compose/stacks)))
      (is (not (contains? s :compose/suites)))
      (is (nil? (get-in s [:compose/projects "shop" :project/provides]))))))

(deftest stacks-and-suites-in-settings
  (let [s (:ok (config/settings
                {:compose/stacks [{:stack/id "checkout" :stack/consumers ["shop" "billing/api"]
                                   :stack/needs {:otel {}}}]
                 :compose/suites {:smoke {:suite/image "ghcr.io/acme/smoke:1"
                                          :suite/needs {:kafka {:version "^3"}}
                                          :suite/projects ["shop"]
                                          :suite/report-format "junit"}}}
                nil home))]
    (is (= ["shop" "billing/api"] (get-in s [:compose/stacks "checkout" :stack/consumers])))
    (is (= {"otel" {}} (get-in s [:compose/stacks "checkout" :stack/needs])))
    (is (= "smoke" (get-in s [:compose/suites "smoke" :suite/id])) "map key becomes the id")
    (is (= :junit (get-in s [:compose/suites "smoke" :suite/report-format])))))

(deftest invalid-stack-values-are-refused
  (testing "a need with an unparseable range"
    (is (= :compose/invalid-config
           (:error (config/settings {:compose/projects {:shop {:project/dir "/w" :project/needs {:kafka {:version "latest"}}}}}
                                    nil home)))))
  (testing "a provide without its service"
    (is (= :compose/invalid-config
           (:error (config/settings {:compose/projects {:infra {:project/dir "/w" :project/provides {:kafka {:version "3"}}}}}
                                    nil home)))))
  (testing "a suite with neither image nor command"
    (is (= :compose/invalid-config
           (:error (config/settings {:compose/suites [{:suite/id "x" :suite/projects ["shop"]}]} nil home)))))
  (testing "a stack with no consumers"
    (is (= :compose/invalid-config
           (:error (config/settings {:compose/stacks [{:stack/id "s" :stack/consumers []}]} nil home))))))

(deftest version-ranges
  (doseq [ok ["*" "3" "3.7" "3.7.1" "^3" "^3.7" "~3.7" ">=3.5 <4" "=3.7.1" ">2"]]
    (is (nil? (schema/explain schema/VersionRange ok)) ok))
  (doseq [bad ["" "latest" "^" ">=3.5<4" "3.x" "v3"]]
    (is (some? (schema/explain schema/VersionRange bad)) bad)))

(deftest instance-resolution-and-wiring-shapes
  (let [shared {:instance/id "infra/kafka" :instance/capability "kafka" :instance/provider "infra"
                :instance/service "kafka" :instance/version "3.7" :instance/share? true
                :instance/consumers ["shop" "billing"] :instance/aliases ["kafka"]
                :instance/compose-project "infra"}
        private {:instance/id "infra/kafka@legacy" :instance/capability "kafka" :instance/provider "infra"
                 :instance/service "kafka" :instance/share? false
                 :instance/consumers ["legacy"] :instance/aliases ["kafka"]
                 :instance/compose-project "infra-legacy-r-01"}]
    (is (nil? (schema/explain schema/Instance shared)))
    (is (nil? (schema/explain schema/Resolution
                              {:resolution/instances {"infra/kafka" shared "infra/kafka@legacy" private}
                               :resolution/bindings {"shop" {"kafka" "infra/kafka"}
                                                     "billing" {"kafka" "infra/kafka"}
                                                     "legacy" {"kafka" "infra/kafka@legacy"}}})))
    (is (nil? (schema/explain schema/Wiring
                              {:wiring/consumer "shop" :wiring/run "r-01"
                               :wiring/network "hive-r-01-shop"
                               :wiring/attach [{:instance "infra/kafka" :aliases ["kafka"]}]
                               :wiring/env {"KAFKA_BROKERS" "kafka:9092" "HIVE_RUN_ID" "r-01"
                                            "KAFKA_TOPIC_PREFIX" "r-01.shop."
                                            "PG_PASSWORD" {:pass "shop/pg"}}
                               :wiring/prefix "r-01.shop."})))
    (is (some? (schema/explain schema/Wiring
                               {:wiring/consumer "shop" :wiring/run "r-01" :wiring/network "default"
                                :wiring/attach [] :wiring/env {} :wiring/prefix "r-01.shop."}))
        "a consumer attaches only to its view network hive-<run>-<consumer>")
    (is (some? (schema/explain schema/Instance (dissoc shared :instance/aliases)))
        "an instance always names the aliases it answers to")))

(def gen-run-id
  (gen/fmap (fn [[c cs]] (apply str c cs))
            (gen/tuple (gen/elements (seq "abcdefghijklmnopqrstuvwxyz0123456789"))
                       (gen/vector (gen/elements (seq "abcdefghijklmnopqrstuvwxyz0123456789-")) 2 39))))

(defspec run-ids-fit-compose-project-and-network-names 200
  (prop/for-all [run gen-run-id]
    (and (m/validate schema/RunId run)
         (re-matches #"^[a-z0-9][a-z0-9_-]*$" (str "infra-legacy-" run))
         (nil? (schema/explain schema/Instance
                               {:instance/id "p/c@x" :instance/capability "c" :instance/provider "p"
                                :instance/service "c" :instance/share? false :instance/consumers ["x"]
                                :instance/aliases ["c"] :instance/compose-project (str "p-x-" run)})))))

(def gen-need
  (gen/let [version (gen/elements ["*" "3" "^3" "~3.7" ">=3.5 <4"])
            share? gen/boolean]
    {:version version :share? share?}))

(defspec any-needs-map-keeps-a-project-valid 100
  (prop/for-all [needs (gen/map (gen/elements ["kafka" "postgres" "redis" "otel" "s3"]) gen-need)]
    (some? (:ok (config/settings {:compose/projects {:shop {:project/dir "/w" :project/needs needs}}} nil home)))))
