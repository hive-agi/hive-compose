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
           (:error (config/settings {:compose/stacks [{:stack/id "s" :stack/consumers []}]} nil home)))))
  (testing "malformed stacks and suites are refused, never thrown on"
    (doseq [cfg [{:compose/stacks [{:stack/consumers ["shop"]}]}
                 {:compose/stacks "oops"}
                 {:compose/stacks {:s "oops"}}
                 {:compose/suites [{:suite/image "x"}]}
                 {:compose/suites 42}]]
      (is (= :compose/invalid-config (:error (config/settings cfg nil home))) (pr-str cfg)))))

(deftest via-from-json-is-a-keyword
  (is (= :toxiproxy
         (get-in (:ok (config/settings {:compose/projects {:shop {:project/dir "/w"
                                                                  :project/needs {"kafka" {:via "toxiproxy"}}}}}
                                       nil home))
                 [:compose/projects "shop" :project/needs "kafka" :via]))))

(deftest version-ranges
  (doseq [ok ["*" "3" "3.7" "3.7.1" "^3" "^3.7" "~3" "~3.7" ">=3.5 <4" "=3.7.1" ">2" ">=3.7.0-rc1" "^1.0.0-rc.1+build.5"]]
    (is (nil? (schema/explain schema/VersionRange ok)) ok))
  (doseq [bad ["" "latest" "^" ">=3.5<4" "3.x" "v3" "3\n" "^3 " "* *"]]
    (is (some? (schema/explain schema/VersionRange bad)) (pr-str bad))))

(deftest trailing-newlines-are-refused
  (is (some? (schema/explain schema/CapabilityId "kafka\n")))
  (is (some? (schema/explain schema/Version "3.7\n")))
  (is (some? (schema/explain schema/RunId "abc\n")))
  (is (nil? (schema/explain schema/Version "1.0.0-rc.1+build.5"))))

(deftest docker-names
  (is (= "hive-r01-billing-api" (schema/view-network "r01" "billing/api")))
  (is (= "hive-r01-suite-smoke" (schema/view-network "r01" "suite:smoke")))
  (is (= "infra-shop-web-api-r01" (schema/private-project "infra" "Shop/web+api" "r01")))
  (is (some? (schema/explain schema/RunId "ci-1")) "a run id holds no '-'"))

(def wiring
  {:wiring/consumer "shop" :wiring/run "r01"
   :wiring/network "hive-r01-shop"
   :wiring/attach [{:instance "infra/kafka" :aliases ["kafka"]}
                   {:instance "db/postgres@shop" :aliases ["postgres"]}]
   :wiring/env {"KAFKA_BROKERS" "kafka:9092" "HIVE_RUN_ID" "r01"
                "KAFKA_TOPIC_PREFIX" "r01.shop."
                "PG_PASSWORD" {:pass "shop/pg"}}
   :wiring/prefix "r01.shop."})

(deftest instance-resolution-and-wiring-shapes
  (let [shared {:instance/id "infra/kafka" :instance/capability "kafka" :instance/provider "infra"
                :instance/service "kafka" :instance/version "3.7" :instance/share? true
                :instance/consumers ["shop" "billing"] :instance/aliases ["kafka"]
                :instance/compose-project "infra"}
        private {:instance/id "infra/kafka@legacy" :instance/capability "kafka" :instance/provider "infra"
                 :instance/service "kafka" :instance/share? false
                 :instance/consumers ["legacy"] :instance/aliases ["kafka"]
                 :instance/compose-project (schema/private-project "infra" "legacy" "r01")}]
    (is (nil? (schema/explain schema/Instance shared)))
    (is (nil? (schema/explain schema/Instance private)))
    (is (nil? (schema/explain schema/Resolution
                              {:resolution/instances {"infra/kafka" shared "infra/kafka@legacy" private}
                               :resolution/bindings {"shop" {"kafka" "infra/kafka"}
                                                     "billing" {"kafka" "infra/kafka"}
                                                     "legacy" {"kafka" "infra/kafka@legacy"}}})))
    (is (nil? (schema/explain schema/Wiring wiring)))
    (is (nil? (schema/explain schema/Wiring (assoc wiring :wiring/services ["api"]))))
    (testing "the alias model is enforced"
      (is (some? (schema/explain schema/Wiring (assoc wiring :wiring/network "default")))
          "a consumer attaches only to its view network")
      (is (some? (schema/explain schema/Wiring (assoc wiring :wiring/network "hive-other-shop")))
          "the view network of another run")
      (is (some? (schema/explain schema/Wiring (assoc wiring :wiring/consumer "billing")))
          "the view network of another consumer")
      (is (some? (schema/explain schema/Wiring (update wiring :wiring/attach conj
                                                       {:instance "x/kafka" :aliases ["kafka"]})))
          "two instances under one alias would make DNS ambiguous")
      (is (some? (schema/explain schema/Instance (assoc private :instance/consumers ["legacy" "shop"])))
          "a private instance has one consumer")
      (is (some? (schema/explain schema/Instance (dissoc private :instance/compose-project)))
          "a private instance names its compose project")
      (is (some? (schema/explain schema/Instance (dissoc shared :instance/aliases)))
          "an instance always names the aliases it answers to"))))

(def gen-run-id
  (gen/fmap #(apply str %)
            (gen/vector (gen/elements (seq "abcdefghijklmnopqrstuvwxyz0123456789")) 3 40)))

(def gen-consumer
  (gen/fmap (fn [[c cs]] (apply str c cs))
            (gen/tuple (gen/elements (seq "abcXYZ019"))
                       (gen/vector (gen/elements (seq "abcXYZ019_.+/:-")) 0 12))))

(defspec docker-names-are-valid-and-runs-never-collide 200
  (prop/for-all [r1 gen-run-id r2 gen-run-id c1 gen-consumer c2 gen-consumer]
    (and (m/validate schema/RunId r1)
         (m/validate schema/ConsumerId c1)
         (m/validate schema/ComposeProjectName (schema/private-project "infra" c1 r1))
         (nil? (schema/explain schema/Wiring (assoc wiring :wiring/run r1 :wiring/consumer c1
                                                    :wiring/network (schema/view-network r1 c1))))
         ;; a RunId holds no '-', so the run is recoverable from either name
         (or (= r1 r2)
             (not= (schema/view-network r1 c1) (schema/view-network r2 c2)))
         (or (= r1 r2)
             (not= (schema/private-project "infra" c1 r1) (schema/private-project "infra" c2 r2))))))

(def gen-need
  (gen/let [version (gen/elements ["*" "3" "^3" "~3.7" ">=3.5 <4"])
            share? gen/boolean]
    {:version version :share? share?}))

(def gen-needs
  (gen/map (gen/elements ["kafka" "postgres" "redis" "otel" "s3"]) gen-need))

(defspec any-needs-map-keeps-a-project-valid 100
  (prop/for-all [needs gen-needs]
    (some? (:ok (config/settings {:compose/projects {:shop {:project/dir "/w" :project/needs needs}}} nil home)))))

(defspec keyword-and-string-capability-keys-give-the-same-settings 100
  (prop/for-all [needs gen-needs]
    (let [kw (update-keys needs keyword)
          cfg (fn [n] {:compose/projects {:shop {:project/dir "/w" :project/needs n}}
                       :compose/stacks [{:stack/id "s" :stack/consumers ["shop"] :stack/needs n}]})]
      (= (config/settings (cfg needs) nil home) (config/settings (cfg kw) nil home)))))

(defspec normalizing-twice-changes-nothing 100
  (prop/for-all [needs gen-needs]
    (let [stacks [{:stack/id :s :stack/consumers ["shop"] :stack/needs (update-keys needs keyword)}]
          suites {:smoke {:suite/image "i" :suite/needs needs :suite/dir "~/t" :suite/report-format "tap"}}
          once-st (config/normalize-stacks stacks)
          once-su (config/normalize-suites home suites)]
      (and (= once-st (config/normalize-stacks once-st))
           (= once-su (config/normalize-suites home once-su))))))

(def gen-bad-range
  (gen/one-of [(gen/elements ["latest" "v3" "3.x" "^" "~" ">=" "3..7" ">=3<4" "x"])
               (gen/fmap #(str % "\n") (gen/elements ["3" "^3" "*"]))
               (gen/fmap #(str "^" %) gen/string-alphanumeric)]))

(defspec unparseable-ranges-are-refused 100
  (prop/for-all [bad gen-bad-range]
    (or (re-matches #"\^[0-9]+([-+][0-9A-Za-z.+-]+)?" bad)
        (= :compose/invalid-config
           (:error (config/settings {:compose/projects {:shop {:project/dir "/w" :project/needs {"kafka" {:version bad}}}}}
                                    nil home))))))
