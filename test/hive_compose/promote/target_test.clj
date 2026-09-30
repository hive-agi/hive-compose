(ns hive-compose.promote.target-test
  (:require [clojure.test :refer [deftest is]]
            [hive-compose.promote.target :as target]
            [hive-compose.schema :as schema]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(deftest parse-targets
  (is (= {:project nil :names ["sisf-web"]} (target/parse "sisf-web")))
  (is (= {:project "sisf" :names ["sisf-web"]} (target/parse "sisf/sisf-web")))
  (is (= {:project "sisf" :names ["web" "crm"]} (target/parse " sisf/web, crm,web ")))
  (is (= {:project nil :names ["a" "b"]} (target/parse "a+b")))
  (is (= {:project nil :names []} (target/parse ""))))

(deftest section-ids-are-canonical
  (is (= "sisf/crm+web" (target/section-id "sisf" ["web" "crm"])))
  (is (= (target/section-id "p" ["b" "a"]) (target/section-id "p" ["a" "b"]))))

(deftest a-section-is-a-valid-profile
  (let [proj {:project/id "sisf" :project/dir "/w/dc" :project/files ["docker-compose.yml"]
              :project/ttl-minutes 20 :project/name "dc"}
        s (target/section proj ["sisf-web"] ["sisf-web"])]
    (is (nil? (schema/explain schema/Profile s)))
    (is (= "sisf/sisf-web" (:profile/id s)))
    (is (= ["sisf-web"] (:profile/services s)))
    (is (= "dc" (:profile/project s)))
    (is (= 20 (:profile/ttl-minutes s)))))

(deftest profile-members-exclude-always-on
  (is (= ["sisf-web"] (target/profile-members ["envoy" "postgres" "sisf-web"] ["postgres" "envoy"]))))

(deftest unknown-service-is-recognised
  (is (target/unknown-service? (r/err :compose/command-failed {:stderr "no such service: x"})))
  (is (not (target/unknown-service? (r/err :compose/command-failed {:stderr "daemon down"}))))
  (is (not (target/unknown-service? (r/ok [])))))
