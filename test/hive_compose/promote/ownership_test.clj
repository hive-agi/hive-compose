(ns hive-compose.promote.ownership-test
  "The ownership decision over fixtures modelled on the 2026-11-13 incident
   (card 20261113112930-07a70584): the gestao-usuarios section held envoy,
   auth-service and the web relay of `dc`; the escala session brought its
   own section up with another compose file and HOST_GATEWAY_IP."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.promote.ownership :as own]))

;; SPDX-License-Identifier: MIT

(def base-files ["docker-compose.yml" "docker-compose.override.yml"])

(def gu-profile
  {:profile/id "dc/sisf-web" :profile/dir "/w/funeraria/dc" :profile/project "dc"
   :profile/files base-files
   :profile/env {"HOST_GATEWAY_IP" "172.25.0.9" "DB_PASSWORD" {:pass "dc/db"}}})

(def escala-profile
  {:profile/id "dc/sisf-escala" :profile/dir "/w/funeraria/dc" :profile/project "dc"
   :profile/files (conj base-files "docker-compose.escala-dev.yml")
   :profile/env {"HOST_GATEWAY_IP" "192.168.2.106" "DB_PASSWORD" {:pass "dc/db"}}})

(def active
  {"dc/sisf-web" {:profile/id "dc/sisf-web" :project "dc"
                  :services ["sisf-web-relay" "envoy"]
                  :needs ["auth-service" "envoy" "keycloak" "postgres" "sisf-web-relay"]
                  :profile gu-profile :started-at 0 :last-touch 0
                  :claims {"ling-gu05" {:since 0 :seen 0 :note "gestao-usuarios e2e"}}}
   "dc/inventory" {:profile/id "dc/inventory" :project "dc" :services ["inventory-frontend-dev"]
                   :profile (assoc gu-profile :profile/id "dc/inventory") :started-at 0 :last-touch 0}
   "shop/api" {:profile/id "shop/api" :project "shop" :services ["envoy"] :started-at 0 :last-touch 0}})

(def escala-up
  {:active active :self "dc/sisf-escala" :project "dc" :p escala-profile
   :services ["auth-service" "envoy" "keycloak" "postgres" "sisf-escala" "sisf-web-relay"]
   :recreate? false})

(deftest the-incident-is-refused-naming-holder-and-agent
  (let [[c & more] (own/conflicts escala-up)]
    (is (empty? more) "inventory holds none of these; shop is another compose project")
    (is (= "dc/sisf-web" (:section c)))
    (is (= ["ling-gu05"] (:agents c)))
    (is (= ["auth-service" "envoy" "keycloak" "postgres" "sisf-web-relay"] (:services c)))
    (is (= :config-differs (:reason c)))
    (is (= {:files true :env ["HOST_GATEWAY_IP"]} (:differs c)) "names variables, never values")
    (is (re-find #"held by section dc/sisf-web \(agent ling-gu05\)" (own/describe [c])))
    (is (not (re-find #"192\.168|172\.25" (own/describe [c]))))))

(deftest same-inputs-share-dependencies-freely
  (let [same (assoc escala-up :p (assoc gu-profile :profile/id "dc/sisf-escala"))]
    (is (= [] (own/conflicts same)) "compose keeps a container whose config hash is unchanged")))

(deftest recreate-always-conflicts-with-a-live-holder
  (let [same (assoc escala-up :p (assoc gu-profile :profile/id "dc/sisf-escala")
                    :services ["envoy"] :recreate? true)
        [c] (own/conflicts same)]
    (is (= :recreate (:reason c)))
    (is (= ["envoy"] (:services c)))
    (is (nil? (:differs c)))
    (is (re-find #"recreate=true would replace" (own/describe [c])))))

(deftest a-section-never-conflicts-with-itself
  (is (= [] (own/conflicts (assoc escala-up :self "dc/sisf-web" :recreate? true)))))

(deftest liveness
  (testing "orphaned and unused: the reaper takes it, it holds nothing"
    (let [act (assoc-in active ["dc/sisf-web" :orphaned-at] 5)]
      (is (= [] (own/conflicts (assoc escala-up :active act))))))
  (testing "orphaned but seen in use: still live, flagged"
    (let [act (-> active
                  (assoc-in ["dc/sisf-web" :orphaned-at] 5)
                  (update "dc/sisf-web" dissoc :claims)
                  (assoc-in ["dc/sisf-web" :usage] {:at 5 :in-use? true :by [{:agent "ling-x"} {:port 10000 :peers 1}]}))
          [c] (own/conflicts (assoc escala-up :active act))]
      (is (:orphaned? c))
      (is (= ["ling-x"] (:agents c)) "agents seen working in it stand as owners"))))

(deftest an-entry-without-a-snapshot-counts-as-different
  (let [act (update active "dc/sisf-web" dissoc :profile)
        [c] (own/conflicts (assoc escala-up :active act))]
    (is (= {:unknown true} (:differs c)))))

(deftest unclaimed-holder-reads-as-such
  (let [act (update active "dc/sisf-web" dissoc :claims)
        [c] (own/conflicts (assoc escala-up :active act))]
    (is (= [] (:agents c)))
    (is (re-find #"no claiming agent" (own/describe [c])))))

(deftest differences-are-pure-and-name-what-differs
  (is (= {} (own/differences (own/inputs gu-profile) (own/inputs gu-profile))))
  (is (= {:dir true} (own/differences (own/inputs gu-profile)
                                      (own/inputs (assoc gu-profile :profile/dir "/other/dc")))))
  (is (= {} (own/differences (own/inputs gu-profile)
                             (own/inputs (assoc gu-profile :profile/dir "/w/funeraria/dc/"))))
      "a trailing slash is the same directory")
  (is (= {:env ["DB_PASSWORD"]}
         (own/differences (own/inputs gu-profile)
                          (own/inputs (assoc-in gu-profile [:profile/env "DB_PASSWORD"] {:pass "dc/other"}))))
      "references compare by rendering"))
