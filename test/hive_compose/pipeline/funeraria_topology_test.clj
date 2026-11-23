(ns hive-compose.pipeline.funeraria-topology-test
  "funeraria's live topology on the stub engine (PR #62 consumer check): three
   active presets in project dc. flow-payment runs from /w/staging and holds
   sisf-payment and sisf-ledger; flow-sync and pv01-e2e run from /w/gate and
   share postgres, keycloak and redis with it. Only the holders of the NAMED
   services choose where a call runs from; a dependency the other sections
   hold is protected by the ownership guard, not by that choice."
  (:require [clojure.data.json :as json]
            [clojure.test :refer [deftest is testing]]
            [hive-compose.addon :as addon]
            [hive-compose.stub :as stub]))

;; SPDX-License-Identifier: MIT

(def model
  {"postgres" {} "keycloak" {} "redis" {}
   "sisf-payment" {:deps ["postgres" "keycloak"]}
   "sisf-ledger" {:deps ["postgres"]}
   "sisf-sync" {:deps ["postgres" "keycloak"]}
   "sisf-payment-e2e" {:deps ["postgres" "keycloak" "redis"]}})

(def project
  {:project/id "dc" :project/dir "/w/dc" :project/files ["docker-compose.yml"]
   :project/env {"SISF_PAYMENT_SRC" "../../wt/dc-staging-view-sisf-payment"}})

(defn- preset [id dir services src]
  {:profile/id id :profile/dir dir :profile/project "dc"
   :profile/files ["docker-compose.yml" "docker-compose.override.yml"]
   :profile/env {"SISF_PAYMENT_SRC" src}
   :profile/services services})

(def presets
  [(preset "flow-payment" "/w/staging" ["sisf-payment" "sisf-ledger" "postgres" "keycloak" "redis"]
           "../../wt/pv01-merge-sisf-payment")
   (preset "flow-sync" "/w/gate" ["sisf-sync" "postgres" "keycloak" "redis"] "../../wt/gate-sisf-payment")
   (preset "pv01-e2e" "/w/gate" ["sisf-payment-e2e" "postgres" "keycloak" "redis"] "../../wt/gate-sisf-payment")])

(defn- session [f]
  (let [e (stub/engine {} {} {"dc" model})
        state-file (str (System/getProperty "java.io.tmpdir") "/hc-funeraria-" (System/nanoTime) ".edn")
        a (addon/make-addon {:compose/engine e :compose/process-table nil :compose/runner nil
                             :compose/state-file state-file :compose/config-source (constantly {})
                             :compose/profiles-file "/nonexistent/profiles.edn"
                             :compose/profiles presets :compose/projects [project]
                             :compose/swarm? false :compose/notify? false :compose/adopt? false
                             :compose/initial-delay-ms 3600000})
        call (fn [params]
               (let [res ((:handler (addon/tool a)) params)]
                 {:res (json/read-str (get-in res [:content 0 :text]) :key-fn keyword)
                  :error? (boolean (:isError res))}))]
    (try
      (.initialize! ^hive_addon.protocol.IAddon a {})
      ;; the live state: three sections up, sharing postgres, keycloak and redis
      ;; (the gate sections were brought up with allow_shared, as funeraria does)
      (is (not (:error? (call {"command" "up" "profile" "flow-payment"}))))
      (is (not (:error? (call {"command" "up" "profile" "flow-sync" "allow_shared" true}))))
      (is (not (:error? (call {"command" "up" "profile" "pv01-e2e" "allow_shared" true}))))
      (f {:e e :call call})
      (finally
        (.shutdown! ^hive_addon.protocol.IAddon a)
        (.delete (java.io.File. state-file))))))

(defn- engine-calls
  "Engine calls that act on containers since `n` recorded calls."
  [e n]
  (filterv #(#{:up :restart :recreate} (first %)) (drop n (stub/calls e))))

(deftest one-restart-in-place-runs-from-the-holder-of-the-named-service
  (session
   (fn [{:keys [e call]}]
     (let [n (count (stub/calls e))
           {:keys [error? res]} (call {"command" "restart" "target" "dc/sisf-payment"})
           p (:profile (peek (stub/restarts e)))]
       (is (not error?) (pr-str res))
       (is (= "/w/staging" (:profile/dir p)) "flow-payment holds sisf-payment; the /w/gate holders of postgres do not decide")
       (is (= "../../wt/pv01-merge-sisf-payment" (get-in p [:profile/env "SISF_PAYMENT_SRC"])))
       (is (= [[:restart "dc" ["sisf-payment"]]] (engine-calls e n)) "nothing but sisf-payment")))))

(deftest two-restart-recreate-is-guarded-then-runs-from-the-holder
  (session
   (fn [{:keys [e call]}]
     (testing "the guard: flow-payment holds sisf-payment, so a recreate by another section needs allow_shared"
       (let [n (count (stub/calls e))
             {:keys [error? res]} (call {"command" "restart" "target" "dc/sisf-payment" "recreate" true})]
         (is error?)
         (is (= "held-by-another-section" (:error res)))
         (is (= [{:section "flow-payment" :services ["sisf-payment"] :reason "recreate"}]
                (mapv #(select-keys % [:section :services :reason]) (:holders res)))
             "only the named service; postgres held by /w/gate is not why")
         (is (= [] (engine-calls e n)))))
     (testing "with allow_shared: recreated from /w/staging, only sisf-payment"
       (let [before (stub/container-ids e "dc")
             n (count (stub/calls e))
             {:keys [error? res]} (call {"command" "restart" "target" "dc/sisf-payment" "recreate" true "allow_shared" true})
             p (:profile (peek (stub/restarts e)))
             after (stub/container-ids e "dc")]
         (is (not error?) (pr-str res))
         (is (= "/w/staging" (:profile/dir p)))
         (is (= [[:recreate "dc" ["sisf-payment"]]] (engine-calls e n)))
         (is (= (dissoc before "sisf-payment") (dissoc after "sisf-payment")) "every other container kept")
         (is (not= (get before "sisf-payment") (get after "sisf-payment"))))))))

(deftest three-up-target-from-the-holder-its-dependencies-guarded
  (session
   (fn [{:keys [e call]}]
     (testing "a plain up also applies to its depends_on (postgres, keycloak), which the /w/gate sections hold with other inputs: the guard refuses"
       (let [n (count (stub/calls e))
             {:keys [error? res]} (call {"command" "up" "target" "dc/sisf-payment"})]
         (is error?)
         (is (= "held-by-another-section" (:error res)))
         (is (= #{"flow-sync" "pv01-e2e"} (set (map :section (:holders res)))))
         (is (every? #(= "config-differs" (:reason %)) (:holders res)))
         (is (every? #(= #{"postgres" "keycloak"} (set (:services %))) (:holders res)))
         (is (not-any? #(= "flow-payment" (:section %)) (:holders res)) "the holder of sisf-payment agrees: same inputs")
         (is (= [] (engine-calls e n)))))
     (testing "no_deps: only sisf-payment, from /w/staging, nothing refused"
       (let [before (stub/container-ids e "dc")
             n (count (stub/calls e))
             {:keys [error? res]} (call {"command" "up" "target" "dc/sisf-payment" "no_deps" true})
             p (:profile (peek (stub/ups e)))]
         (is (not error?) (pr-str res))
         (is (= "/w/staging" (:profile/dir p)))
         (is (= "../../wt/pv01-merge-sisf-payment" (get-in p [:profile/env "SISF_PAYMENT_SRC"])))
         (is (= [[:up "dc" ["sisf-payment"]]] (engine-calls e n)))
         (is (= before (stub/container-ids e "dc")) "same inputs as its holder: nothing recreated"))))))

(deftest four-restart-of-a-shared-dependency-names-the-disagreeing-holders
  (session
   (fn [{:keys [e call]}]
     (let [n (count (stub/calls e))
           {:keys [error? res]} (call {"command" "restart" "target" "dc/postgres"})]
       (is error?)
       (is (= "holders-disagree" (:error res)))
       (is (= ["flow-payment" "flow-sync" "pv01-e2e"] (mapv :section (:holders res))))
       (is (= [] (engine-calls e n)) "nothing guessed"))
     (testing "profile= beside the target picks the holder"
       (let [{:keys [error? res]} (call {"command" "restart" "target" "dc/postgres" "profile" "flow-sync"})
             p (:profile (peek (stub/restarts e)))]
         (is (not error?) (pr-str res))
         (is (= "/w/gate" (:profile/dir p)))
         (is (= ["postgres"] (:services (peek (stub/restarts e))))))))))
