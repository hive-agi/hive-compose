(ns hive-compose.pipeline.section-env-dir-test
  "A section's dir, files and env survive a recreate (card
   20261123112129-701a7d69, friction 20261122190320-62303266): a preset's
   :profile/env beats the project's :project/env, a call env is remembered by
   the section, `restart target=` runs from the live section holding the
   service, an allow_shared answer says per shared service whether it was
   recreated or kept, and status names the checkout under test."
  (:require [clojure.data.json :as json]
            [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.addon :as addon]
            [hive-compose.promote.ownership :as own]
            [hive-compose.promote.target :as target]
            [hive-compose.stub :as stub]))

;; SPDX-License-Identifier: MIT

(def model
  {"postgres" {}
   "sisf-payment" {:deps ["postgres"]}
   "sisf-sync" {:deps ["postgres"]}
   "envoy" {:deps ["postgres"]}})

(def project
  {:project/id "dc" :project/dir "/w/dc" :project/files ["docker-compose.yml" "docker-compose.override.yml"]
   :project/env {"SISF_PAYMENT_SRC" "../../wt/dc-staging-view-sisf-payment" "GW" "172.25.0.9"}})

(def preset-env
  {"SISF_PAYMENT_SRC" "../../wt/pv01-merge-sisf-payment"
   "SISF_SYNC_SRC" "../../wt/pv01-merge-sisf-sync"
   "GITEA_TOKEN" {:pass "Gitea/ci/token"}})

(def flow-payment
  {:profile/id "flow-payment" :profile/dir "/w/pv01" :profile/project "dc"
   :profile/files ["docker-compose.yml" "docker-compose.override.yml"]
   :profile/env preset-env
   :profile/services ["sisf-payment"]})

(def flow-sync
  (assoc flow-payment :profile/id "flow-sync" :profile/services ["sisf-sync"]))

(defn- session
  "An initialized addon over a stub engine; `f` gets {:call (fn [params] ..)
   :e engine}. Every call answers {:res parsed :error? bool}."
  [presets f]
  (let [e (stub/engine {} {} {"dc" model})
        state-file (str (System/getProperty "java.io.tmpdir") "/hc-envdir-" (System/nanoTime) ".edn")
        a (addon/make-addon {:compose/engine e
                             :compose/process-table nil
                             :compose/runner nil
                             :compose/state-file state-file
                             :compose/config-source (constantly {})
                             :compose/profiles-file "/nonexistent/profiles.edn"
                             :compose/profiles presets
                             :compose/projects [project]
                             :compose/swarm? false :compose/notify? false :compose/adopt? false
                             :compose/initial-delay-ms 3600000})]
    (try
      (.initialize! ^hive_addon.protocol.IAddon a {})
      (f {:e e
          :call (fn [params]
                  (let [res ((:handler (addon/tool a)) params)]
                    {:res (json/read-str (get-in res [:content 0 :text]) :key-fn keyword)
                     :error? (boolean (:isError res))}))})
      (finally
        (.shutdown! ^hive_addon.protocol.IAddon a)
        (.delete (java.io.File. state-file))))))

(defn- last-restart [e] (:profile (peek (stub/restarts e))))
(defn- last-up [e] (:profile (peek (stub/ups e))))

;; ---------------------------------------------------------------------------
;; the precedence law

(def gen-env
  (gen/map (gen/elements ["A_SRC" "B" "C" "D_SRC" "E"])
           (gen/one-of [gen/string-alphanumeric
                        (gen/fmap (fn [s] {:pass (str "p/" s)}) gen/string-alphanumeric)])
           {:max-elements 4}))

(defn- profile-gen [id dir]
  (gen/let [env gen-env remembered gen-env files (gen/vector (gen/elements ["a.yml" "b.yml"]) 0 2)]
    (cond-> {:profile/id id :profile/dir dir :profile/project "dc"}
      (seq files) (assoc :profile/files files)
      (seq env) (assoc :profile/env env)
      (seq remembered) (assoc :profile/call-env remembered))))

(defspec env-precedence-law 200
  (prop/for-all [p (profile-gen "dc/x" "/w/dc")
                 holder (gen/one-of [(gen/return nil) (profile-gen "flow" "/w/pv01")])
                 call-env gen-env]
    (let [out (target/effective-profile p {:holder holder :call-env call-env})
          ;; weakest to strongest
          layers [(if holder (:profile/env holder) (:profile/env p))
                  (:profile/call-env holder)
                  (:profile/call-env p)
                  call-env]
          expected (apply merge {} layers)
          inputs-from (or holder p)]
      (and (= expected (or (:profile/env out) {}))
           (every? (fn [[k v]] (= v (get (:profile/env out) k))) call-env)
           (= (:profile/dir inputs-from) (:profile/dir out))
           (= (:profile/files inputs-from) (:profile/files out))
           (= (:profile/id p) (:profile/id out))
           ;; what a call gave is remembered, so a later call re-applies it
           (= (merge (:profile/call-env holder) (:profile/call-env p) call-env)
              (or (:profile/call-env out) {}))))))

(deftest effective-profile-without-holder-or-env-is-the-profile
  (is (= flow-payment (target/effective-profile flow-payment {})))
  (is (= (target/project-profile project) (target/effective-profile (target/project-profile project) nil))))

;; ---------------------------------------------------------------------------
;; defect 1: a preset's env and a call's env survive a recreate

(deftest a-preset-env-beats-the-project-env-on-a-restart-of-its-service
  (session [flow-payment]
           (fn [{:keys [call e]}]
             (is (not (:error? (call {"command" "up" "profile" "flow-payment"}))))
             (let [{:keys [error? res]} (call {"command" "restart" "target" "dc/sisf-payment"
                                               "recreate" true "allow_shared" true})
                   p (last-restart e)]
               (is (not error?) (pr-str res))
               (is (= "../../wt/pv01-merge-sisf-payment" (get-in p [:profile/env "SISF_PAYMENT_SRC"]))
                   "the preset's value, not the project's")
               (is (= {:pass "Gitea/ci/token"} (get-in p [:profile/env "GITEA_TOKEN"])) "a reference stays a reference")
               (is (nil? (get-in p [:profile/env "GW"])) "no project value the section did not run with")))))

(deftest a-call-env-is-remembered-by-the-section
  (testing "a target section: a later restart keeps the env the up gave"
    (session []
             (fn [{:keys [call e]}]
               (is (not (:error? (call {"command" "up" "target" "dc/envoy" "env" {"GW" "192.168.2.106"}}))))
               (is (not (:error? (call {"command" "restart" "target" "dc/envoy" "recreate" true}))))
               (is (= "192.168.2.106" (get-in (last-restart e) [:profile/env "GW"]))))))
  (testing "a preset: a later up without env keeps it, a pass ref stays a ref"
    (session [flow-payment]
             (fn [{:keys [call e]}]
               (is (not (:error? (call {"command" "up" "profile" "flow-payment"
                                        "env" {"SISF_PAYMENT_SRC" "../../wt/hotfix" "X" {"pass" "a/b"}}}))))
               (is (not (:error? (call {"command" "up" "profile" "flow-payment"}))))
               (is (= "../../wt/hotfix" (get-in (last-up e) [:profile/env "SISF_PAYMENT_SRC"])))
               (is (= {:pass "a/b"} (get-in (last-up e) [:profile/env "X"])))
               (testing "a plain up of a preset never needs env= to keep its own values"
                 (is (= "../../wt/pv01-merge-sisf-sync" (get-in (last-up e) [:profile/env "SISF_SYNC_SRC"]))))))))

;; ---------------------------------------------------------------------------
;; defect 2: restart target= runs from the holding section's dir

(deftest restart-of-a-held-service-runs-from-the-holders-dir
  (session [flow-payment]
           (fn [{:keys [call e]}]
             (call {"command" "up" "profile" "flow-payment"})
             (is (not (:error? (call {"command" "restart" "target" "dc/sisf-payment"}))))
             (let [p (last-restart e)]
               (is (= "/w/pv01" (:profile/dir p)) "the preset's checkout, not the project dir")
               (is (= ["docker-compose.yml" "docker-compose.override.yml"] (:profile/files p)))
               (is (= ["sisf-payment"] (:profile/services p)))))))

(deftest restart-of-a-service-nobody-holds-uses-the-project
  (session [flow-payment]
           (fn [{:keys [call e]}]
             (is (not (:error? (call {"command" "restart" "target" "dc/envoy"}))))
             (is (= "/w/dc" (:profile/dir (last-restart e))))
             (is (= "../../wt/dc-staging-view-sisf-payment"
                    (get-in (last-restart e) [:profile/env "SISF_PAYMENT_SRC"]))))))

(deftest holders-that-disagree-are-an-error-and-profile-picks-one
  (session [flow-payment (assoc flow-sync :profile/dir "/w/other")]
           (fn [{:keys [call e]}]
             (call {"command" "up" "profile" "flow-payment"})
             (call {"command" "up" "profile" "flow-sync" "allow_shared" true})
             (let [{:keys [error? res]} (call {"command" "restart" "target" "dc/postgres"})]
               (is error?)
               (is (= "holders-disagree" (:error res)))
               (is (= ["flow-payment" "flow-sync"] (mapv :section (:holders res))))
               (is (= {:dir true} (get-in res [:holders 1 :differs])))
               (is (empty? (stub/restarts e)) "nothing was guessed"))
             (let [{:keys [error?]} (call {"command" "restart" "target" "dc/postgres" "profile" "flow-sync"})]
               (is (not error?))
               (is (= "/w/other" (:profile/dir (last-restart e))))))))

(deftest holders-that-agree-share-their-inputs
  (session [flow-payment flow-sync]
           (fn [{:keys [call e]}]
             (call {"command" "up" "profile" "flow-payment"})
             (call {"command" "up" "profile" "flow-sync"})
             (is (not (:error? (call {"command" "restart" "target" "dc/postgres"}))))
             (is (= "/w/pv01" (:profile/dir (last-restart e)))))))

(deftest pick-holder-is-pure
  (let [entry (fn [p services] {:project "dc" :services services :profile p})]
    (is (= {:none true} (own/pick-holder {} "dc" ["x"] nil)))
    (is (= "flow-payment"
           (:section (own/pick-holder {"flow-payment" (entry flow-payment ["sisf-payment"])} "dc" ["sisf-payment"] nil))))
    (is (= {:none true}
           (own/pick-holder {"flow-payment" (entry flow-payment ["sisf-payment"])} "dc" ["sisf-payment"] "other")))))

;; ---------------------------------------------------------------------------
;; report honesty: recreated or kept, from what the engine did

(deftest allow-shared-says-per-service-recreated-or-kept
  (session [flow-payment (assoc-in flow-sync [:profile/env "SISF_SYNC_SRC"] "../../wt/other-sync")]
           (fn [{:keys [call e]}]
             ;; postgres reads none of the *_SRC vars: a call differing only there leaves it alone
             (stub/reads! e "dc" "postgres" ["PGDATA"])
             (call {"command" "up" "profile" "flow-payment"})
             (let [{:keys [error? res]} (call {"command" "up" "profile" "flow-sync" "allow_shared" true})
                   row (first (:shared-overridden res))]
               (is (not error?) (pr-str res))
               (is (= "flow-payment" (:section row)))
               (is (= {:postgres "kept"} (:outcome row)) "the engine left it alone"))
             (let [{:keys [error? res]} (call {"command" "restart" "target" "dc/postgres" "profile" "flow-sync"
                                               "recreate" true "allow_shared" true})
                   rows (:shared-overridden res)]
               (is (not error?) (pr-str res))
               (is (= ["flow-payment" "flow-sync"] (mapv :section rows)))
               (is (every? #(= "recreate" (:reason %)) rows))
               (is (every? #(= {:postgres "recreated"} (:outcome %)) rows))))))

(deftest outcomes-are-pure
  (is (= {"a" "kept" "b" "recreated" "c" "recreated"}
         (own/outcomes ["a" "b" "c"] {"a" "1" "b" "2"} {"a" "1" "b" "3" "c" "4"}))))

;; ---------------------------------------------------------------------------
;; status names the checkout under test

(deftest status-shows-dir-files-and-env-names
  (session [flow-payment]
           (fn [{:keys [call]}]
             (call {"command" "up" "profile" "flow-payment" "env" {"EXTRA" "secret-ish"}})
             (let [{:keys [res]} (call {"command" "status"})
                   row (first (filter #(= "flow-payment" (:id %)) (:profiles res)))
                   text (json/write-str res)]
               (is (= "/w/pv01" (:dir row)))
               (is (= ["docker-compose.yml" "docker-compose.override.yml"] (:files row)))
               (is (= ["EXTRA" "GITEA_TOKEN" "SISF_PAYMENT_SRC" "SISF_SYNC_SRC"] (:env-keys row)))
               (is (= {:SISF_PAYMENT_SRC "../../wt/pv01-merge-sisf-payment"
                       :SISF_SYNC_SRC "../../wt/pv01-merge-sisf-sync"}
                      (:env-src row)))
               (is (not (re-find #"secret-ish" text)) "a non-_SRC value is never shown")
               (is (not (re-find #"Gitea/ci/token" text)) "nor a reference")))))

(deftest an-inactive-preset-row-carries-no-inputs
  (session [flow-payment]
           (fn [{:keys [call]}]
             (let [row (first (:profiles (:res (call {"command" "status"}))))]
               (is (not (contains? row :dir)))
               (is (not (contains? row :env-keys)))))))
