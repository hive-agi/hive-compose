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
   :e engine :state (fn [] state-atom)}. Every call answers {:res parsed
   :error? bool}. `extra` merges into the addon config."
  ([presets f] (session presets f {}))
  ([presets f extra]
   (let [e (stub/engine {} {} {"dc" model})
         state-file (str (System/getProperty "java.io.tmpdir") "/hc-envdir-" (System/nanoTime) ".edn")
         a (addon/make-addon (merge {:compose/engine e
                                     :compose/process-table nil
                                     :compose/runner nil
                                     :compose/state-file state-file
                                     :compose/config-source (constantly {})
                                     :compose/profiles-file "/nonexistent/profiles.edn"
                                     :compose/profiles presets
                                     :compose/projects [project]
                                     :compose/swarm? false :compose/notify? false :compose/adopt? false
                                     :compose/initial-delay-ms 3600000}
                                    extra))]
     (try
       (.initialize! ^hive_addon.protocol.IAddon a {})
       (f {:e e
           :state (fn [] (:state (:ctx @(:state a))))
           :call (fn [params]
                   (let [res ((:handler (addon/tool a)) params)]
                     {:res (json/read-str (get-in res [:content 0 :text]) :key-fn keyword)
                      :error? (boolean (:isError res))}))})
       (finally
         (.shutdown! ^hive_addon.protocol.IAddon a)
         (.delete (java.io.File. state-file)))))))

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
  (is (= {"a" "kept" "b" "recreated" "c" "created" "d" "absent"}
         (own/outcomes ["a" "b" "c" "d"] {"a" "1" "b" "2" "d" "5"} {"a" "1" "b" "3" "c" "4"}))))

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

;; ---------------------------------------------------------------------------
;; review #62: B1, B2, M1, M2

(deftest b1-a-plain-up-of-an-active-target-keeps-its-call-env-and-recreates-nothing
  (session []
           (fn [{:keys [call e]}]
             (is (not (:error? (call {"command" "up" "target" "dc/envoy" "env" {"GW" "192.168.2.106"}}))))
             (let [before (stub/container-ids e "dc")]
               (is (not (:error? (call {"command" "up" "target" "dc/envoy"}))))
               (is (= "192.168.2.106" (get-in (last-up e) [:profile/env "GW"])) "the remembered env, not the project's")
               (is (= before (stub/container-ids e "dc")) "nothing was recreated")))))

(deftest b2b-a-null-forgets-a-remembered-var
  (session [flow-payment]
           (fn [{:keys [call e]}]
             (call {"command" "up" "profile" "flow-payment" "env" {"SISF_PAYMENT_SRC" "../../wt/hotfix"}})
             (let [row #(first (filter (fn [r] (= "flow-payment" (:id r))) (:profiles (:res (call {"command" "status"})))))]
               (is (= ["SISF_PAYMENT_SRC"] (:call-env-keys (row))) "status says which keys a call gave")
               (is (not (:error? (call {"command" "up" "profile" "flow-payment"
                                        "env" {"SISF_PAYMENT_SRC" nil "NEVER_SET" nil}}))))
               (is (= "../../wt/pv01-merge-sisf-payment" (get-in (last-up e) [:profile/env "SISF_PAYMENT_SRC"]))
                   "back to the preset's value")
               (is (not (contains? (:profile/env (last-up e)) "NEVER_SET")) "a null for an unremembered var is a no-op")
               (is (= [] (:call-env-keys (row))))))))

(deftest b2c-a-reload-that-edits-a-declared-var-drops-its-remembered-value
  (let [declared (atom {:compose/profiles [flow-payment]})]
    (session []
             (fn [{:keys [call e]}]
               (call {"command" "up" "profile" "flow-payment"
                      "env" {"SISF_PAYMENT_SRC" "../../wt/hotfix" "SISF_SYNC_SRC" "../../wt/hotfix-sync"}})
               (swap! declared assoc-in [:compose/profiles 0 :profile/env "SISF_PAYMENT_SRC"] "../../wt/edited")
               (let [{:keys [res]} (call {"command" "reload"})]
                 (is (= {:flow-payment ["SISF_PAYMENT_SRC"]} (:call-env-dropped res))))
               (call {"command" "up" "profile" "flow-payment"})
               (is (= "../../wt/edited" (get-in (last-up e) [:profile/env "SISF_PAYMENT_SRC"])) "the edit is the newer intent")
               (is (= "../../wt/hotfix-sync" (get-in (last-up e) [:profile/env "SISF_SYNC_SRC"]))
                   "a remembered var whose declared value did not change stays"))
             {:compose/profiles nil :compose/config-source (fn [] @declared)})))

(defspec stale-call-env-keys-law 200
  (prop/for-all [old gen-env new gen-env remembered gen-env]
    (let [ks (set (target/stale-call-env-keys old new remembered))]
      (every? (fn [k] (= (contains? ks k)
                         (not= (get old k ::none) (get new k ::none))))
              (keys remembered)))))

(deftest m1-an-orphaned-holder-still-says-where-a-restart-comes-from
  (session [flow-payment]
           (fn [{:keys [call e state]}]
             (call {"command" "up" "profile" "flow-payment"})
             (swap! (state) assoc-in [:active "flow-payment" :orphaned-at] 1)
             (is (not (:error? (call {"command" "restart" "target" "dc/sisf-payment" "recreate" true}))))
             (is (= "/w/pv01" (:profile/dir (last-restart e))) "not the project dir"))))

(deftest m2-a-recreate-env-is-remembered-by-the-holding-section
  (session [flow-payment]
           (fn [{:keys [call e]}]
             ;; postgres reads none of the *_SRC vars, as in dc
             (stub/reads! e "dc" "postgres" ["PGDATA"])
             (call {"command" "up" "profile" "flow-payment"})
             (is (not (:error? (call {"command" "restart" "target" "dc/sisf-payment" "recreate" true "allow_shared" true
                                      "env" {"SISF_PAYMENT_SRC" "../../wt/hotfix"}}))))
             (let [before (stub/container-ids e "dc")]
               (call {"command" "up" "profile" "flow-payment"})
               (is (= "../../wt/hotfix" (get-in (last-up e) [:profile/env "SISF_PAYMENT_SRC"])))
               (is (= before (stub/container-ids e "dc")) "the holder's next up does not recreate back")))))

;; ---------------------------------------------------------------------------
;; review #62 M3: claim, adopt and up target= of a held service

(deftest m3a-a-claim-that-starts-a-section-uses-the-remembered-env-inside-the-guard
  (session [flow-payment]
           (fn [{:keys [call e]}]
             (call {"command" "up" "profile" "flow-payment" "env" {"SISF_SYNC_SRC" "../../wt/hotfix-sync"}})
             ;; its container died: a claim of the service must start it again
             (stub/halt! e "dc" ["sisf-payment"])
             (let [{:keys [error? res]} (call {"command" "claim" "target" "dc/sisf-payment" "agent" "ling-1"})
                   p (last-up e)]
               (is (not error?) (pr-str res))
               (is (some? (:started res)) "started, through up's guards")
               (is (= "/w/pv01" (:profile/dir p)) "from the holder's checkout, not the project dir")
               (is (= "../../wt/pv01-merge-sisf-payment" (get-in p [:profile/env "SISF_PAYMENT_SRC"])))
               (is (= "../../wt/hotfix-sync" (get-in p [:profile/env "SISF_SYNC_SRC"])) "with the remembered call env")))))

(deftest m3b-adopt-records-the-holders-inputs-or-marks-them-assumed
  (session [flow-payment]
           (fn [{:keys [call e state]}]
             (testing "nothing says how it was started: the preset, marked assumed"
               (stub/run-outside! e "dc" ["postgres" "sisf-payment"])
               (call {"command" "claim" "profile" "flow-payment" "agent" "ling-1"})
               (is (= :assumed (get-in @(state) [:active "flow-payment" :inputs])))
               (let [row (first (filter #(= "flow-payment" (:id %)) (:profiles (:res (call {"command" "status"})))))]
                 (is (= "assumed" (:inputs row)))))
             (testing "an assumed adoption is never trusted as a holder: restart target= falls to the project and the guard"
               (let [{:keys [error? res]} (call {"command" "restart" "target" "dc/sisf-payment" "recreate" true})]
                 (is error? "recreating a service whose inputs are only assumed needs allow_shared")
                 (is (= "held-by-another-section" (:error res)))
                 (is (empty? (stub/restarts e))))))))

(deftest m3b-adopt-takes-the-inputs-of-the-active-holder
  (session [flow-payment (assoc flow-sync :profile/services ["postgres"])]
           (fn [{:keys [call state]}]
             (call {"command" "up" "profile" "flow-sync" "env" {"SISF_PAYMENT_SRC" "../../wt/hotfix"}})
             (call {"command" "claim" "target" "dc/postgres" "agent" "ling-1"})
             (let [a (get-in @(state) [:active "dc/postgres"])]
               (is (some? a) (pr-str (keys (:active @(state)))))
               (is (= "/w/pv01" (get-in a [:profile :profile/dir])) "the holder's checkout, not /w/dc")
               (is (= "../../wt/hotfix" (get-in a [:profile :profile/env "SISF_PAYMENT_SRC"])))
               (is (nil? (:inputs a)) "known, not assumed")))))

(deftest m3c-up-target-of-a-held-service-never-rebuilds-from-project-defaults
  (session [flow-payment]
           (fn [{:keys [call e]}]
             (call {"command" "up" "profile" "flow-payment"})
             (let [{:keys [error? res]} (call {"command" "up" "target" "dc/sisf-payment"})]
               (is (not error?) (pr-str res))
               (is (= "/w/pv01" (:profile/dir (last-up e))) "the holder's checkout")
               (is (= "../../wt/pv01-merge-sisf-payment" (get-in (last-up e) [:profile/env "SISF_PAYMENT_SRC"])))))))

(deftest an-inactive-preset-row-carries-no-inputs
  (session [flow-payment]
           (fn [{:keys [call]}]
             (let [row (first (:profiles (:res (call {"command" "status"}))))]
               (is (not (contains? row :dir)))
               (is (not (contains? row :env-keys)))))))
