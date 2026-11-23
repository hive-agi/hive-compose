(ns hive-compose.pipeline.ownership-test
  "up, switch and restart recreate=true refuse to take a service from under
   another live section (card 20261113112930-07a70584), through the tool."
  (:require [clojure.data.json :as json]
            [clojure.test :refer [deftest is]]
            [hive-compose.addon :as addon]
            [hive-compose.pipeline.ownership :as ownership]
            [hive-compose.stub :as stub]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def model
  {"envoy" {:deps ["auth-service" "sisf-web-relay"]}
   "auth-service" {}
   "sisf-web-relay" {}
   "sisf-escala" {:deps ["envoy"]}
   "inventory" {}})

(def projects
  [{:project/id "dc" :project/dir "/w/dc" :project/files ["a.yml"]
    :project/env {"HOST_GATEWAY_IP" "172.25.0.9"}}])

(defn- holder-entry
  "The gestao-usuarios section, as the state file keeps it."
  []
  {:profile/id "dc/envoy" :project "dc" :services ["auth-service" "envoy" "sisf-web-relay"]
   :profile {:profile/id "dc/envoy" :profile/dir "/w/dc" :profile/project "dc" :profile/files ["a.yml"]
             :profile/env {"HOST_GATEWAY_IP" "172.25.0.9"} :profile/services ["envoy"]}
   :started-at 0 :last-touch 0
   :claims {"ling-gu05" {:since 0 :seen 0}}})

(defn- tool-call [params]
  (let [e (stub/engine {} {"dc" #{"auth-service" "envoy" "sisf-web-relay"}} {"dc" model})
        state-file (str (System/getProperty "java.io.tmpdir") "/hc-own-" (System/nanoTime) ".edn")
        _ (spit state-file (pr-str {:active {"dc/envoy" (holder-entry)} :current "dc/envoy"}))
        a (addon/make-addon {:compose/engine e
                             :compose/process-table nil
                             :compose/runner nil
                             :compose/state-file state-file
                             :compose/config-source (constantly {})
                             :compose/profiles-file "/nonexistent/profiles.edn"
                             :compose/projects projects
                             :compose/swarm? false :compose/notify? false :compose/adopt? false
                             :compose/initial-delay-ms 3600000})]
    (try
      (.initialize! ^hive_addon.protocol.IAddon a {})
      (let [res ((:handler (addon/tool a)) params)]
        {:res (json/read-str (get-in res [:content 0 :text]) :key-fn keyword)
         :error? (:isError res)
         :calls (filterv #(#{:up :recreate :restart} (first %)) (stub/calls e))})
      (finally
        (.shutdown! ^hive_addon.protocol.IAddon a)
        (.delete (java.io.File. state-file))))))

(def other-gateway {"HOST_GATEWAY_IP" "192.168.2.106"})

(deftest up-with-other-env-is-refused-naming-the-holder
  (doseq [cmd ["up" "switch"]]
    (let [{:keys [res error? calls]} (tool-call {"command" cmd "target" "dc/sisf-escala" "env" other-gateway})]
      (is error? cmd)
      (is (= "held-by-another-section" (:error res)))
      (is (= [{:section "dc/envoy" :agents ["ling-gu05"]
               :services ["auth-service" "envoy" "sisf-web-relay"]
               :reason "config-differs" :differs {:env ["HOST_GATEWAY_IP"]}}]
             (:holders res)))
      (is (re-find #"agent ling-gu05" (:message res)))
      (is (re-find #"allow_shared=true" (:hint res)))
      (is (= [] calls) "nothing was started"))))

(deftest up-with-the-same-inputs-shares
  (let [{:keys [error? calls]} (tool-call {"command" "up" "target" "dc/sisf-escala"})]
    (is (not error?))
    (is (= 1 (count calls)))))

(deftest up-of-services-nobody-holds-goes-ahead
  (let [{:keys [error?]} (tool-call {"command" "up" "target" "dc/inventory" "env" other-gateway})]
    (is (not error?))))

(deftest restart-recreate-is-refused-plain-restart-is-not
  (let [{:keys [res error? calls]} (tool-call {"command" "restart" "target" "dc/envoy,sisf-escala" "recreate" true})]
    (is error?)
    (is (= "recreate" (get-in res [:holders 0 :reason])))
    (is (= ["envoy"] (get-in res [:holders 0 :services])))
    (is (= [] calls)))
  (let [{:keys [error? calls]} (tool-call {"command" "restart" "target" "dc/envoy,sisf-escala"})]
    (is (not error?))
    (is (= :restart (ffirst calls)))))

(deftest a-section-recreates-its-own-services
  (let [{:keys [error? calls]} (tool-call {"command" "restart" "target" "dc/envoy" "recreate" true})]
    (is (not error?) "the holder is the caller itself")
    (is (= :recreate (ffirst calls)))))

(deftest allow-shared-overrides-and-says-so
  (let [{:keys [res error? calls]} (tool-call {"command" "up" "target" "dc/sisf-escala"
                                               "env" other-gateway "allow_shared" "true"})]
    (is (not error?))
    (is (= [{:section "dc/envoy" :agents ["ling-gu05"]
             :services ["auth-service" "envoy" "sisf-web-relay"] :reason "config-differs"}]
           (mapv #(dissoc % :outcome) (:shared-overridden res))))
    (is (= #{"recreated" "kept"} (into #{"recreated" "kept"} (vals (:outcome (first (:shared-overridden res))))))
        "each shared service says what the engine did")
    (is (= 1 (count calls)))))

(deftest a-failed-check-never-blocks
  (let [e (stub/engine {} {} {"dc" model})
        ctx {:engine e :state (atom {:active {"dc/envoy" (holder-entry)}})}
        p {:profile/id "dc/x" :profile/dir "/w/dc" :profile/project "dc"}]
    (stub/fail! e :closure)
    (let [out (ownership/guarded ctx p :up false false #(r/ok {:started true}))]
      (is (r/ok? out))
      (is (= :stub/fault (get-in out [:ok :ownership-check :error]))))))

(deftest no-compose-query-when-nobody-else-holds-anything
  (let [e (stub/engine {} {} {"dc" model})
        ctx {:engine e :state (atom {:active {}})}]
    (is (= (r/ok []) (ownership/check ctx {:profile/id "dc/x" :profile/dir "/w/dc"} :up false)))
    (is (= [] (stub/calls e)))))
