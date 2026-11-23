(ns hive-compose.pipeline.build-test
  "`compose build` through the addon's dispatch over the stub engine (card
   20261123112858-6f2c31bc): the effective inputs are used, the default
   selection and narrowing, the two refusals, running-old-image, one build
   per compose project at a time, the failure answer; and the CLI argv."
  (:require [clojure.data.json :as json]
            [clojure.test :refer [deftest is testing]]
            [hive-compose.adapter.cli :as cli]
            [hive-compose.addon :as addon]
            [hive-compose.api :as api]
            [hive-compose.stub :as stub]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def model
  {"postgres" {:image "postgres:16"}
   "sisf-payment" {:deps ["postgres"] :build? true}
   "sisf-sync" {:deps ["postgres"] :build? true :image "registry.local/sisf-sync:dev"}
   "envoy" {:deps ["postgres"]}})

(def project
  {:project/id "dc" :project/dir "/w/dc" :project/files ["docker-compose.yml"]
   :project/env {"SISF_PAYMENT_SRC" "../../wt/dc-staging"}})

(def flow-payment
  {:profile/id "flow-payment" :profile/dir "/w/pv01" :profile/project "dc"
   :profile/files ["docker-compose.yml" "docker-compose.override.yml"]
   :profile/env {"SISF_PAYMENT_SRC" "../../wt/pv01-merge-sisf-payment"}
   :profile/services ["sisf-payment"]})

(defn- session [presets f]
  (let [e (stub/engine {} {} {"dc" model})
        state-file (str (System/getProperty "java.io.tmpdir") "/hc-build-" (System/nanoTime) ".edn")
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
          :a a
          :call (fn [params]
                  (let [res ((:handler (addon/tool a)) params)]
                    {:res (json/read-str (get-in res [:content 0 :text]) :key-fn keyword)
                     :error? (boolean (:isError res))}))})
      (finally
        (.shutdown! ^hive_addon.protocol.IAddon a)
        (.delete (java.io.File. state-file))))))

(defn- last-build [e] (peek (stub/builds e)))

(deftest a-target-held-by-a-preset-in-another-dir-builds-from-that-dir
  (session [flow-payment]
           (fn [{:keys [call e]}]
             (is (not (:error? (call {"command" "up" "profile" "flow-payment"}))))
             (let [{:keys [res error?]} (call {"command" "build" "target" "dc/sisf-payment"})
                   p (:profile (last-build e))]
               (is (not error?) (pr-str res))
               (is (= "/w/pv01" (:profile/dir p)) "the holder's checkout, not the project dir")
               (is (= ["docker-compose.yml" "docker-compose.override.yml"] (:profile/files p)))
               (is (= "../../wt/pv01-merge-sisf-payment" (get-in p [:profile/env "SISF_PAYMENT_SRC"])))
               (is (= "dc" (:project res)) "the compose project name, so the tag is dc-sisf-payment")
               (is (= "/w/pv01" (:dir res)))
               (is (= [{:service "sisf-payment" :image "dc-sisf-payment"
                        :id (get (stub/image-ids e) "dc-sisf-payment")}]
                      (:built res)))
               (is (re-matches #"sha256:\d{64}" (get-in res [:built 0 :id])))
               (is (integer? (:duration-ms res))))))
  (testing "nobody holds it: the project's own inputs"
    (session []
             (fn [{:keys [call e]}]
               (is (not (:error? (call {"command" "build" "target" "dc/sisf-payment"}))))
               (is (= "/w/dc" (:profile/dir (:profile (last-build e)))))))))

(deftest the-default-builds-the-closure-services-that-declare-build
  (session []
           (fn [{:keys [call e]}]
             (let [{:keys [res]} (call {"command" "build" "target" "dc/sisf-payment,sisf-sync"})]
               (is (= ["sisf-payment" "sisf-sync"] (:services (last-build e))) "postgres declares no build:")
               (is (= ["dc-sisf-payment" "registry.local/sisf-sync:dev"] (mapv :image (:built res))))
               (is (= {:no-cache? false :pull? false} (:opts (last-build e))))))))

(deftest services-narrow-and-flags-pass-through
  (session []
           (fn [{:keys [call e]}]
             (let [{:keys [res error?]} (call {"command" "build" "target" "dc/sisf-payment,sisf-sync"
                                               "services" "sisf-sync" "no_cache" true "pull" "true"})]
               (is (not error?) (pr-str res))
               (is (= ["sisf-sync"] (:services (last-build e))))
               (is (= {:no-cache? true :pull? true} (:opts (last-build e))))
               (is (= ["sisf-sync"] (mapv :service (:built res))))))))

(deftest a-service-outside-the-section-or-without-build-is-refused-by-name
  (session []
           (fn [{:keys [call e]}]
             (let [{:keys [res error?]} (call {"command" "build" "target" "dc/sisf-payment" "services" "sisf-sync"})]
               (is error?)
               (is (= "unknown-names" (:error res)))
               (is (= ["sisf-sync"] (:services res))))
             (let [{:keys [res error?]} (call {"command" "build" "target" "dc/sisf-payment" "services" "postgres"})]
               (is error?)
               (is (= "invalid-param" (:error res)))
               (is (= ["postgres"] (:value res))))
             (is (empty? (stub/builds e)) "nothing was built"))))

(deftest nothing-to-build-is-an-ok-answer-saying-so
  (session []
           (fn [{:keys [call e]}]
             (let [{:keys [res error?]} (call {"command" "build" "target" "dc/envoy"})]
               (is (not error?))
               (is (= [] (:built res)))
               (is (re-find #"nothing to build" (:note res)))
               (is (empty? (stub/builds e)))))))

(deftest a-build-recreates-nothing-and-names-who-runs-the-old-image
  (session [flow-payment]
           (fn [{:keys [call e]}]
             (call {"command" "up" "profile" "flow-payment"})
             (let [cids (stub/container-ids e "dc")
                   ups (count (stub/ups e))
                   {:keys [res]} (call {"command" "build" "profile" "flow-payment"})]
               (is (= [{:section "flow-payment" :services ["sisf-payment"]}] (:running-old-image res)))
               (is (= cids (stub/container-ids e "dc")) "no container recreated")
               (is (= ups (count (stub/ups e))))
               (is (empty? (stub/restarts e)))
               (is (not-any? #(#{:stop :down :rm-by-name :stop-by-name} (first %)) (stub/calls e)))))))

(deftest one-build-per-compose-project-at-a-time
  (session []
           (fn [{:keys [call e]}]
             (let [entered (promise) gate (promise)]
               (stub/on-call! e (fn [c] (when (= :build (first c)) (deliver entered true) @gate)))
               (let [first-build (future (call {"command" "build" "target" "dc/sisf-payment"}))]
                 (is (deref entered 10000 false) "the first build runs")
                 (let [{:keys [res error?]} (call {"command" "build" "target" "dc/sisf-sync"})]
                   (is error?)
                   (is (= "build-busy" (:error res)))
                   (is (= "dc/sisf-payment" (get-in res [:running :section])))
                   (is (re-find #"one at a time" (:message res))))
                 (deliver gate true)
                 (is (not (:error? (deref first-build 10000 {:error? true}))))
                 (stub/on-call! e nil)
                 (testing "once it ended the next build runs"
                   (is (not (:error? (call {"command" "build" "target" "dc/sisf-sync"})))))
                 (is (= 2 (count (stub/builds e)))))))))

(deftest a-failed-build-names-the-service-and-the-output-tail
  (session []
           (fn [{:keys [call e]}]
             (stub/build-fails! e (str (apply str (repeat 50 "#5 step\n"))
                                       "target sisf-sync: failed to solve: process \"/bin/sh -c make\" did not complete\n"))
             (let [{:keys [res error?]} (call {"command" "build" "target" "dc/sisf-payment,sisf-sync"})]
               (is error?)
               (is (= "build-failed" (:error res)))
               (is (= "sisf-sync" (:service res)))
               (is (= 30 (count (:output-tail res))))
               (is (re-find #"failed to solve" (peek (:output-tail res))))
               (is (= 17 (:exit res))))
             (testing "the slot is released after a failure"
               (stub/build-fails! e nil)
               (is (not (:error? (call {"command" "build" "target" "dc/sisf-payment"}))))))))

(deftest the-api-maps-the-build-categories
  (is (= 502 (api/http-status (r/err :compose/build-failed {}))))
  (is (= 409 (api/http-status (r/err :compose/build-busy {}))))
  (is (contains? (get addon/command-params "build") "services"))
  (is (= #{"target" "profile" "services" "no_cache" "pull" "env"} (get addon/command-params "build"))))

(deftest the-cli-builds-with-the-base-argv-of-the-section
  (let [p {:profile/id "dc/sisf-payment" :profile/dir "/w/pv01" :profile/project "dc"
           :profile/files ["a.yml" "b.yml"] :profile/compose-profiles ["backend"]}]
    (is (= ["docker" "compose" "--project-directory" "/w/pv01" "-p" "dc" "-f" "a.yml" "-f" "b.yml"
            "--profile" "backend" "build" "--no-cache" "--pull" "sisf-payment"]
           (cli/build-argv p ["sisf-payment"] {:no-cache? true :pull? true})))
    (is (= (into (cli/base-argv p) ["build" "x"]) (cli/build-argv p ["x"] {}))))
  (is (= {"api" {:build? true :image "dc-api"} "db" {:build? false :image "postgres:16"}
          "web" {:build? true :image "reg/web:1"}}
         (cli/build-facts (json/write-str {"services" {"api" {"build" {"context" "."}}
                                                       "db" {"image" "postgres:16"}
                                                       "web" {"build" "." "image" "reg/web:1"}}})
                          "dc"))))
