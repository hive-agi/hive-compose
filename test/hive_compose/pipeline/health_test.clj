(ns hive-compose.pipeline.health-test
  "status says up front when compose rejects a project's config; projects
   tells configured compose files from the ones docker recorded in labels."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.pipeline.ops :as ops]
            [hive-compose.stub :as stub]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- ctx [engine projects active]
  {:engine engine
   :settings {:compose/default-ttl-minutes 60
              :compose/default-idle-action :stop
              :compose/profiles {}
              :compose/projects projects}
   :state (atom {:active active :current nil})
   :now (constantly 0)})

(def dc {"dc" {:project/id "dc" :project/dir "/w/dc"
               :project/files ["docker-compose.yml" "docker-compose.override.yml"]}})

(deftest status-reports-a-rejected-project-config-up-front
  (let [engine (stub/engine {} {"dc" #{"web"}} {"dc" {"web" {}}})
        active {"dc/web" {:profile/id "dc/web" :project "dc" :services ["web"]
                          :started-at 0 :last-touch -7200000
                          :profile {:profile/id "dc/web" :profile/dir "/w/dc" :profile/services ["web"]}}}
        c (ctx engine dc active)]
    (testing "a healthy project"
      (let [s (ops/status c)]
        (is (:ok? s))
        (is (true? (get-in s [:config "dc" :ok?])))
        (is (nil? (:problem s)))))
    (testing "compose rejecting the config (a missing required variable)"
      (stub/fail! engine :listed)
      (let [s (ops/status c)
            row (first (filter #(= "dc/web" (:id %)) (:profiles s)))]
        (is (false? (:ok? s)))
        (is (false? (get-in s [:config "dc" :ok?])))
        (is (r/err? (get-in s [:config "dc" :error])) "the error compose answered is shown")
        (is (string? (:problem s)))
        (is (false? (:config-ok? row)))
        (is (true? (:reap-blocked? row)) "an overdue section of a broken project does not read as healthy")))))

(deftest projects-tells-configured-files-from-label-files
  (let [engine (stub/engine {} {} {})
        c (ctx engine dc {})]
    (swap! (:world engine) assoc :ls
           [{:name "dc" :status "running(3)"
             :config-files "/w/dc/docker-compose.yml,/old/wt/docker-compose.override.yml"}
            {:name "other" :status "running(1)" :config-files "/x/compose.yml"}])
    (let [[d o] (:ok (ops/projects c))]
      (is (= ["/w/dc/docker-compose.yml" "/old/wt/docker-compose.override.yml"] (:label-files d)))
      (is (= [{:id "dc" :files ["/w/dc/docker-compose.yml" "/w/dc/docker-compose.override.yml"]}]
             (:configured d)))
      (is (= ["/old/wt/docker-compose.override.yml"] (:stale-files d)))
      (is (false? (:files-match? d)))
      (is (nil? (:configured o)) "an unconfigured project is only listed")
      (is (= ["/x/compose.yml"] (:label-files o))))
    (testing "labels matching the configured files"
      (swap! (:world engine) assoc :ls
             [{:name "dc" :status "running(3)"
               :config-files "/w/dc/docker-compose.yml,/w/dc/docker-compose.override.yml"}])
      (let [[d] (:ok (ops/projects c))]
        (is (= [] (:stale-files d)))
        (is (true? (:files-match? d)))))))
