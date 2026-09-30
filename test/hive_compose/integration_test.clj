(ns hive-compose.integration-test
  "Against the real docker daemon, with a throwaway two-profile compose project."
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [hive-compose.adapter.cli :as cli]
            [hive-compose.pipeline.ops :as ops]
            [hive-compose.port :as port]))

;; SPDX-License-Identifier: MIT

(def compose-yml
  "services:
  db:
    image: busybox:1.36
    command: [\"sleep\", \"600\"]
  api:
    image: busybox:1.36
    command: [\"sleep\", \"600\"]
    depends_on: [db]
  web:
    image: busybox:1.36
    command: [\"sleep\", \"600\"]
    depends_on: [db]
    profiles: [web]
")

(defn- project-dir []
  (let [d (str (java.nio.file.Files/createTempDirectory "hive-compose-it"
                                                        (make-array java.nio.file.attribute.FileAttribute 0)))]
    (spit (io/file d "compose.yml") compose-yml)
    d))

(deftest ^:integration switch-and-reap-against-docker
  (let [dir (project-dir)
        project (str "hivecomposeit" (System/currentTimeMillis))
        engine (cli/make-engine {})
        base {:profile/dir dir :profile/project project}
        clock (atom 0)
        c {:engine engine
           :settings {:compose/default-ttl-minutes 1
                      :compose/default-idle-action :stop
                      :compose/profiles {"api" (assoc base :profile/id "api" :profile/services ["api"])
                                         "web" (assoc base :profile/id "web" :profile/services ["web"]
                                                      :profile/compose-profiles ["web"])}}
           :state (atom {:active {} :current nil})
           :now #(deref clock)}
        running #(port/-running engine (assoc base :profile/id "any"))]
    (try
      (is (= ["api" "db"] (:services (:ok (ops/up! c "api")))))
      (is (= #{"api" "db"} (:ok (running))))
      (testing "switch keeps the shared db"
        (is (= ["api"] (-> (ops/switch! c "web") :ok :released first :services)))
        (is (= #{"db" "web"} (:ok (running)))))
      (testing "the reaper stops the idle profile"
        (reset! clock 120000)
        (is (= ["db" "web"] (-> (ops/reap! c) :reaped first :services)))
        (is (= #{} (:ok (running)))))
      (finally
        (port/-down! engine (assoc base :profile/compose-profiles ["web"]) ["api" "db" "web"])))))

(def sections-yml
  "services:
  worker:
    image: busybox:1.36
    command: [\"sleep\", \"600\"]
  db:
    image: busybox:1.36
    command: [\"sleep\", \"600\"]
  cache:
    image: busybox:1.36
    command: [\"sleep\", \"600\"]
  api:
    image: busybox:1.36
    command: [\"sleep\", \"600\"]
    profiles: [backend]
    depends_on:
      db:
        condition: service_started
      cache:
        condition: service_started
        required: false
")

(deftest ^:integration a-target-runs-only-its-yaml-closure
  (let [dir (str (java.nio.file.Files/createTempDirectory "hive-compose-sections"
                                                          (make-array java.nio.file.attribute.FileAttribute 0)))
        _ (spit (io/file dir "compose.yml") sections-yml)
        project (str "hivecomposesec" (System/currentTimeMillis))
        engine (cli/make-engine {})
        whole {:profile/id "whole" :profile/dir dir :profile/project project}
        c {:engine engine
           :settings {:compose/default-ttl-minutes 60
                      :compose/default-idle-action :stop
                      :compose/profiles {}
                      :compose/projects {"it" {:project/id "it" :project/dir dir :project/name project}}}
           :state (atom {:active {} :current nil})
           :now (constantly 0)}]
    (try
      (testing "a profile-gated service with a soft dependency"
        (let [p (:ok (ops/subject c {:target "api"}))]
          (is (= "it/api" (:profile/id p)))
          (is (= ["api" "cache" "db"] (:services (:ok (ops/up-profile! c p)))))
          (is (= #{"api" "cache" "db"} (:ok (port/-running engine whole))) "worker stays down")))
      (testing "the native profile name resolves to the same section content"
        (is (= ["api"] (:profile/services (:ok (ops/subject c {:target "backend"}))))))
      (testing "down takes only that section"
        (ops/down-profile! c (:ok (ops/subject c {:target "it/api"})) :down)
        (is (= #{} (:ok (port/-running engine whole)))))
      (finally
        (port/-down! engine (assoc whole :profile/compose-profiles ["backend"]) ["api" "cache" "db" "worker"])))))
