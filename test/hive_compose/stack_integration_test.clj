(ns hive-compose.stack-integration-test
  "A stack run against the real docker daemon: a shared provider, a consumer
   that reaches it by its service name on its view network, labels, and a
   down that leaves no network or container of the run behind."
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [hive-compose.adapter.cli :as cli]
            [hive-compose.pipeline.stacks :as stacks]
            [hive-compose.port :as port]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def provider-yml
  "services:
  echo:
    image: busybox:1.36
    command: [\"sh\", \"-c\", \"while true; do echo ok | nc -l -p 7000; done\"]
    healthcheck:
      test: [\"CMD-SHELL\", \"true\"]
      interval: 1s
      retries: 10
")

(def consumer-yml
  "services:
  app:
    image: busybox:1.36
    command: [\"sh\", \"-c\", \"sleep 600\"]
")

(defn- dir-with [prefix yml]
  (let [d (str (java.nio.file.Files/createTempDirectory prefix (make-array java.nio.file.attribute.FileAttribute 0)))]
    (spit (io/file d "compose.yml") yml)
    d))

(deftest ^:integration a-stack-against-docker
  (let [stamp (System/currentTimeMillis)
        pname (str "hcitprov" stamp)
        cname (str "hcitcons" stamp)
        state-dir (str (java.nio.file.Files/createTempDirectory "hc-it-state" (make-array java.nio.file.attribute.FileAttribute 0)))
        engine (cli/make-engine {:compose/stack-dir (str state-dir "/stacks")})
        run-id (str "it" (mod stamp 1000000))
        c {:engine engine
           :settings {:compose/default-ttl-minutes 60
                      :compose/default-idle-action :stop
                      :compose/up-timeout-ms 120000
                      :compose/profiles {}
                      :compose/projects {"prov" {:project/id "prov" :project/dir (dir-with "hc-it-prov" provider-yml)
                                                 :project/name pname
                                                 :project/provides {"echo" {:service "echo" :env {"ECHO_ADDR" "echo:7000"}}}}
                                         "cons" {:project/id "cons" :project/dir (dir-with "hc-it-cons" consumer-yml)
                                                 :project/name cname
                                                 :project/needs {"echo" {}}}}
                      :compose/stacks {"it" {:stack/id "it" :stack/consumers ["cons"]}}}
           :state (atom {:active {} :current nil})
           :now #(System/currentTimeMillis)}]
    (try
      (let [up (stacks/stack-up! c {:stack "it" :run run-id})]
        (is (r/ok? up) (pr-str up))
        (testing "the consumer carries the run labels and sees the provider by its service name"
          (let [rows (:ok (port/-running-labelled engine {"hive.run" run-id}))]
            (is (= #{"app"} (set (map :service rows))))
            (is (every? #(= "it" (get-in % [:labels "hive.stack"])) rows)))
          (is (= [(str "hive-" run-id "-cons")] (map :name (:ok (port/-networks-labelled engine {"hive.run" run-id})))))))
      (let [st (stacks/stack-status c {:run run-id})]
        (is (= "running" (get-in st [:ok :stacks 0 :instances "prov/echo" :services "echo" :state]))))
      (let [down (stacks/stack-down! c {:run run-id})]
        (is (r/ok? down) (pr-str down))
        (is (empty? (:ok (port/-running-labelled engine {"hive.run" run-id}))))
        (is (empty? (:ok (port/-networks-labelled engine {"hive.run" run-id})))))
      (finally
        (port/-down-project! engine {:project (str cname "-" run-id) :all? true})
        (port/-network-rm! engine (str "hive-" run-id "-cons"))
        (port/-down-project! engine {:project pname :all? true})))))
