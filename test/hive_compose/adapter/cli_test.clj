(ns hive-compose.adapter.cli-test
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.adapter.cli :as cli]
            [hive-compose.port :as port]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def profile {:profile/id "full"
              :profile/dir "/w/shop"
              :profile/files ["compose.yml" "compose.dev.yml"]
              :profile/compose-profiles ["debug"]
              :profile/services ["api"]
              :profile/env {"TAG" "dev"}})

(defn- recording-exec
  "An exec with hive-system's contract that records (argv, opts) and answers
   from `reply` (argv -> {:exit :stdout :stderr} or a Result)."
  [calls reply]
  (fn [argv opts]
    (swap! calls conj [argv opts])
    (let [x (reply argv)]
      (if (or (r/ok? x) (r/err? x)) x (r/ok x)))))

(deftest argv-addresses-the-profile
  (is (= ["docker" "compose" "--project-directory" "/w/shop" "-p" "shop"
          "-f" "compose.yml" "-f" "compose.dev.yml" "--profile" "debug"]
         (cli/base-argv profile)))
  (is (= ["config" "--format" "json" "api"] (take-last 4 (cli/closure-argv profile))))
  (is (= ["up" "-d" "api" "db"] (take-last 4 (cli/up-argv profile ["api" "db"]))))
  (is (= ["up" "-d" "--wait" "api"] (take-last 4 (cli/up-argv (assoc profile :profile/wait? true) ["api"]))))
  (is (= ["stop" "api"] (take-last 2 (cli/stop-argv profile ["api"]))))
  (is (= ["down" "api"] (take-last 2 (cli/down-argv profile ["api"]))))
  (testing "down never removes volumes"
    (is (not-any? #{"-v" "--volumes"} (cli/down-argv profile ["api"])))))

(deftest parses-both-ps-formats
  (let [ndjson "{\"Service\":\"db\",\"State\":\"running\"}\n{\"Service\":\"api\",\"State\":\"exited\"}\n"
        array "[{\"Service\":\"db\",\"State\":\"running\"},{\"Service\":\"web\",\"State\":\"running\"}]"]
    (is (= #{"db"} (cli/running-services (cli/parse-json-rows ndjson))))
    (is (= #{"db" "web"} (cli/running-services (cli/parse-json-rows array))))
    (is (= [] (cli/parse-json-rows "  ")))))

(deftest closure-reads-config-services
  (is (= ["api" "db" "migrate"]
         (cli/closure-services "{\"name\":\"shop\",\"services\":{\"migrate\":{},\"api\":{},\"db\":{}}}"))))

(deftest engine-runs-in-the-profile-dir-with-its-env
  (let [calls (atom [])
        e (cli/make-engine {:exec (recording-exec calls (constantly {:exit 0 :stdout "{\"services\":{\"api\":{}}}"}))})]
    (is (= {:ok ["api"]} (port/-closure e profile)))
    (let [[argv opts] (first @calls)]
      (is (= "docker" (first argv)))
      (is (= "/w/shop" (:dir opts)))
      (is (= {"TAG" "dev"} (:env opts))))))

(deftest empty-service-set-never-reaches-docker
  (let [calls (atom [])
        e (cli/make-engine {:exec (recording-exec calls (constantly {:exit 0 :stdout ""}))})]
    (doseq [f [port/-up! port/-stop! port/-down!]]
      (is (= :no-services (:skipped (:ok (f e profile []))))))
    (is (empty? @calls) "a bare `compose stop` would stop the whole project")))

(deftest failures-are-errs
  (let [e (cli/make-engine {:exec (recording-exec (atom []) (constantly {:exit 1 :stdout "" :stderr "boom"}))})
        res (port/-stop! e profile ["api"])]
    (is (= :compose/command-failed (:error res)))
    (is (= "boom" (:stderr res))))
  (let [e (cli/make-engine {:exec (recording-exec (atom []) (constantly {:exit 0 :stdout "not json"}))})]
    (is (= :compose/unparseable (:error (port/-closure e profile)))))
  (let [e (cli/make-engine {:exec (recording-exec (atom []) (constantly (r/err :shell/timeout {:timeout-ms 1})))})]
    (is (= :shell/timeout (:error (port/-running e profile))))))
