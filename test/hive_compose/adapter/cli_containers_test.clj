(ns hive-compose.adapter.cli-containers-test
  "Releasing containers by name through CliEngine: found by compose labels,
   stopped (and removed for :down) with plain docker, no compose file loaded."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.adapter.cli :as cli]
            [hive-compose.port :as port]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def p {:profile/id "dc/adopted" :profile/dir "/w/dc" :profile/project "dc"
        :profile/files ["docker-compose.yml" "docker-compose.escala-dev.yml"]})

(deftest containers-are-listed-by-label-without-compose
  (let [argv (cli/containers-argv p)]
    (is (= ["docker" "ps" "-a" "--filter" "label=com.docker.compose.project=dc"] (subvec argv 0 5)))
    (is (not-any? #{"compose" "-f"} argv) "no compose file is loaded")))

(deftest container-names-keeps-only-the-named-services
  (is (= ["dc-fluxo-api-1" "dc-postgres-1"]
         (cli/container-names "dc-postgres-1\tpostgres\ndc-web-1\tweb\n\ndc-fluxo-api-1\tfluxo-api\n"
                              ["fluxo-api" "postgres" "absent"]))))

(defn- engine [listing calls]
  (cli/make-engine {:exec (fn [argv opts]
                            (swap! calls conj [argv opts])
                            (r/ok {:exit 0 :stdout (if (= "ps" (second argv)) listing "") :stderr ""}))}))

(deftest stop-by-name-runs-docker-stop-on-the-matching-containers
  (let [calls (atom [])
        res (port/-stop-containers! (engine "dc-fluxo-api-1\tfluxo-api\ndc-web-1\tweb\n" calls)
                                    p ["fluxo-api"] :stop)]
    (is (= ["dc-fluxo-api-1"] (:containers (:ok res))))
    (is (= [["docker" "stop" "dc-fluxo-api-1"]] (mapv first (rest @calls))))
    (testing "no compose env is passed: nothing interpolates"
      (is (every? #(nil? (:env (second %))) @calls)))))

(deftest down-by-name-also-removes-the-containers
  (let [calls (atom [])]
    (port/-stop-containers! (engine "dc-fluxo-api-1\tfluxo-api\n" calls) p ["fluxo-api"] :down)
    (is (= [["docker" "stop" "dc-fluxo-api-1"] ["docker" "rm" "dc-fluxo-api-1"]]
           (mapv first (rest @calls))))))

(deftest nothing-to-stop-is-a-skip
  (let [calls (atom [])]
    (is (= :no-containers (:skipped (:ok (port/-stop-containers! (engine "" calls) p ["web"] :stop)))))
    (is (= 1 (count @calls)) "only the listing ran")
    (is (= :no-services (:skipped (:ok (port/-stop-containers! (engine "" calls) p [] :stop)))))))
