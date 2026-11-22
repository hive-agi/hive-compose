(ns hive-compose.e2e-fixtures-test
  "The e2e fixture images build. A guard for test/resources/e2e, which the
  stack e2e proof (hc-11) mounts: the Go `orders` service, the Node `billing`
  service and the sh+curl suite runner. Integration only: it needs a docker
  daemon, and skips cleanly (one passing assertion, a printed reason) without one."
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.test :refer [deftest is testing]]))

;; SPDX-License-Identifier: MIT

(def e2e-root
  "The fixture tree, resolved from the classpath (test/resources is on it)."
  (some-> (io/resource "e2e/README.md") io/file .getParentFile))

(def images
  "Fixture directory -> the tag its compose.yml names."
  [["orders" "hive-compose-e2e/orders:dev"]
   ["billing" "hive-compose-e2e/billing:dev"]
   ["suite" "hive-compose-e2e/suite:dev"]])

(defn- docker-available? []
  (try (zero? (:exit (sh/sh "docker" "info" "--format" "{{.ServerVersion}}")))
       (catch java.io.IOException _ false)))

(defn- docker-build [dir tag]
  (sh/sh "docker" "build" "-q" "-t" tag (str dir)))

(deftest ^:integration fixture-images-build
  (is (some? e2e-root) "test/resources/e2e is on the classpath")
  (if-not (docker-available?)
    (do (println "SKIP e2e-fixtures-test: no docker daemon")
        (is true))
    (doseq [[d tag] images]
      (testing d
        (let [{:keys [exit err]} (docker-build (io/file e2e-root d) tag)]
          (is (zero? exit) (str "docker build " d " failed:\n" err)))))))
