(ns hive-compose.adapter.cli-published-test
  (:require [clojure.test :refer [deftest is]]
            [hive-compose.adapter.cli :as cli]
            [hive-compose.port :as port]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def ps-out
  (str "{\"Service\":\"api\",\"State\":\"running\",\"Publishers\":[{\"URL\":\"0.0.0.0\",\"TargetPort\":8080,\"PublishedPort\":18080,\"Protocol\":\"tcp\"},{\"URL\":\"::\",\"TargetPort\":8080,\"PublishedPort\":18080,\"Protocol\":\"tcp\"}]}\n"
       "{\"Service\":\"postgres\",\"State\":\"running\",\"Publishers\":[{\"URL\":\"\",\"TargetPort\":5432,\"PublishedPort\":0,\"Protocol\":\"tcp\"}]}\n"
       "{\"Service\":\"worker\",\"State\":\"exited\",\"Publishers\":[{\"URL\":\"0.0.0.0\",\"TargetPort\":9000,\"PublishedPort\":19000,\"Protocol\":\"tcp\"}]}\n"))

(deftest published-ports-are-those-of-running-containers-that-publish-one
  (is (= {"api" #{18080}} (cli/published-ports (cli/parse-json-rows ps-out)))
      "an unpublished port (0) and a stopped container are left out"))

(deftest the-engine-answers-published-ports-through-ps
  (let [calls (atom [])
        exec (fn [argv _opts] (swap! calls conj argv) (r/ok {:exit 0 :stdout ps-out :stderr ""}))
        e (cli/make-engine {:exec exec})]
    (is (= {"api" #{18080}} (:ok (port/-published e {:profile/id "shop" :profile/dir "/w/shop"}))))
    (is (= ["ps" "--format" "json"] (take-last 3 (first @calls))))))
