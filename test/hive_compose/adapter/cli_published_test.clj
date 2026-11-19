(ns hive-compose.adapter.cli-published-test
  (:require [clojure.test :refer [deftest is testing]]
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

(def inspect-out
  (str "[{\"State\":{\"Running\":true},\"NetworkSettings\":{\"Networks\":{"
       "\"shop_default\":{\"IPAddress\":\"172.18.0.3\",\"GlobalIPv6Address\":\"\"},"
       "\"shared\":{\"IPAddress\":\"172.20.0.5\",\"GlobalIPv6Address\":\"fd00::5\"}}}},"
       "{\"State\":{\"Running\":false},\"NetworkSettings\":{\"Networks\":{"
       "\"shop_default\":{\"IPAddress\":\"172.18.0.9\"}}}}]"))

(deftest container-addresses-are-those-of-running-containers-on-every-network
  (is (= #{"172.18.0.3" "172.20.0.5" "fd00::5"} (cli/container-addresses (cli/parse-json-rows inspect-out)))
      "a stopped container and blank addresses are left out"))

(deftest the-engine-answers-container-addresses-through-ps-and-inspect
  (let [calls (atom [])
        exec (fn [argv _opts]
               (swap! calls conj argv)
               (r/ok {:exit 0 :stderr ""
                      :stdout (if (= ["ps" "-q"] (take-last 2 argv)) "abc\ndef\n" inspect-out)}))
        e (cli/make-engine {:exec exec})
        p {:profile/id "shop" :profile/dir "/w/shop"}]
    (is (satisfies? port/IContainerAddresses e))
    (is (= #{"172.18.0.3" "172.20.0.5" "fd00::5"} (:ok (port/-addresses e p))))
    (is (= ["docker" "inspect" "--type" "container" "abc" "def"] (second @calls)))
    (testing "no running container asks nothing more"
      (let [e (cli/make-engine {:exec (fn [_ _] (r/ok {:exit 0 :stdout "" :stderr ""}))})]
        (is (= #{} (:ok (port/-addresses e p))))))))
