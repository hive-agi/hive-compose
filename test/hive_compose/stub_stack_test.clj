(ns hive-compose.stub-stack-test
  "The stub's IStackEngine behaves like docker does where the pipeline relies on it."
  (:require [clojure.test :refer [deftest is]]
            [hive-compose.port :as port]
            [hive-compose.stub :as stub]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(deftest networks-labels-and-connect
  (let [e (stub/engine {})
        l {"hive.run" "r1x" "hive.stack" "s"}]
    (is (:created? (:ok (port/-network-create! e "hive-r1x-a" l))))
    (is (false? (:created? (:ok (port/-network-create! e "hive-r1x-a" l)))))
    (is (= :compose/command-failed
           (:error (port/-up-project! e {:project "a" :services ["app"] :networks ["nope"]})))
        "an external network that does not exist refuses, as compose does")
    (is (r/ok? (port/-up-project! e {:project "a" :services ["app"] :labels l :networks ["hive-r1x-a"]})))
    (is (r/ok? (port/-up-project! e {:project "kafka" :services ["kafka"]})))
    (is (r/ok? (port/-network-connect! e "hive-r1x-a" {:project "kafka" :service "kafka"} ["kafka"])))
    (is (= {"a-app-1" ["app"] "kafka-kafka-1" ["kafka"]} (get-in (stub/networks e) ["hive-r1x-a" :members])))
    (is (= ["a-app-1"] (map :name (:ok (port/-running-labelled e {"hive.run" "r1x"})))))
    (is (= ["hive-r1x-a"] (map :name (:ok (port/-networks-labelled e {"hive.run" "r1x"})))))
    (is (= :compose/no-container
           (:error (port/-network-connect! e "hive-r1x-a" {:project "x" :service "y"} ["y"]))))
    (is (= ["a-app-1" "kafka-kafka-1"] (:disconnected (:ok (port/-network-rm! e "hive-r1x-a")))))
    (is (empty? (stub/networks e)))
    (is (= #{} (get-in (stub/containers e) ["kafka-kafka-1" :networks])))))

(deftest wait-fails-on-unhealthy
  (let [e (stub/engine {})]
    (stub/health! e "pg" "db" :unhealthy)
    (is (r/err? (port/-up-project! e {:project "pg" :services ["db"] :wait? true})))
    (is (r/ok? (port/-up-project! e {:project "pg" :services ["db"]})) "without --wait compose does not look")
    (is (= :unhealthy (:health (first (:ok (port/-running-labelled e {"com.docker.compose.project" "pg"}))))))))

(deftest down-project
  (let [e (stub/engine {})]
    (port/-up-project! e {:project "p" :services ["a" "b"]})
    (port/-down-project! e {:project "p" :services ["a"]})
    (is (= #{"b"} (stub/running e "p")))
    (port/-down-project! e {:project "p" :all? true})
    (is (= #{} (stub/running e "p")))
    (is (empty? (stub/containers e)))))
