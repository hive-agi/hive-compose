(ns hive-compose.adapter.host-exit-test
  "Real boundary: a host program that exits at once is reported :exited with
   its exit code and the last lines of its output, through the host runner
   and the ops pipeline."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.adapter.host :as host]
            [hive-compose.pipeline.ops :as ops]
            [hive-compose.port :as port]
            [hive-compose.stub :as stub]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- temp-dir []
  (str (java.nio.file.Files/createTempDirectory "hive-compose-exit"
                                                (make-array java.nio.file.attribute.FileAttribute 0))))

(deftest a-program-that-exits-at-once-reports-code-and-output
  (let [dir (temp-dir)
        prog {:program/id "boom" :program/dir dir :program/with ["web"]
              :program/command ["sh" "-c" "echo boom; exit 3"]}
        c {:engine (stub/engine {"web" ["web"]})
           :runner (host/make-runner {:compose/systemd-scope? false})
           :settings {:compose/default-ttl-minutes 60
                      :compose/default-idle-action :stop
                      :compose/log-dir (str dir "/logs")
                      :compose/programs? true
                      :compose/program-grace-ms 5000
                      :compose/profiles {"web" {:profile/id "web" :profile/dir dir
                                                :profile/services ["web"]
                                                :profile/programs [prog]}}}
           :state (atom {:active {} :current nil})
           :now (constantly 0)}
        res (ops/up! c "web")
        [row] (:programs (:ok res))]
    (is (r/ok? res) (pr-str res))
    (is (= :exited (:status row)) (pr-str row))
    (is (true? (:exited? row)))
    (is (= 3 (:exit-code row)))
    (is (some #{"boom"} (:log-tail row)))
    (is (nil? (get-in @(:state c) [:active "web" :programs "boom"])) "not held")
    (testing "the runner answers the same for that pid"
      (is (= 3 (port/-exit-code (:runner c) (:pid row))))
      (is (false? (port/-alive? (:runner c) (:pid row)))))))

(deftest a-program-that-lives-past-the-grace-is-running
  (let [dir (temp-dir)
        rn (host/make-runner {:compose/systemd-scope? false})
        started (port/-start! rn {:program/id "sleeper" :program/dir dir
                                  :program/command ["sleep" "30"]}
                              (str dir "/sleeper.log"))
        pid (:pid (:ok started))]
    (try
      (is (r/ok? started))
      (is (port/-alive? rn pid))
      (is (nil? (port/-exit-code rn pid)) "no exit code while it lives")
      (finally (port/-halt! rn pid)))))
