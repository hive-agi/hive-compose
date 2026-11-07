(ns hive-compose.adapter.host-scope-test
  "The host runner starts a program in a systemd user scope of its own when
   asked, so it survives an OOM kill of the hive JVM's cgroup."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [hive-compose.adapter.host :as host]
            [hive-compose.port :as port]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- start-with [scope?]
  (let [spawned (atom [])
        rn (host/make-runner {:spawn (fn [cmd opts] (swap! spawned conj [cmd opts])
                                       (r/ok {:pid 4242}))
                              :alive? (constantly false)
                              :compose/systemd-scope? scope?})
        log (str (System/getProperty "java.io.tmpdir") "/hc-scope-test/x.log")
        res (port/-start! rn {:program/id "web" :program/dir "/tmp"
                              :program/command ["npx" "shadow-cljs" "watch" "app"]}
                          log)]
    {:res res :script (str (ffirst @spawned))}))

(deftest scope-on-starts-through-systemd-run
  (let [{:keys [res script]} (start-with true)]
    (is (= {:pid 4242} (:ok res)) "the kept pid is still the spawned one")
    (is (str/includes? script "systemd-run --user --scope"))
    (is (re-find #"--unit='hc-web-\d+'" script))
    (is (str/includes? script "setsid 'npx'"))))

(deftest scope-off-is-the-plain-setsid-start
  (let [{:keys [script]} (start-with false)]
    (is (not (str/includes? script "systemd-run")))
    (is (str/includes? script "exec setsid 'npx'"))))

(deftest scope-unit-names-the-program
  (testing "hc-<id>-<ms>"
    (is (= "hc-web-7" (host/scope-unit {:program/id "web"} 7)))))
