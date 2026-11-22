(ns hive-compose.adapter.suite-runner-test
  "The capped suite runner against a recorded exec, plus one real-boundary
   run: a host command over its cap is OOM-killed inside its own systemd user
   scope and diagnosed (skipped where no user manager answers)."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [hive-compose.adapter.host :as host]
            [hive-compose.adapter.suite-runner :as sr]
            [hive-compose.port :as port]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- recording-exec
  "An exec that records every argv and answers with `(answer argv)`."
  [answer]
  (let [calls (atom [])]
    {:calls calls
     :exec (fn [argv opts] (swap! calls conj [argv opts]) (answer argv))}))

(defn- runner [exec systemd?]
  (sr/make-runner {:exec exec :now (constantly 1000) :compose/systemd-scope? systemd?}))

(def ^:private cmd-spec
  {:runner :command :command ["go" "test" "./..."] :dir "/src" :env {"HIVE_RUN_ID" "r1"}
   :test-id "e2e/orders" :resources {:mem "256m"}})

(deftest host-command-runs-in-a-capped-scope
  (let [{:keys [exec calls]} (recording-exec
                              (fn [argv]
                                (cond
                                  (= "systemd-run" (first argv)) (r/ok {:exit 137 :stdout "" :stderr "Killed"})
                                  (some #{"show"} argv) (r/ok {:exit 0 :stdout "oom-kill\n" :stderr ""})
                                  :else (r/ok {:exit 0 :stdout "" :stderr ""}))))
        res (port/-run! (runner exec true) cmd-spec)
        [argv opts] (first @calls)]
    (is (r/ok? res))
    (is (= ["systemd-run" "--user" "--scope" "--quiet"] (take 4 argv)))
    (is (some #{"MemoryMax=268435456"} argv))
    (is (some #{"MemorySwapMax=0"} argv))
    (is (= ["go" "test" "./..."] (take-last 3 argv)))
    (is (= "sh" (nth argv (- (count argv) 8))) "output goes to a log file, not this heap")
    (is (= "/src" (:dir opts)))
    (is (= {"HIVE_RUN_ID" "r1"} (:env opts)) "env goes through the process env, not the argv")
    (testing "the scope is read, then reset"
      (is (= "show" (nth (first (nth @calls 1)) 2)))
      (is (= "reset-failed" (nth (first (nth @calls 2)) 2))))
    (is (= "e2e/orders OOM-killed at 256m" (get-in res [:ok :oom :oom/line])))
    (is (= :systemd (get-in res [:ok :cap :mode])))
    (is (= 137 (get-in res [:ok :exit])))))

(deftest a-plain-failure-is-no-oom
  (let [{:keys [exec]} (recording-exec
                        (fn [argv]
                          (if (some #{"show"} argv)
                            (r/ok {:exit 0 :stdout "exit-code\n" :stderr ""})
                            (r/ok {:exit 1 :stdout "FAIL TestOrders" :stderr ""}))))
        res (:ok (port/-run! (runner exec true) cmd-spec))]
    (is (= 1 (:exit res)))
    (is (nil? (:oom res)))
    (is (= ["FAIL TestOrders"] (:output res)))))

(deftest without-systemd-ulimit-is-used-and-reported
  (let [{:keys [exec calls]} (recording-exec (fn [_] (r/ok {:exit 0 :stdout "ok" :stderr ""})))
        res (:ok (port/-run! (runner exec false) cmd-spec))
        [argv] (first @calls)]
    (is (= ["sh" "-c" "ulimit -v 262144 && exec \"$@\"" "sh"] (take 4 argv)))
    (is (= ["go" "test" "./..."] (take-last 3 argv)))
    (is (= 1 (count @calls)) "no scope to read")
    (is (= :ulimit (get-in res [:cap :mode])))
    (is (str/includes? (get-in res [:cap :note]) "systemd-run absent"))))

(deftest no-cap-runs-the-command-as-is
  (let [{:keys [exec calls]} (recording-exec (fn [_] (r/ok {:exit 0 :stdout "" :stderr ""})))
        res (:ok (port/-run! (runner exec true) (dissoc cmd-spec :resources)))]
    (is (= "sh" (ffirst (first @calls))))
    (is (= ["go" "test" "./..."] (take-last 3 (ffirst @calls))))
    (is (not-any? #{"systemd-run"} (ffirst @calls)))
    (is (= :none (get-in res [:cap :mode])))))

(deftest a-timeout-is-an-answer
  (let [{:keys [exec]} (recording-exec (fn [argv]
                                         (if (= "systemd-run" (first argv))
                                           (r/err :shell/timeout {:timeout-ms 5})
                                           (r/ok {:exit 0 :stdout "success" :stderr ""}))))
        res (port/-run! (runner exec true) cmd-spec)]
    (is (r/ok? res))
    (is (true? (get-in res [:ok :timed-out?])))
    (is (nil? (get-in res [:ok :exit])))))

(deftest a-runner-that-cannot-start-is-an-error
  (let [{:keys [exec]} (recording-exec (fn [_] (r/err :shell/exec-failed {:message "no such file"})))]
    (is (= :compose/runner-failed (:error (port/-run! (runner exec true) cmd-spec))))))

(def ^:private container-spec
  {:runner :container :image "node:22" :command ["npm" "test"] :name "hive-r1-e2e-orders"
   :env {"HIVE_RUN_ID" "r1" "SECRET" "s3cr3t"} :mounts [{:host "/state/r" :container "/reports"}]
   :network "hive-r1-orders" :labels {"hive.run" "r1"} :test-id "e2e/orders"
   :resources {:mem "512m" :cpus 2}})

(deftest container-runs-capped-and-is-inspected
  (let [inspect [{"State" {"OOMKilled" true "ExitCode" 137} "HostConfig" {"Memory" 536870912}}]
        {:keys [exec calls]} (recording-exec
                              (fn [argv]
                                (case (second argv)
                                  "run" (r/ok {:exit 137 :stdout "" :stderr ""})
                                  "inspect" (r/ok {:exit 0 :stdout (json/write-str inspect) :stderr ""})
                                  (r/ok {:exit 0 :stdout "" :stderr ""}))))
        res (:ok (port/-run! (runner exec true) container-spec))
        [run-argv run-opts] (first @calls)]
    (is (= ["sh" "-c" "exec \"$@\" >/dev/null 2>&1 </dev/null" "sh"] (take 4 run-argv))
        "the container's output is never drained into this heap")
    (is (= ["docker" "run" "--name" "hive-r1-e2e-orders"] (take 4 (drop 4 run-argv))))
    (is (some #(= ["docker" "logs" "--tail" "50" "hive-r1-e2e-orders"] (first %)) @calls))
    (is (= ["--memory" "536870912" "--memory-swap" "536870912" "--cpus" "2" "node:22" "npm" "test"]
           (take-last 9 run-argv)))
    (is (some #{"SECRET"} run-argv))
    (is (not-any? #(str/includes? % "s3cr3t") run-argv) "env values never reach the argv")
    (is (= "s3cr3t" (get-in run-opts [:env "SECRET"])))
    (is (= ["docker" "rm" "-f" "hive-r1-e2e-orders"] (first (last @calls))))
    (is (= "e2e/orders OOM-killed at 512m" (get-in res [:oom :oom/line])))
    (is (= :docker (get-in res [:cap :mode])))))

(deftest a-killed-container-without-oom-is-no-oom
  (let [inspect [{"State" {"OOMKilled" false "ExitCode" 137} "HostConfig" {"Memory" 536870912}}]
        {:keys [exec]} (recording-exec
                        (fn [argv]
                          (if (= "inspect" (second argv))
                            (r/ok {:exit 0 :stdout (json/write-str inspect) :stderr ""})
                            (r/ok {:exit 137 :stdout "" :stderr ""}))))
        res (:ok (port/-run! (runner exec true) container-spec))]
    (is (= 137 (:exit res)))
    (is (nil? (:oom res)))))

(deftest real-scope-oom-is-diagnosed
  (if-not (host/systemd-user-scope?)
    (is true "no systemd user manager here: the real-scope check is skipped")
    (let [rn (sr/make-runner {})
          res (port/-run! rn {:runner :command :test-id "hog/self"
                              ;; tail buffers /dev/zero without bound: well past 32m at once
                              :command ["sh" "-c" "tail /dev/zero"]
                              :timeout-ms 60000
                              :resources {:mem "32m"}})]
      (is (r/ok? res))
      (is (= "hog/self OOM-killed at 32m" (get-in res [:ok :oom :oom/line])))
      (is (= :scope (get-in res [:ok :oom :oom/source]))))))
