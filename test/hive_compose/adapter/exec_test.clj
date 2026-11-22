(ns hive-compose.adapter.exec-test
  "IDataExec on the CLI engine and ICommandRunner on the host runner, over an
   injected exec: secrets reach the process env only, by name in the argv,
   and are scrubbed from output."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.adapter.cli :as cli]
            [hive-compose.adapter.exec :as exec]
            [hive-compose.adapter.host :as host]
            [hive-compose.port :as port]
            [hive-dsl.result :as r]
            [hive-compose.adapter.secrets :as secrets]))

;; SPDX-License-Identifier: MIT

(defn- recording-exec [answers]
  (let [calls (atom [])]
    [calls (fn [argv opts]
             (swap! calls conj [argv opts])
             (r/ok (or (some (fn [[pred a]] (when (pred argv) a)) answers)
                       {:exit 0 :stdout "" :stderr ""})))]))

(def resolver
  "The real pass resolver over an exec that answers `pass show <path>`."
  (secrets/make-resolver {:exec (fn [argv _] (r/ok {:exit 0 :stdout (str "s3cret-" (last argv) "\n") :stderr ""}))}))

(deftest docker-exec-passes-env-by-name
  (let [[calls ex] (recording-exec [[#(= "ps" (second %)) {:exit 0 :stdout "pg-db-1\n" :stderr ""}]
                                    [#(= "exec" (second %)) {:exit 0 :stdout "pw is s3cret-db/pw" :stderr ""}]])
        e (cli/make-engine {:exec ex :compose/resolver resolver})
        res (port/-exec! e {:project "pg" :service "db"} ["psql" "-c" "SELECT 1"]
                         {:env {"PGPASSWORD" {:pass "db/pw"} "A" "1"}})
        [argv opts] (second @calls)]
    (is (r/ok? res) (pr-str res))
    (is (= ["docker" "exec" "-e" "A" "-e" "PGPASSWORD" "pg-db-1" "psql" "-c" "SELECT 1"] argv))
    (is (= {"A" "1" "PGPASSWORD" "s3cret-db/pw"} (:env opts)))
    (is (= "pw is pass:db/pw" (get-in res [:ok :out])) "output scrubbed")
    (is (not-any? #(re-find #"s3cret" %) argv))))

(deftest docker-exec-errors
  (testing "no running container"
    (let [[_ ex] (recording-exec [])
          e (cli/make-engine {:exec ex})]
      (is (= :compose/no-container (:error (port/-exec! e {:project "p" :service "s"} ["x"] {}))))))
  (testing "non-zero exit carries stderr"
    (let [[_ ex] (recording-exec [[#(= "ps" (second %)) {:exit 0 :stdout "c1" :stderr ""}]
                                  [#(= "exec" (second %)) {:exit 1 :stdout "" :stderr "already exists"}]])
          res (port/-exec! (cli/make-engine {:exec ex}) {:project "p" :service "s"} ["x"] {})]
      (is (= [:compose/command-failed 1 "already exists"] [(:error res) (:exit res) (:stderr res)])))))

(deftest host-runner-runs-to-completion-in-dir
  (let [[calls ex] (recording-exec [])
        rn (host/make-runner {:exec ex :compose/resolver resolver :compose/systemd-scope? false})
        res (port/-run-command! rn ["./seed.sh"] {:dir "/w/kafka" :env {"T" "p."} :timeout-ms 5000})]
    (is (r/ok? res))
    (is (= [["./seed.sh"] {:timeout-ms 5000 :dir "/w/kafka" :env {"T" "p."}}] (first @calls)))
    (is (= ["docker" "exec" "c" "ls"] (exec/exec-argv "c" [] ["ls"])))))
