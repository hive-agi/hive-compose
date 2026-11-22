(ns hive-compose.promote.oom-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.promote.oom :as oom]))

;; SPDX-License-Identifier: MIT

(deftest mem-notation
  (testing "docker notation to bytes"
    (is (= 536870912 (oom/mem-bytes "512m")))
    (is (= 536870912 (oom/mem-bytes "512MB")))
    (is (= 2147483648 (oom/mem-bytes "2g")))
    (is (= 65536 (oom/mem-bytes "64kb")))
    (is (= 100 (oom/mem-bytes "100")))
    (is (= 100 (oom/mem-bytes 100))))
  (testing "not a size"
    (is (nil? (oom/mem-bytes nil)))
    (is (nil? (oom/mem-bytes 0)))
    (is (nil? (oom/mem-bytes -1)))
    (is (nil? (oom/mem-bytes "lots")))
    (is (nil? (oom/mem-bytes "0m")))
    (is (nil? (oom/mem-bytes "1.5g"))))
  (testing "bytes to notation, largest exact unit"
    (is (= "512m" (oom/mem-str 536870912)))
    (is (= "2g" (oom/mem-str 2147483648)))
    (is (= "1536m" (oom/mem-str 1610612736)))
    (is (= "1000b" (oom/mem-str 1000)))
    (is (= "1k" (oom/mem-str 1024)))))

(defspec mem-str-round-trips 200
  (prop/for-all [n (gen/large-integer* {:min 1 :max (* 64 1024 1024 1024)})]
    (= n (oom/mem-bytes (oom/mem-str n)))))

(deftest capped-argv-per-mode
  (let [mem (oom/mem-bytes "256m")]
    (testing "systemd: a user scope with MemoryMax and no swap"
      (is (= ["systemd-run" "--user" "--scope" "--quiet" "--unit=u1"
              "-p" "MemoryMax=268435456" "-p" "MemorySwapMax=0" "--" "go" "test" "./..."]
             (oom/capped-argv ["go" "test" "./..."] {:mode :systemd :mem mem :unit "u1"}))))
    (testing "cpus become a CPUQuota"
      (is (= ["-p" "CPUQuota=150%"]
             (->> (oom/capped-argv ["x"] {:mode :systemd :mem mem :cpus 1.5 :unit "u"})
                  (drop 9) (take 2) vec))))
    (testing "a string command goes through sh -c"
      (is (= ["sh" "-c" "make e2e"]
             (take-last 3 (oom/capped-argv "make e2e" {:mode :systemd :mem mem :unit "u"})))))
    (testing "ulimit fallback: KiB of address space, the command exec'd"
      (is (= ["sh" "-c" "ulimit -v 262144 && exec \"$@\"" "sh" "npm" "test"]
             (oom/capped-argv ["npm" "test"] {:mode :ulimit :mem mem}))))
    (testing "no cap: the command as is"
      (is (= ["npm" "test"] (oom/capped-argv ["npm" "test"] {:mode :none}))))))

(deftest cap-mode-and-note
  (is (= :none (oom/cap-mode nil true)))
  (is (= :systemd (oom/cap-mode 1024 true)))
  (is (= :ulimit (oom/cap-mode 1024 false)))
  (is (re-find #"systemd-run absent" (oom/cap-note :ulimit (oom/mem-bytes "1g"))))
  (is (re-find #"MemoryMax=1g" (oom/cap-note :systemd (oom/mem-bytes "1g"))))
  (is (nil? (oom/cap-note :none nil))))

(deftest scope-unit-is-a-valid-unit-name
  (is (= "hc-suite-orders-e2e-api-7" (oom/scope-unit "orders-e2e/api" 7)))
  (is (re-matches #"[a-zA-Z0-9_.-]+" (oom/scope-unit "a b/c:d" 1))))

(deftest container-cap-flags
  (is (= ["--memory" "536870912" "--memory-swap" "536870912" "--cpus" "2"]
         (oom/container-cap-args {:mem 536870912 :cpus 2})))
  (is (= [] (oom/container-cap-args {}))))

(def ^:private oom-inspect
  {"State" {"OOMKilled" true "ExitCode" 137 "Status" "exited"}
   "HostConfig" {"Memory" 536870912}})

(deftest container-finding-reads-docker-inspect
  (testing "OOMKilled with exit 137 is an OOM kill at the container's cap"
    (is (= {:oom/service "kafka" :oom/source :container :oom/cap 536870912 :oom/exit 137
            :oom/line "kafka OOM-killed at 512m"}
           (oom/container-finding "kafka" oom-inspect))))
  (testing "no cap on the container: the host OOM killer"
    (is (= "kafka OOM-killed (no cap; host OOM killer)"
           (:oom/line (oom/container-finding "kafka" (assoc-in oom-inspect ["HostConfig" "Memory"] 0))))))
  (testing "exit 137 alone (docker kill, SIGKILL) is no OOM"
    (is (nil? (oom/container-finding "kafka" (assoc-in oom-inspect ["State" "OOMKilled"] false)))))
  (testing "OOMKilled with another exit code is not claimed"
    (is (nil? (oom/container-finding "kafka" (assoc-in oom-inspect ["State" "ExitCode"] 1)))))
  (testing "nothing to inspect"
    (is (nil? (oom/container-finding "kafka" nil)))))

(deftest host-finding-reads-the-scope-or-the-output
  (let [mem (oom/mem-bytes "1g")]
    (testing "scope Result oom-kill"
      (is (= "orders/api OOM-killed at 1g"
             (:oom/line (oom/host-finding "orders/api" {:mode :systemd :mem mem :exit 137
                                                        :scope-result "oom-kill\n"})))))
    (testing "scope Result success or exit-code is no OOM"
      (is (nil? (oom/host-finding "s" {:mode :systemd :mem mem :exit 1 :scope-result "exit-code"})))
      (is (nil? (oom/host-finding "s" {:mode :systemd :mem mem :exit 0 :scope-result "success"}))))
    (testing "ulimit: a failed allocation in the output"
      (is (= "s hit its address-space cap at 1g (ulimit -v)"
             (:oom/line (oom/host-finding "s" {:mode :ulimit :mem mem :exit 1
                                               :output "Error occurred during initialization of VM\nCould not reserve enough space for object heap"})))))
    (testing "ulimit: a plain test failure is no OOM"
      (is (nil? (oom/host-finding "s" {:mode :ulimit :mem mem :exit 1 :output "1 test failed"})))
      (is (nil? (oom/host-finding "s" {:mode :ulimit :mem mem :exit 0 :output "out of memory? no"}))))
    (testing "an uncapped run is never called an OOM kill"
      (is (nil? (oom/host-finding "s" {:mode :none :exit 137 :scope-result "oom-kill"}))))))

(deftest run-and-history-marks
  (let [f (oom/container-finding "kafka" oom-inspect)
        run {:run/id "r1" :suite/id "e2e" :project "orders" :exit 137 :status :error}]
    (is (= f (:oom (oom/mark-run run [nil f]))))
    (is (= run (oom/mark-run run [nil])))
    (is (oom/oom-run? (oom/mark-run run [f])))
    (is (every? :case/oom? (oom/mark-rows (oom/mark-run run [f]) [{:case/id "a"} {:case/id "b"}])))
    (is (= [{:case/id "a"}] (oom/mark-rows run [{:case/id "a"}])))
    (is (= ["a OOM-killed at 512m" "kafka OOM-killed at 512m"]
           (oom/why-lines [f nil (assoc f :oom/service "a" :oom/line "a OOM-killed at 512m") f])))))
