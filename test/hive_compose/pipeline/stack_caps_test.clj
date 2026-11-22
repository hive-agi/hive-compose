(ns hive-compose.pipeline.stack-caps-test
  "hc-16a on the stub engine: plan shows caps and the total, stack-up admits
   or refuses before starting anything, private instances get mem_limit /
   cpus / JAVA_TOOL_OPTIONS in their override, the reaper's pressure guard.
   MemAvailable comes from a stub IHostMemory, never from the host."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.adapter.cli :as cli]
            [hive-compose.pipeline.stacks :as stacks]
            [hive-compose.port :as port]
            [hive-compose.promote.stack-run]
            [hive-compose.stub :as stub]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def gi (* 1024 1024 1024))
(def mi (* 1024 1024))
(def minute 60000)

(defrecord StubMemory [available]
  port/IHostMemory
  (-mem-available [_] (if (some? @available) (r/ok @available) (r/err :compose/meminfo-unreadable {}))))

(def projects
  {"kafka" {:project/id "kafka" :project/dir "/w/kafka"
            :project/provides {"kafka" {:service "kafka" :version "3.7" :resources {:mem "1g" :cpus 2}}}}
   "pg" {:project/id "pg" :project/dir "/w/pg"
         :project/provides {"postgres" {:service "db" :version "16"}}}
   "shop" {:project/id "shop" :project/dir "/w/shop"
           :project/needs {"kafka" {:resources {:mem "2g"}} "postgres" {}}}
   "billing" {:project/id "billing" :project/dir "/w/billing"
              :project/needs {"kafka" {}}}
   "audit" {:project/id "audit" :project/dir "/w/audit"
            :project/needs {"kafka" {:share? false :resources {:jvm? true}}}}})

(def world
  {"kafka" {"kafka" {:deps ["zookeeper"]} "zookeeper" {}}
   "pg" {"db" {}}
   "shop" {"api" {}}
   "billing" {"billing" {}}
   "audit" {"audit" {}}})

(def all-stacks
  {"shop" {:stack/id "shop" :stack/consumers ["shop"]}
   "billing" {:stack/id "billing" :stack/consumers ["billing"] :stack/priority 5}
   "audit" {:stack/id "audit" :stack/consumers ["audit"] :stack/priority -1}
   "all" {:stack/id "all" :stack/consumers ["audit" "billing" "shop"]}})

(defn- ctx [extra-settings available]
  (let [clock (atom 0) hex (atom 0)]
    {:engine (stub/engine {} {} world)
     :host-memory (->StubMemory (atom available))
     :settings (merge {:compose/default-ttl-minutes 60 :compose/orphan-ttl-minutes 5
                       :compose/claim-ttl-minutes 30 :compose/default-idle-action :stop
                       :compose/up-timeout-ms 120000 :compose/profiles {}
                       :compose/projects projects :compose/stacks all-stacks
                       :compose/default-resources {:mem "256m"}}
                      extra-settings)
     :state (atom {:active {} :current nil})
     :now #(deref clock)
     :clock clock
     :rand-hex #(format "%06x" (swap! hex inc))
     :save! (fn [_] (r/ok nil))}))

(defn- ups [c] (filter #(= :up-project (first %)) (stub/calls (:engine c))))

(deftest plan-prints-caps-and-the-total
  (let [c (ctx {} nil)
        p (stacks/plan c {:stack "all"})]
    (is (r/ok? p) (pr-str p))
    (is (= :ok (get-in p [:ok :admission])))
    (let [b (get-in p [:ok :budget])]
      (is (= (* 2 gi) (get-in b [:budget/instances "kafka/kafka" :mem])) "shared: the largest consumer cap")
      (is (= gi (get-in b [:budget/instances "kafka/kafka@audit" :mem])))
      (is (= {"zookeeper" (* 256 mi)} (get-in b [:budget/instances "kafka/kafka@audit" :closure]))
          "private kafka's zookeeper is counted at the default")
      (is (= (+ (* 3 gi) (* 512 mi)) (:budget/total-mem b)))
      (is (= gi (:budget/saved-mem b)) "billing's 1g kafka is shared into shop's 2g one"))
    (let [text (get-in p [:ok :text])]
      (is (re-find #"kafka/kafka  mem 2g \(need\), cpus 2" text))
      (is (re-find #"closure zookeeper=256m" text))
      (is (re-find #"total capped memory 3.5g \(sharing saves 1g" text)))))

(deftest over-budget-is-refused-before-anything-starts
  (testing ":compose/budget"
    (let [c (ctx {:compose/budget "3g"} nil)
          res (stacks/stack-up! c {:stack "all"})]
      (is (= :compose/over-budget (:error res)))
      (is (= :budget (:basis res)))
      (is (= (* 512 mi) (:over res)) "kafka 2g + kafka@audit 1g + its zookeeper 256m + pg 256m")
      (is (empty? (ups c)) "nothing was started")
      (is (empty? (:stacks @(:state c))) "nothing was recorded")
      (is (= :compose/over-budget (get-in (stacks/plan c {:stack "all"}) [:ok :admission :error])))
      (is (re-find #"REFUSED: .*exceeds :compose/budget 3g" (get-in (stacks/plan c {:stack "all"}) [:ok :text])))))
  (testing ":compose/budget is host-wide: live runs count"
    (let [c (ctx {:compose/budget "3g"} nil)]
      (is (r/ok? (stacks/stack-up! c {:stack "shop"})) "shop 2.25g fits 3g alone")
      (let [res (stacks/stack-up! c {:stack "audit"})]
        (is (= :compose/over-budget (:error res)) "audit 1.25g on top of shop's live 2.25g")
        (is (= :budget (:basis res)))
        (is (= (+ (* 2 gi) (* 256 mi)) (:others-mem res)))
        (is (= (+ (* 3 gi) (* 512 mi)) (:total-mem res)))
        (is (= 1 (count (:stacks @(:state c)))) "audit was not recorded"))
      (testing "billing binds shop's kafka: only what it adds counts"
        (is (r/ok? (stacks/stack-up! c {:stack "billing"}))))))
  (testing ":compose/budget: two runs of one stack sum their PRIVATE instances"
    (let [c (ctx {:compose/budget "2g"} nil)]
      (is (r/ok? (stacks/stack-up! c {:stack "audit"})) "audit 1.25g (private kafka + zookeeper) fits 2g")
      (let [res (stacks/stack-up! c {:stack "audit"})]
        (is (= :compose/over-budget (:error res))
            "a second audit run starts a second private kafka: 2.5g > 2g")
        (is (= (+ gi (* 256 mi)) (:others-mem res)) "the first run's private kafka and zookeeper")
        (is (= 1 (count (:stacks @(:state c)))) "the second run was not recorded"))))
  (testing ":compose/budget: a second run of a stack of SHARED instances adds nothing"
    (let [c (ctx {:compose/budget "3g"} nil)]
      (is (r/ok? (stacks/stack-up! c {:stack "shop"})))
      (is (r/ok? (stacks/stack-up! c {:stack "shop"})) "both runs bind the same kafka and postgres")))
  (testing ":stack/budget"
    (let [c (ctx {:compose/stacks (assoc-in all-stacks ["shop" :stack/budget] "1g")} nil)]
      (is (= :stack (:basis (stacks/stack-up! c {:stack "shop"}))))))
  (testing "host: MemAvailable minus reserve"
    (let [c (ctx {:compose/host-reserve "4g"} (* 6 gi))
          res (stacks/stack-up! c {:stack "shop"})]
      (is (= :host (:basis res)) (pr-str res))
      (is (= (* 6 gi) (:available res)))
      (is (= (* 2 gi) (:limit res)))))
  (testing "an unreadable MemAvailable checks the budgets alone"
    (let [c (ctx {:compose/host-reserve "4g"} nil)]
      (is (r/ok? (stacks/stack-up! c {:stack "shop"}))))))

(defrecord HookedMemory [available on-read]
  port/IHostMemory
  (-mem-available [_] (on-read) (r/ok available)))

(defn- blocked? [^Thread t]
  ;; waiting on the stack lock's monitor: BLOCKED, three polls in a row
  (every? (fn [_] (Thread/sleep 5) (= Thread$State/BLOCKED (.getState t))) (range 3)))

(deftest two-concurrent-ups-over-the-budget-admit-one
  ;; audit is 1.25g (private kafka + zookeeper), :compose/budget 2g: two runs
  ;; together exceed it. Each up reads MemAvailable inside its admission; the
  ;; first clock read after it (before the run is recorded) waits until the
  ;; other up has admitted too, or is blocked on the stack lock. Unserialized,
  ;; both admit against a state holding neither; serialized, the second
  ;; admits against the first's recorded budget and is refused.
  (let [admitted (atom #{}) threads (atom []) waited (atom #{})
        c (assoc (ctx {:compose/budget "2g"} nil)
                 :host-memory (->HookedMemory (* 64 gi) #(swap! admitted conj (Thread/currentThread))))
        clock (:now c)
        c (assoc c :now (fn []
                          (let [me (Thread/currentThread)]
                            (when (and (contains? @admitted me) (not (contains? @waited me)))
                              (swap! waited conj me)
                              (let [end (+ (System/currentTimeMillis) 10000)]
                                (loop []
                                  (when-not (or (= 2 (count @admitted))
                                                (some #(and (not= % me) (blocked? %)) @threads))
                                    (when (> (System/currentTimeMillis) end)
                                      (throw (ex-info "timed out waiting for the other up" {})))
                                    (Thread/sleep 2)
                                    (recur))))))
                          (clock)))
        up! #(future (swap! threads conj (Thread/currentThread)) (stacks/stack-up! c {:stack "audit"}))
        f1 (up!) f2 (up!)
        rs [(deref f1 10000 ::timeout) (deref f2 10000 ::timeout)]]
    (is (= 1 (count (filter r/ok? rs))) (pr-str rs))
    (is (= [:compose/over-budget] (keep :error rs)))
    (is (= 1 (count (:stacks @(:state c)))) "only the admitted run is recorded")))

(deftest a-running-shared-instance-takes-no-new-host-memory
  (let [c (ctx {:compose/host-reserve "1g"} (* 4 gi))]
    (is (r/ok? (stacks/stack-up! c {:stack "shop"})) "kafka 2g + pg 256m fit 3g")
    (reset! (:available (:host-memory c)) (+ gi (* 512 mi)))
    (testing "billing binds the running kafka: no new memory"
      (is (r/ok? (stacks/stack-up! c {:stack "billing"}))))))

(deftest private-instances-get-caps-and-the-jvm-flag
  (let [c (ctx {} nil)
        _ (is (r/ok? (stacks/stack-up! c {:stack "audit"})))
        by-project (into {} (map (fn [[_ p _ opts]] [p opts])) (ups c))]
    (is (= {"kafka" {"mem_limit" gi "cpus" 2
                     "environment" {"JAVA_TOOL_OPTIONS" "-XX:MaxRAMPercentage=75"}}
            "zookeeper" {"mem_limit" (* 256 mi)}}
           (get-in by-project ["kafka-audit-audit000001" :service-overrides]))
        "private kafka: provide caps, the need's :jvm? true; its zookeeper capped at the default")
    (is (nil? (get-in by-project ["audit-audit000001" :service-overrides])) "consumers are not capped"))
  (let [c (ctx {} nil)]
    (is (r/ok? (stacks/stack-up! c {:stack "shop"})))
    (is (nil? (some :service-overrides (map #(nth % 3) (ups c))))
        "a shared instance runs from its own files, uncapped in place"))
  (testing "the override document carries mem_limit, cpus and JAVA_TOOL_OPTIONS"
    (let [doc (cli/override-doc {:services ["kafka" "zookeeper"]
                                 :labels {"hive.run" "r1"}
                                 :service-overrides {"kafka" {"mem_limit" gi "cpus" 2
                                                              "environment" {"JAVA_TOOL_OPTIONS" "-XX:MaxRAMPercentage=75"}}}})]
      (is (= gi (get-in doc ["services" "kafka" "mem_limit"])))
      (is (= 2 (get-in doc ["services" "kafka" "cpus"])))
      (is (= "-XX:MaxRAMPercentage=75" (get-in doc ["services" "kafka" "environment" "JAVA_TOOL_OPTIONS"])))
      (is (= {"hive.run" "r1"} (get-in doc ["services" "kafka" "labels"])))
      (is (nil? (get-in doc ["services" "zookeeper" "mem_limit"])))))
  (testing "a service's own JVM flags win per flag"
    (is (= {"kafka" {"mem_limit" gi "cpus" 2
                     "environment" {"JAVA_TOOL_OPTIONS" "-XX:MaxRAMPercentage=50"}}}
           (hive-compose.promote.stack-run/instance-overrides
            {} {:instance/id "kafka/kafka@audit" :instance/provider "kafka" :instance/services ["kafka"]}
            {:mem gi :cpus 2 :jvm? true :unbounded? false :source :provide}
            {"kafka" {:image "confluentinc/cp-kafka:7.6.0" :java-tool-options "-XX:MaxRAMPercentage=50"}}))))
  (testing "a detected java image gets the flag only with a mem_limit"
    (is (= {} (hive-compose.promote.stack-run/instance-overrides
               {} {:instance/provider "kafka" :instance/services ["kafka"]}
               {:unbounded? false :source :none}
               {"kafka" {:image "eclipse-temurin:21-jre"}}))
        "uncapped: the JVM keeps its 25% default, never 75% of the host")
    (is (= "-XX:MaxRAMPercentage=75"
           (get-in (hive-compose.promote.stack-run/instance-overrides
                    {} {:instance/provider "kafka" :instance/services ["kafka"]}
                    {:mem gi :unbounded? false :source :provide}
                    {"kafka" {:image "eclipse-temurin:21-jre"}})
                   ["kafka" "environment" "JAVA_TOOL_OPTIONS"]))))
  (testing "a closure service with its own mem_limit keeps it"
    (is (= {} (hive-compose.promote.stack-run/instance-overrides
               {} {:instance/provider "kafka" :instance/services []}
               {:closure {"zookeeper" (* 256 mi)} :unbounded? false :source :none}
               {"zookeeper" {:image "zookeeper:3.9" :mem-limit "512m"}})))))

(deftest pressure-guard-skips-a-run-that-frees-nothing
  (let [c (ctx {:compose/host-reserve "4g"} (* 64 gi))
        shop (get-in (stacks/stack-up! c {:stack "shop"}) [:ok :run])
        billing (get-in (stacks/stack-up! c {:stack "billing"}) [:ok :run])]
    (stacks/claim! c shop "ling-1" {})
    (swap! (:clock c) + (* 6 minute))
    (stacks/claim! c shop "ling-1" {})
    (reset! (:available (:host-memory c)) (* 3 gi))
    (is (= [] (:reaped (stacks/reap! c)))
        "billing only binds shop's kafka, which stays up: stopping it frees nothing")
    (is (contains? (:stacks @(:state c)) billing))))

(deftest service-facts-parse
  (is (= {"a" {:image "eclipse-temurin:21" :java-tool-options "-Xmx1g" :mem-limit 1073741824}
          "b" {:image "postgres:16" :mem-limit "512m"}}
         (cli/service-facts
          "{\"services\":{\"a\":{\"image\":\"eclipse-temurin:21\",\"mem_limit\":1073741824,\"environment\":{\"JAVA_TOOL_OPTIONS\":\"-Xmx1g\"}},\"b\":{\"image\":\"postgres:16\",\"deploy\":{\"resources\":{\"limits\":{\"memory\":\"512m\"}}},\"environment\":[\"X=1\"]}}}"
          ["a" "b" "c"]))))

(deftest pressure-guard-stops-the-lowest-priority-idle-unclaimed-run
  (let [c (ctx {:compose/host-reserve "4g"} (* 64 gi))
        shop (get-in (stacks/stack-up! c {:stack "shop"}) [:ok :run])
        billing (get-in (stacks/stack-up! c {:stack "billing"}) [:ok :run])
        audit (get-in (stacks/stack-up! c {:stack "audit"}) [:ok :run])]
    (is (every? some? [shop billing audit]))
    (stacks/claim! c audit "ling-1" {})
    (testing "no pressure, nothing reaped"
      (is (= [] (:reaped (stacks/reap! c)))))
    (reset! (:available (:host-memory c)) (* 3 gi))
    (testing "under pressure, a run touched just now is not idle"
      (is (= [] (:reaped (stacks/reap! c)))))
    (swap! (:clock c) + (* 6 minute))
    (stacks/claim! c audit "ling-1" {})
    (testing "audit (-1) is claimed, shop (0) goes before billing (5)"
      (let [rows (:reaped (stacks/reap! c))]
        (is (= [shop] (mapv :run rows)))
        (is (= :memory-pressure (:reason (first rows))))
        (is (= (* 4 gi) (:reserve (first rows))))))
    (testing "one per pass"
      (is (= [billing] (mapv :run (:reaped (stacks/reap! c))))))
    (testing "only the claimed run is left"
      (is (= [] (:reaped (stacks/reap! c))))
      (is (= #{audit} (set (keys (:stacks @(:state c)))))))))
