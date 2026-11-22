(ns hive-compose.promote.resources-test
  "hc-16a: caps precedence, budget sum, admission, JVM flag merge."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.promote.capability :as cap]
            [hive-compose.promote.resources :as res]
            [hive-compose.schema :as schema]
            [hive-dsl.result :as r]
            [malli.core :as m]))

;; SPDX-License-Identifier: MIT

(def gi (* 1024 1024 1024))
(def mi (* 1024 1024))

(deftest parse-and-render-mem
  (is (= 512 (res/parse-mem 512)))
  (is (= (* 512 mi) (res/parse-mem "512m")))
  (is (= (* 2 gi) (res/parse-mem "2G")))
  (is (= (* 64 1024) (res/parse-mem "64kb")))
  (is (= 7 (res/parse-mem "7b")))
  (is (nil? (res/parse-mem "2 g")))
  (is (nil? (res/parse-mem nil)))
  (is (nil? (res/parse-mem 0)))
  (is (= "1g" (res/human-mem gi)))
  (is (= "1.5g" (res/human-mem (+ gi (* 512 mi)))))
  (is (= "512m" (res/human-mem (* 512 mi)))))

(def settings
  {:compose/default-resources {:mem "256m"}
   :compose/projects
   {"infra" {:project/id "infra" :project/dir "/infra"
             :project/provides {"kafka" {:service "kafka" :version "3.7" :resources {:mem "1g" :cpus 1}}
                                "pg" {:service "postgres" :version "16"}
                                "big" {:service "big" :resources :unbounded}}}
    "a" {:project/id "a" :project/dir "/a"
         :project/needs {"kafka" {} "pg" {}}}
    "b" {:project/id "b" :project/dir "/b"
         :project/needs {"kafka" {:resources {:mem "2g"}} "pg" {}}}
    "c" {:project/id "c" :project/dir "/c"
         :project/needs {"kafka" {:share? false} "big" {}}}}})

(defn- resolve! [settings consumers]
  (let [projects (:compose/projects settings)
        needs (:ok (cap/consumer-needs projects consumers {}))]
    [needs (:ok (cap/resolve-needs projects needs))]))

(deftest precedence
  (let [[needs _] (resolve! settings ["a" "b" "c"])]
    (testing "provide over default"
      (is (= {:mem gi :cpus 1 :unbounded? false :source :provide}
             (res/consumer-caps settings nil needs "a" "infra" "kafka"))))
    (testing "need over provide"
      (is (= (* 2 gi) (:mem (res/consumer-caps settings nil needs "b" "infra" "kafka"))))
      (is (= :need (:source (res/consumer-caps settings nil needs "b" "infra" "kafka")))))
    (testing "default when the provide says nothing"
      (is (= {:mem (* 256 mi) :unbounded? false :source :default}
             (res/consumer-caps settings nil needs "a" "infra" "pg"))))
    (testing "unbounded clears the default"
      (is (= {:unbounded? true :source :none}
             (res/consumer-caps settings nil needs "c" "infra" "big"))))
    (testing "stack over everything"
      (is (= (* 3 gi) (:mem (res/consumer-caps settings {:stack/resources {"kafka" {:mem "3g"}}}
                                               needs "b" "infra" "kafka")))))
    (testing "a stack-level unbounded"
      (is (:unbounded? (res/consumer-caps settings {:stack/resources {"pg" :unbounded}}
                                          needs "a" "infra" "pg"))))))

(deftest budget-shows-sharing
  (let [[needs resolution] (resolve! settings ["a" "b" "c"])
        b (res/budget settings nil needs resolution)]
    (is (m/validate schema/Budget b) (pr-str (schema/explain schema/Budget b)))
    (testing "the shared kafka takes the largest consumer cap"
      (is (= (* 2 gi) (get-in b [:budget/instances "infra/kafka" :mem]))))
    (testing "c's private kafka has the provide cap"
      (is (= gi (get-in b [:budget/instances "infra/kafka@c" :mem]))))
    (testing "the total counts shared instances once"
      ;; kafka 2g + kafka@c 1g + pg 256m; big unbounded
      (is (= (+ (* 3 gi) (* 256 mi)) (:budget/total-mem b)))
      ;; unshared: kafka a 1g + b 2g, pg a+b 512m, kafka@c 1g
      (is (= (+ (* 4 gi) (* 512 mi)) (:budget/unshared-mem b)))
      (is (= (+ gi (* 256 mi)) (:budget/saved-mem b))))
    (is (= ["infra/big"] (:budget/uncapped b)))
    (is (re-find #"total capped memory 3.3g \(sharing saves 1.3g" (res/summary b)))))

(deftest admission
  (let [[needs resolution] (resolve! settings ["a" "b"])
        b (res/budget settings nil needs resolution)]
    (is (r/ok? (res/admit b (res/limits settings nil nil))) "no limit, admitted")
    (is (r/ok? (res/admit b (res/limits (assoc settings :compose/budget "3g") nil nil))))
    (let [e (res/admit b (res/limits (assoc settings :compose/budget "2g") nil nil))]
      (is (= :compose/over-budget (:error e)))
      (is (m/validate schema/OverBudget (dissoc e :error)) (pr-str e))
      (is (= :budget (:basis e)))
      (is (= (* 256 mi) (:over e)))
      (is (= ["infra/kafka" "infra/pg"] (mapv :instance (:instances e)))))
    (testing "stack budget wins first"
      (is (= :stack (:basis (res/admit b (res/limits (assoc settings :compose/budget "1g")
                                                     {:stack/budget "1g"} nil))))))
    (testing "host: MemAvailable minus reserve, running instances free"
      (let [lims (res/limits (assoc settings :compose/host-reserve "4g") nil (* 6 gi))
            e (res/admit b lims)]
        (is (= :host (:basis e)))
        (is (= (* 2 gi) (:limit e)))
        (is (= (* 6 gi) (:available e)))
        (is (= (* 4 gi) (:reserve e)))
        (is (r/ok? (res/admit b lims #{"infra/kafka"})) "kafka is already up")))
    (testing "a reserve above MemAvailable floors at 0"
      (is (= 0 (:host (res/limits (assoc settings :compose/host-reserve "8g") nil gi)))))))

(deftest jvm
  (is (res/java-image? "eclipse-temurin:21-jre"))
  (is (res/java-image? "clojure:temurin-21-tools-deps"))
  (is (res/java-image? "registry.local/openjdk:17"))
  (is (res/java-image? "confluentinc/cp-kafka:7.6.0-jdk"))
  (is (not (res/java-image? "postgres:16")))
  (is (not (res/java-image? "redis:7-alpine")))
  (is (res/jvm? {} "eclipse-temurin:21"))
  (is (not (res/jvm? {:jvm? false} "eclipse-temurin:21")))
  (is (res/jvm? {:jvm? true} "app:latest"))
  (is (= "-XX:MaxRAMPercentage=75" (res/merge-jvm-options [res/jvm-flag] nil)))
  (is (= "-XX:MaxRAMPercentage=75 -Dfoo=1" (res/merge-jvm-options [res/jvm-flag] "-Dfoo=1")))
  (is (= "-XX:MaxRAMPercentage=50" (res/merge-jvm-options [res/jvm-flag] "-XX:MaxRAMPercentage=50"))
      "the consumer wins per flag")
  (is (= {"mem_limit" gi "cpus" 1
          "environment" {"JAVA_TOOL_OPTIONS" "-XX:MaxRAMPercentage=75 -Xmx1g"}}
         (res/service-override {:mem gi :cpus 1} true "-Xmx1g")))
  (is (= {} (res/service-override {:unbounded? true} false nil))))

(deftest meminfo-and-pressure
  (is (= (* 20202944 1024)
         (res/parse-meminfo "MemTotal:       64000000 kB\nMemFree:  1 kB\nMemAvailable:   20202944 kB\n")))
  (is (nil? (res/parse-meminfo "MemTotal: 1 kB")))
  (let [s {:compose/host-reserve "4g"
           :compose/stacks {"low" {:stack/id "low" :stack/consumers ["a"] :stack/priority -5}
                            "mid" {:stack/id "mid" :stack/consumers ["a"]}
                            "high" {:stack/id "high" :stack/consumers ["a"] :stack/priority 9}}}
        now 1000000
        ttl 60000
        runs {"low1" {:stack/id "low" :last-touch 10 :claims {"ag" {:seen (- now 10)}}}
              "mid1" {:stack/id "mid" :last-touch 50}
              "mid2" {:stack/id "mid" :last-touch 20}
              "high1" {:stack/id "high" :last-touch 0}
              "keep" {:stack/id "low" :last-touch 0 :idle-action :none}}]
    (is (res/under-pressure? s (* 3 gi)))
    (is (not (res/under-pressure? s (* 5 gi))))
    (is (not (res/under-pressure? s nil)) "unknown is never pressure")
    (is (not (res/under-pressure? {} 1)) "no reserve, no guard")
    (testing "claimed low is spared; lowest priority idle, longest idle first"
      (is (= "mid2" (res/pressure-victim s runs now ttl))))
    (testing "a stale claim no longer protects"
      (is (= "low1" (res/pressure-victim s runs (+ now ttl) ttl))))
    (is (nil? (res/pressure-victim s {"keep" (get runs "keep")} now ttl)))))

;; ---------------------------------------------------------------------------
;; Properties

(def gen-res
  (gen/frequency [[1 (gen/return nil)]
                  [1 (gen/return :unbounded)]
                  [4 (gen/fmap (fn [n] {:mem (* n mi)}) (gen/choose 1 4096))]]))

(def gen-world
  "Settings with one provider of kafka and n consumers, each with an optional
   Need :resources, plus which consumers opt out of sharing."
  (gen/let [n (gen/choose 1 6)
            default gen-res
            provide gen-res
            needs (gen/vector gen-res n)
            private (gen/vector gen/boolean n)
            budget (gen/choose 1 (* 4 4096))]
    (let [ids (mapv #(str "p" %) (range n))]
      {:ids ids
       :private (zipmap ids private)
       :budget (* budget mi)
       :settings (cond-> {:compose/projects
                          (into {"infra" {:project/id "infra" :project/dir "/infra"
                                          :project/provides {"kafka" (cond-> {:service "kafka"}
                                                                       provide (assoc :resources provide))}}}
                                (map (fn [id need]
                                       [id {:project/id id :project/dir (str "/" id)
                                            :project/needs {"kafka" (cond-> {}
                                                                      need (assoc :resources need))}}])
                                     ids needs))}
                   default (assoc :compose/default-resources default))})))

(defn- privatize [settings ids]
  (update settings :compose/projects
          (fn [ps] (reduce #(assoc-in %1 [%2 :project/needs "kafka" :share?] false) ps ids))))

(defspec sharing-never-raises-the-sum 200
  (prop/for-all [{:keys [settings ids private]} gen-world]
    (let [mixed (privatize settings (filter private ids))
          all-private (privatize settings ids)
          [n1 r1] (resolve! mixed ids)
          [n2 r2] (resolve! all-private ids)
          [n3 r3] (resolve! settings ids)
          b-mixed (res/budget mixed nil n1 r1)
          b-private (res/budget all-private nil n2 r2)
          b-shared (res/budget settings nil n3 r3)]
      (and (<= (:budget/total-mem b-shared) (:budget/total-mem b-mixed) (:budget/total-mem b-private))
           (= (:budget/unshared-mem b-shared) (:budget/unshared-mem b-private))
           (= (:budget/total-mem b-private) (:budget/unshared-mem b-private))
           (= (:budget/saved-mem b-shared)
              (- (:budget/unshared-mem b-shared) (:budget/total-mem b-shared)))))))

(defspec admission-never-admits-over-budget 300
  (prop/for-all [{:keys [settings ids budget]} gen-world
                 available (gen/choose 0 (* 16 1024))
                 reserve (gen/choose 1 (* 8 1024))
                 running (gen/vector gen/boolean 1 8)]
    (let [s (assoc settings :compose/budget budget :compose/host-reserve (* reserve mi))
          [needs resolution] (resolve! s ids)
          b (res/budget s nil needs resolution)
          inst-ids (vec (keys (:budget/instances b)))
          up (set (keep-indexed (fn [i id] (when (get running i) id)) inst-ids))
          lims (res/limits s nil (* available mi))
          verdict (res/admit b lims up)
          new-mem (reduce + 0 (keep (fn [[id c]] (when-not (up id) (:mem c))) (:budget/instances b)))]
      (if (r/ok? verdict)
        (and (<= (:budget/total-mem b) budget)
             (<= new-mem (max 0 (- (* available mi) (* reserve mi)))))
        (and (= :compose/over-budget (:error verdict))
             (pos? (:over verdict))
             (m/validate schema/OverBudget (dissoc verdict :error :running-mem)))))))
