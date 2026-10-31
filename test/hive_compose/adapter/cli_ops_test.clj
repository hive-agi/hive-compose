(ns hive-compose.adapter.cli-ops-test
  "`up` through CliEngine over an exec that answers like docker compose does
   for native profiles: `config --format json [targets]` shows a profiled
   service only when it is targeted or its profile is enabled."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.test :refer [deftest is]]
            [hive-compose.adapter.cli :as cli]
            [hive-compose.pipeline.ops :as ops]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- after [argv word] (vec (rest (drop-while #(not= word %) argv))))

(defn- enabled [model argv]
  (let [profiles (set (for [[a b] (partition 2 1 argv) :when (= a "--profile")] b))]
    (sort (for [[s {ps :profiles}] model :when (or (empty? ps) (some profiles ps))] s))))

(defn- closure [model targets]
  (loop [seen (sorted-set) todo (vec targets)]
    (if-let [[s & more] (seq todo)]
      (if (seen s)
        (recur seen (vec more))
        (recur (conj seen s) (into (vec more) (keys (get-in model [s :depends_on])))))
      seen)))

(defn- compose-exec
  "Exec answering `model` ({service {:profiles #{..} :depends_on {dep {\"required\" bool}}}})
   like docker compose; every `up` argv is recorded in `ups`."
  [model ups]
  (fn [argv _]
    (let [cmd (vec (drop-while #(not (#{"config" "ps" "up"} %)) argv))
          ok (fn [s] (r/ok {:exit 0 :stdout s :stderr ""}))]
      (cond
        (= ["config" "--profiles"] cmd)
        (ok (str/join "\n" (sort (distinct (mapcat :profiles (vals model))))))
        (= ["config" "--services"] cmd)
        (ok (str/join "\n" (enabled model argv)))
        (= ["config" "--format" "json"] (take 3 cmd))
        (let [targets (drop 3 cmd)]
          (if-let [missing (first (remove model targets))]
            (r/ok {:exit 1 :stdout "" :stderr (str "no such service: " missing)})
            (let [shown (closure model (if (seq targets) targets (enabled model argv)))]
              (ok (json/write-str
                   {"services" (into {} (for [s shown]
                                          [s (cond-> {}
                                               (get-in model [s :depends_on])
                                               (assoc "depends_on" (get-in model [s :depends_on])))]))})))))
        (= "ps" (first cmd)) (ok "")
        (= "up" (first cmd)) (do (swap! ups conj argv) (ok ""))
        :else (r/ok {:exit 1 :stdout "" :stderr (str "unexpected " argv)})))))

(defn- ctx [model ups]
  {:engine (cli/make-engine {:exec (compose-exec model ups)})
   :settings {:compose/default-ttl-minutes 60
              :compose/default-idle-action :stop
              :compose/profiles {}
              :compose/projects {"p" {:project/id "p" :project/dir "/w/p"}}}
   :state (atom {:active {} :current nil})
   :now (constantly 0)})

(defn- up! [c t] (r/bind (ops/subject c {:target t}) #(ops/up-profile! c %)))

(deftest a-profiled-app-with-a-required-db-and-an-optional-opt
  (let [ups (atom [])
        c (ctx {"app" {:profiles #{"debug"}
                       :depends_on {"db" {"required" true} "opt" {"required" false}}}
                "db" {}
                "opt" {}}
               ups)
        res (up! c "app")]
    (is (r/ok? res))
    (is (= ["app" "db"] (:services (:ok res))))
    (is (= ["up" "-d" "--no-deps" "app" "db"] (take-last 5 (last @ups)))
        "opt is neither named nor pulled in")))

(deftest a-profiled-app-whose-required-db-has-an-optional-opt
  (let [ups (atom [])
        c (ctx {"app" {:profiles #{"debug"} :depends_on {"db" {"required" true}}}
                "db" {:depends_on {"opt" {"required" false}}}
                "opt" {}}
               ups)
        res (up! c "app")]
    (is (r/ok? res))
    (is (= ["app" "db"] (:services (:ok res))) "db must start")
    (is (= ["up" "-d" "--no-deps" "app" "db"] (take-last 5 (last @ups))))))
