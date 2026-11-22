(ns hive-compose.scenario.world
  "The shared world of the stack scenarios (hc-verify): one kafka provider,
   consumers A, B (share kafka ^3), C (kafka ^3, :share? false) and D (kafka
   ^2, which nobody provides), on the stub engine (IComposeEngine +
   IStackEngine). Every scenario drives the real pipeline
   (`hive-compose.pipeline.stacks`: resolve -> plan -> wiring -> engine) over
   it; only the engine is a stub. Helpers read the stub world as Docker would
   answer it: who an alias resolves to on a network, which containers run."
  (:require [clojure.string :as str]
            [hive-compose.adapter.cli :as cli]
            [hive-compose.pipeline.stacks :as stacks]
            [hive-compose.stub :as stub]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def kafka-provide
  {:service "kafka" :version "3.7.1"
   :env {"KAFKA_BROKERS" "kafka:9092"}
   :tenancy {:env "KAFKA_TOPIC_PREFIX"}})

(def projects
  {"infra" {:project/id "infra" :project/dir "/w/infra"
            :project/provides {"kafka" kafka-provide}}
   "a" {:project/id "a" :project/dir "/w/a" :project/needs {"kafka" {:version "^3"}}}
   "b" {:project/id "b" :project/dir "/w/b" :project/needs {"kafka" {:version "^3"}}}
   "c" {:project/id "c" :project/dir "/w/c" :project/needs {"kafka" {:version "^3" :share? false}}}
   "d" {:project/id "d" :project/dir "/w/d" :project/needs {"kafka" {:version "^2"}}}})

(def models
  "Compose models: kafka is KRaft (no zookeeper), each consumer one app."
  {"infra" {"kafka" {}}
   "a" {"app" {}} "b" {"app" {}} "c" {"app" {}} "d" {"app" {}}})

(def stack-defs
  {"ab" {:stack/id "ab" :stack/consumers ["a" "b"]}
   "abc" {:stack/id "abc" :stack/consumers ["a" "b" "c"]}
   "abd" {:stack/id "abd" :stack/consumers ["a" "b" "d"]}
   "a" {:stack/id "a" :stack/consumers ["a"]}
   "b" {:stack/id "b" :stack/consumers ["b"]}
   "c" {:stack/id "c" :stack/consumers ["c"]}})

(defn settings
  ([] (settings projects))
  ([projects]
   {:compose/default-ttl-minutes 60
    :compose/orphan-ttl-minutes 5
    :compose/claim-ttl-minutes 30
    :compose/default-idle-action :stop
    :compose/up-timeout-ms 120000
    :compose/profiles {}
    :compose/projects projects
    :compose/stacks stack-defs}))

(defn ctx
  "A pipeline context over a fresh stub world. `opts`: :running
   ({project #{service}} up before any run), :projects (override)."
  ([] (ctx {}))
  ([{:keys [running] :as opts}]
   (let [clock (atom 0) saved (atom nil)]
     {:engine (stub/engine {} (or running {}) models)
      :settings (settings (or (:projects opts) projects))
      :state (atom {:active {} :current nil})
      :now #(deref clock)
      :clock clock
      :saved saved
      :save! #(r/ok (reset! saved %))})))

(defn engine [c] (:engine c))

(defn up! [c stack run] (stacks/stack-up! c {:stack stack :run run}))

(defn down! [c run] (stacks/stack-down! c {:run run}))

(defn resolve-alias
  "Container names `alias` resolves to on `network`, as Docker DNS answers:
   every member joined under that alias."
  [c network alias]
  (vec (sort (for [[container aliases] (get-in (stub/networks (engine c)) [network :members])
                   :when (some #{alias} aliases)]
               container))))

(defn containers-of
  "Container names running `service` (any compose project)."
  [c service]
  (vec (sort (for [[n {s :service}] (stub/containers (engine c)) :when (= s service)] n))))

(defn mutating-calls
  "Engine calls that change the world (anything but reads)."
  [c]
  (filterv #(#{:up :down :stop :restart :recreate :up-project :down-project
               :network-create :network-rm :network-connect :rm-by-name :stop-by-name} (first %))
           (stub/calls (engine c))))

(defn down-argvs
  "The docker argv each recorded stack down would run (the CLI engine's own
   builder), so a scenario can check no down ever names volumes."
  [c]
  (for [[op project what] (stub/calls (engine c)) :when (= :down-project op)]
    (cli/down-project-argv (if (= :all what) {:project project :all? true} {:project project :services what}) nil)))

(defn volume-flag? [argv] (boolean (some #{"-v" "--volumes"} argv)))

(defn wiring-of
  "The Wiring of `consumer` in `run` as the pipeline computes it."
  [c stack run consumer]
  (get-in (stacks/resolve-plan c stack run) [:ok :wirings consumer]))

;; A broker the scenarios talk to through the view network, keyed by the
;; container an alias resolves to: it shows which kafka a consumer reaches
;; and that tenancy prefixes keep topics apart on a shared one. It stands in
;; for the real broker of phase B (docker), nothing more.

(defn broker [] (atom {}))

(defn produce! [b c network topic msg]
  (let [[k] (resolve-alias c network "kafka")]
    (swap! b update-in [k topic] (fnil conj []) msg)
    k))

(defn consume [b c network topic]
  (let [[k] (resolve-alias c network "kafka")]
    (get-in @b [k topic] [])))

(defn topics-on [b container] (vec (sort (keys (get @b container)))))

(defn prefix-of [w] (get-in w [:wiring/env "KAFKA_TOPIC_PREFIX"]))

(defn no-dash? [s] (not (str/includes? s "-")))
