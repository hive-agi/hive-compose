(ns hive-compose.addon
  "IAddon `hive.compose`. Construction is pure. `initialize!` reads the config
   and profiles file, loads the active map and starts the reaper; `shutdown!`
   stops the reaper. Containers and host programs are never touched by the
   lifecycle itself.

   The reaper is one daemon thread waking every `:compose/tick-seconds`. Its first
   pass adopts running stacks of configured profiles (when `:compose/adopt?`);
   every pass forgets profiles taken down outside the addon and releases the ones
   idle past their TTL.

   Config: see `hive-compose.promote.config`. `:compose/engine` injects an
   IComposeEngine in place of the docker CLI, `:compose/runner` an
   IProgramRunner in place of the host one."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [hive-addon.protocol :as addon]
            [hive-compose.adapter.cli :as cli]
            [hive-compose.adapter.host :as host]
            [hive-compose.boundary.files :as files]
            [hive-compose.pipeline.ops :as ops]
            [hive-compose.promote.config :as config]
            [hive-compose.port :as port]
            [hive-dsl.result :as r]
            [hive-compose.promote.profile :as profile])
  (:import (java.util.concurrent Executors ScheduledExecutorService ThreadFactory TimeUnit)
           (java.util.concurrent.atomic AtomicBoolean)))

;; SPDX-License-Identifier: MIT

(def addon-id-value "hive.compose")

(defn- now-ms [] (System/currentTimeMillis))

(defn- daemon-scheduler ^ScheduledExecutorService []
  (Executors/newSingleThreadScheduledExecutor
   (reify ThreadFactory
     (newThread [_ runnable]
       (doto (Thread. ^Runnable runnable "hive-compose-reaper")
         (.setDaemon true))))))

;; ---------------------------------------------------------------------------
;; context

(defn- build-settings
  "Result<Settings> from merged config `cfg`, reading the profiles file."
  [cfg]
  (let [home (System/getProperty "user.home")]
    (r/let-ok [file-profiles (files/read-profiles (config/profiles-file cfg home))]
      (config/settings cfg file-profiles home))))

(defn- build-ctx [cfg settings persisted]
  {:engine (or (:compose/engine cfg) (cli/make-engine settings))
   :runner (or (:compose/runner cfg) (host/make-runner cfg))
   :settings settings
   :state (atom (merge {:active {} :current nil} (select-keys persisted [:active :current])))
   :now (or (:compose/now cfg) now-ms)
   :save! (or (:compose/save! cfg) #(files/save-state! (:compose/state-file settings) %))})

;; ---------------------------------------------------------------------------
;; reaper

(defn tick!
  "One reaper pass on the calling thread: adopt once, then reap. Answers the
   pass, or {:skipped ..} when not active or a pass is running."
  [{:keys [state]}]
  (let [{:keys [ctx ^AtomicBoolean busy adopted?]} @state]
    (cond
      (nil? ctx) {:skipped :not-initialized}
      (not (.compareAndSet busy false true)) {:skipped :busy}
      :else
      (try
        (let [adopted (when (and (not adopted?) (get-in ctx [:settings :compose/adopt?]))
                        (ops/adopt! ctx))
              pass (cond-> (ops/reap! ctx) (seq adopted) (assoc :adopted adopted))]
          (swap! state assoc :adopted? true :last-pass (assoc pass :at ((:now ctx))))
          pass)
        (catch Throwable t
          (swap! state assoc :last-pass {:at (now-ms) :error (str t)})
          {:error (str t)})
        (finally (.set busy false))))))

;; ---------------------------------------------------------------------------
;; tool

(defn- text-result [x]
  {:content [{:type "text" :text (json/write-str x :escape-slash false)}]})

(defn- result->content [res]
  (if (r/ok? res)
    (text-result (:ok res))
    (assoc (text-result res) :isError true)))

(defn- param [params k]
  (or (get params k) (get params (keyword k))))

(def command-help
  "One line per command: what it does and the params it reads."
  (array-map
   "help" "this list"
   "status" "active sections: services, idle time, reaper countdown, host programs"
   "targets" "what each configured project offers as a target: services and native profiles"
   "programs" "host programs each project declares, how each starts, whether its nREPL answers"
   "up" "start a section alongside the others. target|profile; no_deps (only the named services), wait (until healthy), env {VAR value}"
   "switch" "make a section current, stopping what only the previous one needed. same params as up"
   "down" "remove a section's containers (never volumes), keeping what other sections need. target|profile"
   "stop" "stop a section's containers, keeping what other sections need. target|profile"
   "touch" "reset the idle clock. target|profile for one section; none for every active one; project to narrow"
   "ps" "what a section needs and which of it runs. target|profile"
   "logs" "last log lines of a section. target|profile; tail"
   "reap" "run the idle reaper now"
   "adopt" "take charge of running containers no section owns"
   "projects" "every compose project on the host"
   "reload" "re-read the config"))

(def commands
  (vec (keys command-help)))

(declare reload!)

(defn- flag
  "A boolean param as sent over MCP (true, \"true\"), nil when absent."
  [params k]
  (let [v (param params k)]
    (cond (nil? v) nil
          (string? v) (= "true" (str/lower-case v))
          :else (boolean v))))

(defn- call-options
  "The per-call options of `up`/`switch`, from the tool params."
  [params]
  {:no-deps? (flag params "no_deps")
   :wait? (flag params "wait")
   :env (some->> (param params "env")
                 (into {} (map (fn [[k v]] [(name k) (str v)]))))})

(defn- dispatch [a command params]
  (let [ctx (:ctx @(:state a))
        id (some-> (param params "profile") str)
        tgt (some-> (param params "target") str)
        on-subject (fn [f] (r/bind (ops/subject ctx {:target tgt :id id}) f))
        opts (call-options params)]
    (cond
      (= "help" command) (r/ok {:commands command-help})
      (nil? ctx) (r/err :compose/not-initialized {:lifecycle (:lifecycle @(:state a))
                                                  :errors (:errors @(:state a))})
      :else
      (case command
        "status" (r/ok (assoc (ops/status ctx) :last-pass (:last-pass @(:state a))))
        "targets" (ops/targets ctx)
        "programs" (ops/programs ctx)
        "up" (on-subject #(ops/up-profile! ctx (profile/with-call-options % opts)))
        "switch" (on-subject #(ops/switch-profile! ctx (profile/with-call-options % opts)))
        "down" (on-subject #(ops/down-profile! ctx % :down))
        "stop" (on-subject #(ops/down-profile! ctx % :stop))
        "touch" (if (or tgt id)
                  (on-subject #(ops/touch! ctx (:profile/id %)))
                  (ops/touch-all! ctx (some-> (param params "project") str)))
        "ps" (on-subject #(ops/ps-profile ctx %))
        "logs" (on-subject #(ops/logs-profile ctx % (or (some-> (param params "tail") long) 100)))
        "reap" (r/ok (tick! a))
        "adopt" (r/ok {:adopted (ops/adopt! ctx)})
        "projects" (ops/projects ctx)
        "reload" (reload! a)
        (r/err :compose/unknown-command {:command command :known commands
                                         :hint "command=help lists what each does"})))))

(defn tool [a]
  {:name "compose"
   :description (str "docker compose sections for local development. Name a TARGET (a service or native compose "
                     "profile of a configured compose project, e.g. target=sisf-web, target=sisf/frontend, "
                     "target=sisf-web,sisf-crm) and exactly that runs, closed over the YAML's depends_on; "
                     "the section's id is <project>/<targets>. Presets from compose-profiles.edn work via profile=ID. "
                     "A section also starts the host PROGRAMS its project pairs with its services (a shadow-cljs "
                     "watch, a JVM): a Clojure directory starts with its nREPL, and up/status answer the port. "
                     "A section keeps alive everything its services depend on, so releasing or reaping another "
                     "section never stops a dependency this one needs. "
                     "up/switch take no_deps (start only the named services, e.g. past a failing migration), "
                     "wait (until healthy) and env. touch with no target resets every active section. "
                     "command=help lists every command with its params.")
   :inputSchema {:type "object"
                 :properties {"command" {:type "string" :enum commands}
                              "target" {:type "string" :description "[up|switch|down|stop|touch|ps|logs] services or native profiles: name, project/name, or a,b"}
                              "profile" {:type "string" :description "[up|switch|down|stop|touch|ps|logs] preset or active section id"}
                              "no_deps" {:type "boolean" :description "[up|switch] start only the named services, not their depends_on (which must already run)"}
                              "wait" {:type "boolean" :description "[up|switch] wait until the started services are healthy"}
                              "env" {:type "object" :description "[up|switch] extra environment for compose interpolation, e.g. {\"HOST_GATEWAY_IP\": \"192.168.1.5\"}"}
                              "project" {:type "string" :description "[touch] with no target: only the sections of this project"}
                              "tail" {:type "integer" :description "[logs] lines per service (default 100)"}}
                 :required ["command"]}
   :handler (fn [params]
              (result->content (dispatch a (str (param params "command")) params)))})

;; ---------------------------------------------------------------------------
;; lifecycle

(defn- start! [a cfg settings]
  (let [{:keys [state]} a
        persisted (files/load-state (:compose/state-file settings))
        ctx (build-ctx cfg settings (if (r/ok? persisted) (:ok persisted) {}))
        ^ScheduledExecutorService sched (daemon-scheduler)
        tick (:compose/tick-seconds settings)]
    (swap! state assoc :lifecycle :active :cfg cfg :ctx ctx :scheduler sched :adopted? false
           :errors (when (r/err? persisted) [(pr-str persisted)]))
    (.scheduleWithFixedDelay sched
                             ^Runnable (fn []
                                         (try (tick! a)
                                              (catch Throwable t
                                                (swap! state assoc :last-pass {:at (now-ms) :error (str t)}))))
                             (long (or (:compose/initial-delay-ms cfg) 5000))
                             (long (* 1000 tick))
                             TimeUnit/MILLISECONDS)
    {:success? true :errors []
     :metadata {:profiles (count (:compose/profiles settings))
                :active (count (:active @(:state ctx)))
                :tick-seconds tick}}))

(defn reload!
  "Re-read config and profiles file into the running context, keeping the
   active map. Result<{:profiles n}>."
  [{:keys [state]}]
  (locking state
    (let [{:keys [cfg ctx]} @state]
      (r/let-ok [settings (build-settings cfg)]
        (swap! state assoc :ctx (assoc ctx :settings settings))
        (r/ok {:profiles (vec (sort (keys (:compose/profiles settings))))})))))

(defn- initialize-addon! [a seed runtime-config]
  (let [{:keys [state]} a]
    (locking state
      (if (= :active (:lifecycle @state))
        {:success? true :already-initialized? true}
        (let [cfg (merge (:addon/config seed) seed (:addon/config runtime-config) runtime-config)
              settings (build-settings cfg)]
          (if (r/err? settings)
            (do (swap! state assoc :lifecycle :error :errors [(pr-str settings)])
                {:success? false :errors [(str "invalid hive.compose config: " (pr-str settings))]})
            (try
              (start! a cfg (:ok settings))
              (catch Exception e
                (swap! state assoc :lifecycle :error :errors [(ex-message e)])
                {:success? false :errors [(ex-message e)]}))))))))

(defn- shutdown-addon! [{:keys [state]}]
  (locking state
    (when-let [^ScheduledExecutorService sched (:scheduler @state)]
      (.shutdownNow sched))
    (swap! state #(-> % (dissoc :scheduler :ctx) (assoc :lifecycle :stopped))))
  nil)

(defrecord HiveComposeAddon [state seed]
  addon/IAddon
  (addon-id [_] addon-id-value)
  (addon-type [_] :native)
  (capabilities [_] #{:tools :health-reporting :compose})
  (initialize! [this runtime-config] (initialize-addon! this seed runtime-config))
  (shutdown! [this] (shutdown-addon! this))
  (tools [this] [(tool this)])
  (schema-extensions [_] {})
  (health [_]
    (let [{:keys [lifecycle ctx errors]} @state]
      (if (and (= :active lifecycle) ctx)
        (let [v (port/-version (:engine ctx))]
          {:status (if (r/ok? v) :ok :degraded)
           :details (cond-> {:profiles (count (get-in ctx [:settings :compose/profiles]))
                             :active (vec (sort (keys (:active @(:state ctx)))))}
                      (r/ok? v) (assoc :compose-version (:ok v))
                      (r/err? v) (assoc :engine-error v))})
        {:status :down :details {:lifecycle lifecycle :errors errors}})))
  (excluded-tools [_] #{})
  (hooks [this]
    {:compose/touch! (fn [id] (if-let [ctx (:ctx @state)] (ops/touch! ctx id) (r/err :compose/not-initialized)))
     :compose/status (fn [] (some-> (:ctx @state) ops/status))
     :compose/tick! (fn [] (tick! this))}))

(defn make-addon
  "Uninitialized IAddon. No thread, file or container is touched."
  ([] (make-addon {}))
  ([seed] (->HiveComposeAddon (atom {:lifecycle :new :busy (AtomicBoolean. false)})
                              (or seed {}))))

(defn addon-ctor
  "hive-addon.mount constructor: config -> uninitialized IAddon."
  [config]
  (make-addon config))
