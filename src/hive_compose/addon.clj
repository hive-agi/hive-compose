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
            [hive-dsl.result :as r])
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

(def commands
  ["status" "targets" "programs" "up" "switch" "down" "stop" "touch" "ps" "logs" "reap" "adopt" "projects" "reload"])

(declare reload!)

(defn- dispatch [a command params]
  (let [ctx (:ctx @(:state a))
        id (some-> (param params "profile") str)
        tgt (some-> (param params "target") str)
        on-subject (fn [f] (r/bind (ops/subject ctx {:target tgt :id id}) f))]
    (if (nil? ctx)
      (r/err :compose/not-initialized {:lifecycle (:lifecycle @(:state a))
                                       :errors (:errors @(:state a))})
      (case command
        "status" (r/ok (assoc (ops/status ctx) :last-pass (:last-pass @(:state a))))
        "targets" (ops/targets ctx)
        "programs" (ops/programs ctx)
        "up" (on-subject #(ops/up-profile! ctx %))
        "switch" (on-subject #(ops/switch-profile! ctx %))
        "down" (on-subject #(ops/down-profile! ctx % :down))
        "stop" (on-subject #(ops/down-profile! ctx % :stop))
        "touch" (on-subject #(ops/touch! ctx (:profile/id %)))
        "ps" (on-subject #(ops/ps-profile ctx %))
        "logs" (on-subject #(ops/logs-profile ctx % (or (some-> (param params "tail") long) 100)))
        "reap" (r/ok (tick! a))
        "adopt" (r/ok {:adopted (ops/adopt! ctx)})
        "projects" (ops/projects ctx)
        "reload" (reload! a)
        (r/err :compose/unknown-command {:command command :known commands})))))

(defn tool [a]
  {:name "compose"
   :description (str "docker compose sections for local development. Name a TARGET (a service or native compose "
                     "profile of a configured compose project, e.g. target=sisf-web, target=sisf/frontend, "
                     "target=sisf-web,sisf-crm) and exactly that runs, closed over the YAML's depends_on; "
                     "the section's id is <project>/<targets>. Presets from compose-profiles.edn work via profile=ID. "
                     "A section also starts the host PROGRAMS its project pairs with its services (a shadow-cljs "
                     "watch, a JVM): a Clojure directory starts with its nREPL, and up/status answer the port. "
                     "targets: what each project offers. programs: the host programs each project declares, how "
                     "each starts and whether its nREPL answers. status: active sections, idle time, reaper countdown. "
                     "up: start alongside others. switch: make current, stopping what only the previous one needed. "
                     "down/stop: remove/stop a section's containers (volumes never) and stop its programs, keeping "
                     "what other active sections need. touch: reset the idle clock (ps/logs touch too). reap: run "
                     "the idle reaper now. adopt: take charge of running stacks nobody owns. projects: every compose "
                     "project on the host. reload: re-read config.")
   :inputSchema {:type "object"
                 :properties {"command" {:type "string" :enum commands}
                              "target" {:type "string" :description "[up|switch|down|stop|touch|ps|logs] services or native profiles: name, project/name, or a,b"}
                              "profile" {:type "string" :description "[up|switch|down|stop|touch|ps|logs] preset or active section id"}
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
