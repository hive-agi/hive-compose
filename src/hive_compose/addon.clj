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
   IProgramRunner in place of the host one, `:compose/history` an
   IFlakeHistory in place of the history file, `:compose/resolver` the
   {scheme ISecretBackend} env references are read through in place of `pass`."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [hive-addon.protocol :as addon]
            [hive-compose.adapter.cli :as cli]
            [hive-compose.adapter.host :as host]
            [hive-compose.adapter.suite-runner :as suite-runner]
            [hive-compose.pipeline.data :as data]
            [hive-compose.pipeline.suites :as suites]
            [hive-compose.boundary.files :as files]
            [hive-compose.boundary.history :as history]
            [hive-compose.pipeline.faults :as faults]
            [hive-compose.pipeline.flaky :as flaky]
            [hive-compose.pipeline.isolated :as isolated]
            [hive-compose.promote.isolated :as iso-model]
            [hive-compose.promote.target :as target]
            [hive-compose.pipeline.ops :as ops]
            [hive-compose.pipeline.stacks :as stacks]
            [hive-compose.pipeline.why :as why]
            [hive-compose.adapter.tempo :as tempo]
            [hive-compose.promote.config :as config]
            [hive-compose.promote.env :as env]
            [hive-compose.port :as port]
            [hive-compose.schema :as schema]
            [hive-dsl.result :as r]
            [hive-compose.promote.profile :as profile]
            [hive-compose.pipeline.restart :as restart]
            [hive-compose.pipeline.swarm :as swarm]
            [hive-compose.adapter.swarm :as swarm-adapter]
            [hive-compose.adapter.procnet :as procnet]
            [hive-compose.adapter.meminfo :as meminfo]
            [hive-compose.adapter.procfs :as procfs]
            [hive-compose.pipeline.orphans :as orphans]
            [hive-compose.pipeline.ownership :as ownership]
            [hive-addon.host :as addon-host]
            [hive-compose.api :as api]
            [hive-compose.adapter.http :as http]
            [hive-compose.adapter.exec])
  (:import (java.util.concurrent Executors ScheduledExecutorService ThreadFactory TimeUnit)
           (java.util.concurrent.atomic AtomicBoolean)))

;; SPDX-License-Identifier: MIT

(def addon-id-value "hive.compose")

(defn- now-ms [] (System/currentTimeMillis))

(def default-tempo-url
  "Where `why` reads traces when `:compose/tempo-url` names none: the otel
   provider's Tempo port as published on the host."
  "http://127.0.0.1:3200")

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

(def ^:private host-declared-config
  "The host's reader of an addon's declared config block (`:addons <id>` in
   config.edn), the same one its init path merges into the config `initialize!`
   receives. Soft-resolved: the published addon never requires hive-mcp, and
   with no host it answers {}."
  (addon-host/soft 'hive-mcp.addons.core/addon-declared-config (constantly {})))

(defn- declared-config
  "{:block m :source s}: the addon config block as declared NOW and where it
   came from. `:compose/config-source` (a 0-arity fn, for tests and non-MCP
   hosts) when `base` carries one, else the host's declared-config reader.
   Anything but a map counts as {}."
  [base]
  (let [own (:compose/config-source base)
        m (if own (own) (host-declared-config addon-id-value))]
    {:block (if (map? m) m {})
     :source (cond own :config-source
                   (map? m) :host-config
                   :else :none)}))

(defn- merged-config
  "The config the addon runs on: `seed`, then the declared block, then the
   caller's own options. `runtime-config` is what `initialize!` received; the
   host built it by merging the block declared at that time (`declared-then`)
   under its options, so the entries it still holds unchanged from that block
   are dropped and the block declared now takes their place. One function for
   init and reload, so a reload sees exactly what a fresh init would."
  [seed runtime-config declared-then]
  (let [base (merge (:addon/config seed) seed (:addon/config runtime-config))
        own (into {} (remove (fn [[k v]] (and (contains? declared-then k)
                                              (= v (get declared-then k)))))
                  runtime-config)
        {:keys [block source]} (declared-config (merge base own))]
    {:cfg (merge base block own)
     :declared block
     :source source}))

(defn- build-ctx
  "The context. Unless `:compose/save!` is injected, every save merges with the
   state file (`files/sync-state!`) and `:sync!` does the same from the reaper,
   so an instance sharing the file through a hot remount loses nothing."
  [cfg settings persisted]
  (let [st (merge {:active {} :current nil} (select-keys persisted [:active :current :stacks :preexisting :runs]))
        state (atom st)
        base (atom st)
        sync! #(files/sync-state! (:compose/state-file settings) base state)]
    {:engine (or (:compose/engine cfg)
                 (cli/make-engine (merge settings (select-keys cfg [:compose/resolver]))))
     :runner (or (:compose/runner cfg) (host/make-runner cfg))
     ;; hc-8 suites under their hc-16b caps, secrets read at spawn
     :suite-runner (or (:compose/suite-runner cfg)
                       (suite-runner/make-runner (select-keys cfg [:compose/resolver :compose/systemd-scope?])))
     ;; every cell result goes to the flake history (hc-13) ...
     :record-run! (fn [ctx run] (flaky/record-run! ctx run))
     ;; ... and the stack's data lifecycle runs around and between suites (hc-9)
     :data-phase! (fn [ctx run phase] (data/run-phase! ctx run phase))
     :default-resources (:compose/default-resources settings)
     :swarm (or (:compose/swarm cfg) (swarm-adapter/make-swarm))
     :activity (or (:compose/activity-probe cfg) (procnet/make-activity))
     :processes (if (contains? cfg :compose/process-table)
                  (:compose/process-table cfg)
                  (when (get cfg :compose/orphan-guard? true) (procfs/make-process-table)))
     :host-memory (if (contains? cfg :compose/host-memory)
                    (:compose/host-memory cfg)
                    (meminfo/make-host-memory))
     :history (or (:compose/history cfg)
                  (history/file-history (:compose/history-file settings)))
     :settings settings
     :state state
     :faults (atom {})
     :toxiproxy (:compose/toxiproxy cfg)
     :now (or (:compose/now cfg) now-ms)
     :save! (or (:compose/save! cfg) (fn [_] (sync!)))
     :sync! (when-not (:compose/save! cfg) sync!)
     ;; hc-10 why: the trace store the otel provider runs (Tempo on its host port)
     :trace-query (or (:compose/trace-query cfg)
                      (tempo/make-query {:base-url (or (:compose/tempo-url cfg) default-tempo-url)}))
     :state-dir (:compose/runs-dir cfg)
     :note! (:compose/why-note! cfg)}))

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
        (let [_ (when-let [sync! (:sync! ctx)] (sync!))
              adopted (when (and (not adopted?) (get-in ctx [:settings :compose/adopt?]))
                        (ops/adopt! ctx))
              observed (swarm/observe! ctx)
              reaped (ops/reap! ctx)
              _ (swarm/announce-reaped! ctx (:reaped reaped))
              ;; an isolated run whose last section was just reaped ends, and
              ;; its own volumes go with it (Pedro, 2026-11-23)
              ;; step 2 first: volumes whose grace ran out on an EARLIER pass
              runs-swept (isolated/sweep-due! ctx)
              _ (isolated/note-observed! ctx observed)
              runs-ended (into (isolated/after-reap! ctx (:reaped reaped) observed)
                               (when (= :ok (:swarm observed)) (isolated/end-orphans! ctx)))
              reaped (cond-> reaped
                       (seq runs-ended) (assoc :runs-ended runs-ended)
                       (seq runs-swept) (assoc :runs-swept runs-swept))
              stack-runs (:reaped (stacks/reap! ctx))
              pass (cond-> (assoc reaped :observed observed)
                     (seq adopted) (assoc :adopted adopted)
                     (seq stack-runs) (assoc :stacks-reaped stack-runs))]
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
   "up" "start a section alongside the others; refused while processes of a dead docker daemon hold its volumes or ports. target|profile; no_deps (only the named services), wait (until healthy), env {VAR value | VAR {\"pass\": path}}, allow_orphans (go ahead anyway); refused when another live section holds a service it would recreate (other compose files, dir or env), naming the section and its agents, unless allow_shared"
   "switch" "make a section current, stopping what only the previous one needed. same params as up"
   "down" "remove a section's containers (never volumes), keeping what other sections need; :kept names those and :note says when nothing was removed. target|profile"
   "stop" "stop a section's containers, keeping what other sections need; :kept names those and :note says when nothing was stopped. target|profile"
   "restart" "restart a section's services in place even when other sections need them; recreate=true recreates the containers (never their depends_on), refused when another live section holds one of them unless allow_shared. target|profile; env; allow_orphans; allow_shared"
   "touch" "reset the idle clock. target|profile for one section; none for every active one; project to narrow"
   "ps" "what a section needs and which of it runs. target|profile"
   "logs" "last log lines of a section. target|profile; tail"
   "claim" "an agent declares it works on a section; on an active or fully running section it only renews or takes it (never starts or recreates a container), else it brings it up through the same guards as up (refused, naming the holder section and its agents, when that would recreate a service another live section holds); the answer names the shared nREPL per module to drive. target (an active section id, a preset id, or services) | profile | target=stack:<run>; agent (default agent_id); note; ttl_minutes (a wave claim, held whether or not the owner stays in the swarm; needs note). Renew before claim TTL"
   "release" "an agent gives up its claim (a wave claim too; agent= names whose claim, and any caller may name any agent: there is no auth); with no claim left the section is reaped after the orphan TTL, or now with stop=true. target (an active section id, a preset id, or services) | profile | target=stack:<run>; agent; stop"
   "who" "per active section: claims (owner, note, wave?, expires-in-seconds), what the last pass saw using it (claims, agents working in its project, new nREPL connections), when it is reaped; per live agent the sections it works in"
   "nrepl" "which shared nREPL drives a module: path (a file or dir) for the closest one, project to filter, none for all; each with advice"
   "reap" "run the idle reaper now (usage pass, then reap)"
   "adopt" "take charge of running containers no section owns"
   "projects" "every compose project on the host"
   "flaky" "test cases ranked by intermittent failure across recorded suite runs, each :flaky, :regression (failing since a named run and sha), :fixed or :stable. suite; project; limit (default 20)"
   "faults" "fault injection on a stack run whose needs go :via :toxiproxy: op add|remove|reset|list (default list); run (run id); proxy (<consumer>.<capability>, see TOXIPROXY_PROXIES); toxic {type latency|timeout|bandwidth|reset_peer|slow_close|limit_data|slicer, attributes {name number}, stream upstream|downstream, toxicity 0..1, name}"
   "reload" "re-read the config"
   "stack-up" "run a stack (:compose/stacks): resolve capabilities, start shared and private instances in order, each waited and health-gated, join them to each consumer's view network hive-<run>-<consumer>, then start the consumers with their wiring env. stack; run (default <stack><6 hex>); wait (default true)"
   "stack-down" "take a stack run down: consumers, view networks, then the instances no other live run or section keeps (never volumes, never what ran before the run). run, or stack for every run of it"
   "stack-status" "stack runs with their instances' and consumers' containers now. stack; run"
   "plan" "what stack-up would start, share or find running (terraform-plan style). stack; run"
   "why" "explain a failed test: its case, the deepest error span carrying a message (trace store, :compose/tempo-url), the span path from the root, container logs of its run in its time window and the stack plan; written to <state>/runs/<run>/why/<case>.md+.edn. test (case id or suite/project/case); run (default the newest run holding it)"
   "test" "run the suite x project matrix (:compose/suites): one stack for the union of what the cells need (or the live run named by run), each suite's runner (a container on the projects' view networks, or a host command) with HIVE_RUN_ID, TRACEPARENT and BAGGAGE test.id, JUnit/TAP read from its reports dir; a summary is written to <state>/runs/<run>/summary.edn; down when every cell passed, kept up for inspection when one failed (the reaper TTL still applies). suites=X,Y and projects=A,B (default all); run (default test<6 hex>); keep_up (keep a passing stack up); dry_run (plan and runner specs only, nothing starts); wait (default true); variants=a,b (the :suite/variants to run, each a cell <suite>@<variant>, default all; every selected suite that declares variants must declare each named one, else the call is refused; a suite without variants runs once). The variant env is set on the suite RUNNER only: the stack and its consumers are NOT restarted or re-env'd per variant, so a variant that toggles a service's own flag tests nothing (follow-up 20261123122851-6e29b6f7); skip_fails (a skipped case fails its cell). exit 0 every cell passed, 1 a test failed, 2 something could not run. A cell passes only when its runner exited 0 and its report parsed: an unparsable report is could-not-run; a container suite, or one that declares :suite/reports-dir or :suite/report-format, must leave a report with at least one case unless :suite/report-required? false. Every case is listed with id, status and time; a could-not-run cell carries :reason and, when known, :service"))

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
  "The per-call options of `up`/`switch`/`restart`, from the tool params. An
   env value {\"pass\" path} stays a reference, read only when containers are
   created."
  [params]
  {:no-deps? (flag params "no_deps")
   :wait? (flag params "wait")
   :env (some->> (param params "env")
                 (into {} (map (fn [[k v]] [(name k) (env/from-param v)]))))})

(defn- checked-options
  "Result<opts>: `opts` when its env is an Env, else why not. The problems
   name the variable and the rule broken, never a value."
  [opts]
  (if-let [problems (some->> (:env opts) (schema/explain schema/Env))]
    (r/err :compose/invalid-param {:param "env" :problems problems
                                   :hint "a value is a string or {\"pass\": \"<store path>\"}"})
    (r/ok opts)))

(defn- agent-param
  "The claiming agent: `agent`, else the `agent_id` the host stamps on a call."
  [params]
  (some-> (or (param params "agent") (param params "agent_id")) str))

(defn- ttl-minutes-param
  "Result<minutes-or-nil>: the `ttl_minutes` of a claim (a JSON integer, or
   digits as text), nil when absent; anything else is an invalid-param."
  [params]
  (let [v (param params "ttl_minutes")]
    (cond
      (nil? v) (r/ok nil)
      (int? v) (r/ok (long v))
      (and (string? v) (re-matches #"\d{1,9}" v)) (r/ok (Long/parseLong v))
      :else (r/err :compose/invalid-param {:param "ttl_minutes" :value v
                                           :hint "a whole number of minutes, at least 1"}))))

(defn- stack-opts
  "The options of `stack-up`/`stack-down`/`stack-status`/`plan` from the tool
   params: stack, run, wait (stack-up waits by default)."
  [params]
  (cond-> {}
    (param params "stack") (assoc :stack (str (param params "stack")))
    (param params "run") (assoc :run (str (param params "run")))
    (some? (flag params "wait")) (assoc :wait? (flag params "wait"))))

(defn- isolated-opts
  "Result<{:run :ports :shared-volumes}> when the call asks for an isolated
   run (isolated=true, or run=<id> on up/switch), else nil."
  [params]
  (let [run (some-> (param params "run") str)
        iso? (flag params "isolated")
        shared (param params "shared_volumes")]
    (when (or iso? run)
      (cond
        (and shared (not (sequential? shared)))
        (r/err :compose/invalid-param {:param "shared_volumes" :hint "a list of volume keys"})
        :else
        (let [ports (iso-model/port-policy (param params "ports"))]
          (if (and ports (schema/explain schema/PortPolicy ports))
            (r/err :compose/invalid-param {:param "ports" :problems (schema/explain schema/PortPolicy ports)
                                           :hint "\"strip\" | \"ephemeral\" | \"keep\" | {\"offset\": n} | {\"publish\": {svc {port host}}}"})
            (r/ok (cond-> {}
                    run (assoc :run run)
                    ports (assoc :ports ports)
                    shared (assoc :shared-volumes (mapv str shared))))))))))

(defn- test-opts
  "The options of `test` from the tool params: suites, projects (\"X,Y\" or a
   list), run, keep_up, dry_run, wait."
  [params]
  (cond-> {}
    (param params "suites") (assoc :suites (param params "suites"))
    (param params "projects") (assoc :projects (param params "projects"))
    (param params "run") (assoc :run (str (param params "run")))
    (some? (flag params "keep_up")) (assoc :keep-up? (flag params "keep_up"))
    (some? (flag params "dry_run")) (assoc :dry-run? (flag params "dry_run"))
    (some? (flag params "wait")) (assoc :wait? (flag params "wait"))
    (param params "variants") (assoc :variants (param params "variants"))
    (some? (flag params "skip_fails")) (assoc :skip-fails? (flag params "skip_fails"))))

(def test-params
  "The params `test-opts` reads; `command-params` \"test\" must equal it
   (addon_test checks), so the API never refuses what `test` reads."
  #{"suites" "projects" "run" "keep_up" "dry_run" "wait" "variants" "skip_fails"})

(defn- announced
  "`res` of an up or switch, with the swarm told about the section's shared
   nREPLs (:announced, when a notice went out)."
  [ctx res]
  (if (r/ok? res)
    (let [n (swarm/announce-up! ctx (get-in res [:ok :profile]))]
      (cond-> res (and n (r/ok? n)) (assoc-in [:ok :announced] true)))
    res))

(defn- faults-request
  "Result<request> of the `faults` command's params: run, op, proxy and toxic
   ({type attributes stream toxicity name}, keys as JSON gives them)."
  [params]
  (let [toxic (param params "toxic")]
    (if (and (some? toxic) (not (map? toxic)))
      (r/err :compose/invalid-param {:param "toxic" :hint "an object {type, attributes, stream, toxicity, name}"})
      (r/ok {:run (some-> (param params "run") str)
             :op (some-> (param params "op") str)
             :proxy (some-> (param params "proxy") str)
             :toxic (when toxic
                      (let [t (into {} (map (fn [[k v]] [(keyword (name k)) v])) toxic)]
                        (cond-> t
                          (map? (:attributes t)) (update :attributes #(into {} (map (fn [[k v]] [(name k) v])) %)))))}))))

(defn dispatch
  "Result of `command` with `params` (the tool's params, string or keyword
   keys) on addon `a`: the one dispatch both transports run, the MCP `compose`
   tool (`tool`) and the control API (`hive-compose.api`)."
  [a command params]
  (let [ctx (:ctx @(:state a))
        run-param (some-> (param params "run") str)
        ;; an isolated run's section: profile=X run=R or target=T run=R
        ;; address the active entry <id>@R (up/switch resolve it themselves)
        in-run? (and run-param (not (#{"up" "switch" "stack-up" "stack-down" "stack-status" "plan" "faults" "why" "test"} command)))
        id (some-> (param params "profile") str (cond-> in-run? (iso-model/section-id run-param)))
        tgt (when-not (and in-run? (param params "target"))
              (some-> (param params "target") str))
        id (or id (when (and in-run? (param params "target"))
                    (let [{:keys [project names]} (target/parse (str (param params "target")))]
                      (when project (iso-model/section-id (target/section-id project names) run-param)))))
        opts (call-options params)
        allow-orphans? (flag params orphans/override-param)
        allow-shared? (flag params ownership/override-param)
        guard (fn [p mode recreate? f]
                (ownership/guarded ctx p mode recreate? allow-shared?
                                   #(orphans/guarded ctx p allow-orphans? f)))
        on-subject* (fn [f adjust]
                      (r/bind (checked-options opts)
                              (fn [o]
                                (r/bind (ops/subject ctx {:target tgt :id id :env (:env o)})
                                        #(f (adjust % o))))))
        on-subject (fn [f] (on-subject* f (fn [p o] (profile/with-call-options p (select-keys o [:env])))))
        ;; claim/release: target is first an active section id or a preset id
        on-section (fn [f] (r/bind (ops/section-subject ctx {:target tgt :id id}) f))
        claim-opts (fn [] (r/map-ok (ttl-minutes-param params)
                                    (fn [t] {:note (some-> (param params "note") str) :ttl-minutes t})))
        with-opts (fn [f] (on-subject* f profile/with-call-options))]
    (cond
      (= "help" command) (r/ok {:commands command-help})
      (nil? ctx) (r/err :compose/not-initialized {:lifecycle (:lifecycle @(:state a))
                                                  :errors (:errors @(:state a))})
      :else
      (case command
        "status" (r/ok (cond-> (merge (assoc (ops/status ctx) :last-pass (:last-pass @(:state a)))
                                      (let [s (isolated/status ctx)] (when (seq (:runs s)) s)))
                         (:api @(:state a)) (assoc :api (:api @(:state a)))))
        "targets" (ops/targets ctx)
        "programs" (ops/programs ctx)
        "up" (if-let [iopts (isolated-opts params)]
               (r/bind iopts (fn [io] (with-opts (fn [p] (if (flag params "dry_run")
                                                            (isolated/plan ctx p io)
                                                            (orphans/guarded ctx p allow-orphans?
                                                                             #(isolated/up! ctx p io)))))))
               (if (flag params "dry_run")
                 ;; M4: a plain up has no plan mode; never run it for real
                 (r/err :compose/invalid-param {:param "dry_run" :hint "dry_run plans an isolated run: add isolated=true or run=<id>"})
                 (announced ctx (with-opts (fn [p] (guard p :up false #(ops/up-profile! ctx p)))))))
        "switch" (if (isolated-opts params)
                   (r/err :compose/invalid-param {:param "run" :hint "an isolated run has no current section: use up run=<id>"})
                   (announced ctx (with-opts (fn [p] (guard p :up false #(ops/switch-profile! ctx p))))))
        "claim" (r/bind (claim-opts)
                        (fn [o]
                          (if-let [run (stacks/claim-run tgt)]
                            (stacks/claim! ctx run (agent-param params) o)
                            ;; a claim on an isolated run's section cancels a pending volume removal
                            (on-section (fn [p] (isolated/cancel-due! ctx (:profile/id p))
                                          (swarm/claim! ctx p (agent-param params)
                                                              ;; a claim that must start the section passes up's guards
                                                              (assoc o :start! #(guard p :up false (fn [] (ops/up-profile! ctx p))))))))))
        "release" (if-let [run (stacks/claim-run tgt)]
                    (stacks/release-claim! ctx run (agent-param params) {:stop? (flag params "stop")})
                    (on-section #(swarm/release-claim! ctx % (agent-param params)
                                                       {:stop? (flag params "stop")})))
        ;; stack-up/stack-down/claim/release and the reaper's stacks/reap! take
        ;; the one stack lock of ctx's state (stacks/serialized): never concurrent
        "stack-up" (stacks/stack-up! ctx (stack-opts params))
        "stack-down" (stacks/stack-down! ctx (stack-opts params))
        "stack-status" (stacks/stack-status ctx (stack-opts params))
        "plan" (stacks/plan ctx (stack-opts params))
        "why" (r/map-ok (why/why! ctx {:test (some-> (param params "test") str)
                                       :run (some-> (param params "run") str)})
                        #(dissoc % :why/markdown))
        "test" (suites/test! ctx (test-opts params))
        "who" (swarm/who ctx)
        "nrepl" (swarm/nrepl ctx {:path (some-> (param params "path") str)
                                  :project (some-> (param params "project") str)})
        "down" (cond
                 (and run-param (nil? id))
                 (isolated/down-run! ctx run-param (boolean (flag params "volumes"))
                                     {:agent (agent-param params) :allow-shared? (boolean allow-shared?)})
                 (flag params "volumes")
                 (r/err :compose/invalid-param {:param "volumes" :hint "volumes=true needs run=<id> alone (an isolated run)"})
                 :else (on-subject #(ops/down-profile! ctx % :down)))
        "stop" (on-subject #(ops/down-profile! ctx % :stop))
        "restart" (with-opts (fn [p] (let [recreate? (flag params "recreate")]
                                       (guard p :restart recreate?
                                              #(restart/restart-profile! ctx p recreate?)))))
        "touch" (if (or tgt id)
                  (on-subject #(do (isolated/cancel-due! ctx (:profile/id %))
                                   (ops/touch! ctx (:profile/id %))))
                  (ops/touch-all! ctx (some-> (param params "project") str)))
        "ps" (on-subject #(ops/ps-profile ctx %))
        "logs" (on-subject #(ops/logs-profile ctx % (or (some-> (param params "tail") long) 100)))
        "reap" (r/ok (tick! a))
        "adopt" (r/ok {:adopted (ops/adopt! ctx)})
        "projects" (ops/projects ctx)
        "flaky" (flaky/flaky ctx
                             {:suite (some-> (param params "suite") str)
                              :project (some-> (param params "project") str)}
                             (let [l (param params "limit")]
                               (max 1 (cond (number? l) (long l)
                                            (and (string? l) (re-matches #"\d+" l)) (Long/parseLong l)
                                            :else 20))))
        "faults" (r/bind (faults-request params) #(faults/faults! ctx %))
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
                     "section never stops a dependency this one needs; down/stop name what they kept and why. "
                     "restart recycles the named services in place (recreate=true recreates them) even when "
                     "another section needs them, but never recreates (restart recreate=true, or up with other "
                     "compose files, dir or env) a service another live section holds unless allow_shared=true; "
                     "the refusal names that section and the agents owning it. "
                     "up/switch take no_deps (start only the named services, e.g. past a failing migration), "
                     "wait (until healthy) and env. touch with no target resets every active section. "
                     "SWARM: an agent working on an app claims its section (claim target=<section, preset or services> note=..; "
                     "renew within the claim TTL, release when done; claiming an active section never restarts it) "
                     "and gets the shared nREPL per module to drive instead of "
                     "starting its own JVM; nrepl path=<file> answers which port drives that file. The reaper keeps "
                     "a section while it is claimed, agents work in its project or new nREPL connections reach it, "
                     "and stops it when nobody does: after its TTL, or the shorter orphan TTL once its last claim ended. "
                     "A WAVE claim (claim ttl_minutes=.. note=.., up to 8 h, owner may be a coordinator) holds until it ends or is released, even when its owner parks or leaves the swarm. "
                     "STACKS: stack-up stack=<id> runs a stack of capability consumers (shared deps once, opt-outs private, "
                     "each consumer on its own view network, health-gated); stack-down, stack-status, plan; claim target=stack:<run>. "
                     "ISOLATED RUNS: up isolated=true (or run=<id> to join one) starts a section as a private copy "
                     "of its compose project (-p <project>-<run>, override: no container_name, host ports per ports=, "
                     "images of the shared project pinned by id, fresh volumes); several sections of one run share one "
                     "project. Every compose call of a run gets HIVE_RUN, HIVE_RUN_PROJECT, HIVE_PORT_OFFSET and "
                     "HIVE_PORT_<SERVICE>_<PORT> (none of the latter under ports=ephemeral: unknown before up); a call env "
                     "naming one is refused. status answers runs with ports, images, config-digest and volumes. "
                     "down run=<id> ends a run keeping its volumes; volumes=true removes only the volumes that run created "
                     "(labelled hive.compose.run/hive.compose.base by its override), refused while another agent claims a "
                     "section of the run unless allow_shared. When the reaper reaps a run's last section it only MARKS the "
                     "volumes for removal after :compose/run-volumes-grace-minutes (default 60, status :stale-run-volumes "
                     ":due-at); up run=<id>, a claim or a touch on its sections in that time keeps the data. A run must own "
                     "its compose project: a project <project>-<run> that already exists is refused. A BIND mount is shared "
                     "with the host and with the shared project (status :binds); an external volume is refused unless named in "
                     ":profile/run-shared-volumes. :profile/run-shared-volumes borrows volumes of the shared "
                     "project: BUILD CACHES ONLY, never a volume holding state. up dry_run=true answers the memory admission. "
                     "SUITES: test suites=X,Y projects=A,B runs the suite x project matrix on one union stack and answers per-cell verdicts from exit code and JUnit/TAP; a failed matrix stays up for inspection. "
                     "command=help lists every command with its params.")
   :inputSchema {:type "object"
                 :properties {"command" {:type "string" :enum commands}
                              "target" {:type "string" :description "[up|switch|down|stop|restart|touch|ps|logs] services or native profiles: name, project/name, or a,b"}
                              "profile" {:type "string" :description "[up|switch|down|stop|restart|touch|ps|logs] preset or active section id"}
                              "no_deps" {:type "boolean" :description "[up|switch] start only the named services, not their depends_on (which must already run)"}
                              "wait" {:type "boolean" :description "[up|switch|stack-up|test] wait until the started services are healthy (stack-up and test: default true)"}
                              "recreate" {:type "boolean" :description "[restart] recreate the containers (up --force-recreate --no-deps) instead of restarting them in place"}
                              "allow_orphans" {:type "boolean" :description "[up|switch|restart] go ahead although processes a dead docker daemon left running still hold the section's volumes or ports (refused by default, naming them)"}
                              "allow_shared" {:type "boolean" :description "[up|switch|restart|down] recreate a service another live section holds (refused by default, naming the holder section and its agents); down run=<id> volumes=true: remove the run's volumes although another agent claims a section of it"}
                              "env" {:type "object" :description "[up|switch|restart] extra environment for compose interpolation, e.g. {\"HOST_GATEWAY_IP\": \"192.168.1.5\"}; a value {\"pass\": \"<store path>\"} is read from pass when the containers are created, and shown only as pass:<path>"}
                              "project" {:type "string" :description "[touch] with no target: only the sections of this project; [nrepl] only this project's routes; [flaky] only runs against this project"}
                              "suite" {:type "string" :description "[flaky] only cases of this suite"}
                              "limit" {:type "integer" :description "[flaky] how many ranked cases (default 20)"}
                              "agent" {:type "string" :description "[claim|release] the agent claiming; defaults to the caller's agent_id"}
                              "note" {:type "string" :description "[claim] what the agent works on (required with ttl_minutes)"}
                              "ttl_minutes" {:type "integer" :description "[claim] make it a WAVE claim: live for this many minutes (at most :compose/claim-max-ttl-minutes, default 480) whether or not the owner (an agent or a coordinator, agent=coordinator:..) stays in the swarm; claim again to move its end, release to end it now"}
                              "stop" {:type "boolean" :description "[release] stop the section now when no claim is left (what other sections need stays up)"}
                              "path" {:type "string" :description "[nrepl] a file or directory: answers the shared nREPL that drives it"}
                              "tail" {:type "integer" :description "[logs] lines per service (default 100)"}
                              "stack" {:type "string" :description "[stack-up|stack-down|stack-status|plan] stack id under :compose/stacks"}
                              "test" {:type "string" :description "[why] the failed test: a case id (orders.place_boom_order) or <suite>/<project>/<case>"}
                              "run" {:type "string" :description "[stack-up|stack-down|stack-status|plan|faults|why|test] the stack run id, ^[a-z0-9]{3,40}$ (stack-up default <stack><6 hex>, test default test<6 hex>); claim/release a run with target=stack:<run>. [up|down|stop|restart|touch|ps|logs|claim|release] an ISOLATED run: up joins (or starts) run <id> as compose project <project>-<run>; the others address the section <id>@<run>; down run=<id> alone takes the whole run down"}
                              "isolated" {:type "boolean" :description "[up] start the section in a new isolated run (a private copy of its compose project under -p <project>-<run>: no container_name, host ports per `ports`, fresh volumes of its own, the shared project's images pinned by id, never built); the answer names the run id"}
                              "ports" {:type ["string" "object"] :description "[up] host ports of an isolated run, fixed at its first up: \"strip\" (default, none), \"ephemeral\" (127.0.0.1, docker picks; no HIVE_PORT_<SVC>_<PORT> vars then), \"keep\", {\"offset\": n} (each published port p as p+n), {\"publish\": {svc {port host}}}. status answers the host port per service and container port"}
                              "shared_volumes" {:type "array" :description "[up] narrow the preset's :profile/run-shared-volumes: volumes an isolated run borrows from the shared project. BUILD CACHES ONLY (Go build/module cache, Maven repository), never a volume holding state; never removed with the run"}
                              "volumes" {:type "boolean" :description "[down] with run=<id> alone: also remove the volumes the isolated run created (default false: kept, listed as stale-run-volumes). The only volume removal hive-compose does; the reaper does it too when it reaps an isolated run"}
                              "suites" {:type "string" :description "[test] suite ids under :compose/suites, X,Y (default all)"}
                              "projects" {:type "string" :description "[test] projects under test, A,B (default every project a selected suite applies to)"}
                              "keep_up" {:type "boolean" :description "[test] keep the stack up even when every cell passed (a failed matrix always stays up)"}
                              "dry_run" {:type "boolean" :description "[test|up] answer the matrix, the stack plan and every runner spec; start nothing. [up] with isolated/run: plan only, no side effect: the override, ports, pinned images and the memory admission (need-mb, free-mb, verdict, and on refusal which sections would have to stop)"}
                              "variants" {:type "string" :description "[test] only these env variants of the suites that declare :suite/variants, a,b (default every variant); each selected suite with variants must declare every named one, else refused; a suite without variants runs once. The variant env reaches the suite RUNNER only, never the stack's consumers (no restart or re-env per variant)"}
                              "skip_fails" {:type "boolean" :description "[test] a skipped case fails its cell (the case stays skipped in the report); default each suite's :suite/skip-fails?, else false"}
                              "op" {:type "string" :enum ["add" "remove" "reset" "list"] :description "[faults] what to do (default list)"}
                              "proxy" {:type "string" :description "[faults] proxy name <consumer>.<capability>"}
                              "toxic" {:type "object" :description "[faults] {type, attributes, stream, toxicity, name}: type latency|timeout|bandwidth|reset_peer|slow_close|limit_data|slicer"}}
                 :required ["command"]}
   :handler (fn [params]
              (result->content (dispatch a (str (param params "command")) params)))})

(def ^:private subject-commands
  "Commands that resolve a section from target or profile (ops/subject)."
  #{"up" "switch" "down" "stop" "restart" "touch" "ps" "logs" "claim" "release"})

(def command-params
  "{command #{param}}: the params each command reads, the one source the
   control API rejects unknown params by and the spec's Req_<command>
   schemas are generated from. Read off the [cmd|..] prefix of each param's
   description in the tool's inputSchema, plus target and profile for the
   commands that resolve a section and agent_id (stamped by the MCP host)
   for claim and release."
  (let [props (dissoc (get-in (tool nil) [:inputSchema :properties]) "command")
        by-desc (apply merge-with into {}
                       (for [[p {:keys [description]}] props
                             [_ cs] (re-seq #"\[([a-z|_-]+)\]" (str description))
                             c (str/split cs #"\|")]
                         {c #{p}}))]
    (into (sorted-map)
          (for [c commands]
            [c (cond-> (get by-desc c #{})
                 (subject-commands c) (into #{"target" "profile"})
                 (#{"claim" "release"} c) (conj "agent_id"))]))))

;; ---------------------------------------------------------------------------
;; lifecycle

(defn- start-api!
  "Serve the control API when `:compose/api` enables it. The bound address
   (or why it failed: the addon still starts) lands in the state's :api, which
   `status` answers; the server itself under :api-server."
  [a cfg]
  (let [{:keys [enabled?] :as acfg} (api/config cfg)]
    (when enabled?
      (let [deps {:dispatch (fn [command params] (dispatch a command params))
                  :commands commands
                  :params command-params
                  :declared (get-in (tool a) [:inputSchema :properties])
                  :health (fn [] (addon/health a))
                  :spec api/spec-text}
            res (http/start! acfg #(api/route deps %))]
        (if (r/ok? res)
          (swap! (:state a) assoc :api-server (:ok res) :api (select-keys (:ok res) [:host :port :url]))
          ;; the keyword: MCP writes it bare, the API qualifies it, as every category
          (swap! (:state a) assoc :api {:error (:error res) :detail (dissoc res :error)}))))))

(defn- stop-api! [{:keys [state]}]
  (when-let [s (:api-server @state)] (http/stop! s))
  (swap! state dissoc :api-server :api))

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
    (start-api! a cfg)
    {:success? true :errors []
     :metadata (cond-> {:profiles (count (:compose/profiles settings))
                        :active (count (:active @(:state ctx)))
                        :tick-seconds tick}
                 (:api @state) (assoc :api (:api @state)))}))

(defn reload!
  "Re-read the addon config block declared now and the profiles file into the
   running context, keeping the active map; the engine and program runner are
   rebuilt from the new config. Answers Result<report>: :source (where the
   block came from), :profiles and :projects (ids now), :changed (projects,
   profiles and settings that differ, by id or key, never by value),
   :sections (active sections re-based on their changed project: :rebased
   ids, :kept with why) and :not-reloaded (what applies only at the next
   initialize!, and that running containers keep the env they were created
   with until recreated)."
  [{:keys [state seed]}]
  (locking state
    (let [{:keys [runtime-config declared ctx] old-cfg :cfg} @state
          {:keys [cfg source]} (merged-config seed runtime-config declared)
          api-changed? (not= (api/config old-cfg) (api/config cfg))
          old (:settings ctx)]
      (r/let-ok [settings (build-settings cfg)]
        (let [fresh (build-ctx cfg settings {})
              ctx' (assoc ctx :settings settings :engine (:engine fresh) :runner (:runner fresh))
              diff (config/settings-diff old settings)
              _ (swap! state assoc :cfg cfg :ctx ctx')
              sections (ops/rebase-sections! ctx' (:compose/projects old))]
          (r/ok (cond-> {:source source
                         :profiles (vec (sort (keys (:compose/profiles settings))))
                         :projects (vec (sort (keys (:compose/projects settings))))
                         :changed diff
                         :sections sections
                         :not-reloaded (cond-> ["env and files of running containers: they apply when the containers are recreated (up after the change, restart recreate=true)"]
                                         api-changed?
                                         (conj "the control API (:compose/api): it is rebound at the next initialize!")
                                         (seq (:restart-required diff))
                                         (conj (str "settings applied at the next initialize!: "
                                                    (str/join ", " (map name (:restart-required diff))))))}
                  (empty? diff) (assoc :note "the declared config matches what was running; nothing changed"))))))))

(defn- initialize-addon! [a seed runtime-config]
  (let [{:keys [state]} a]
    (locking state
      (if (= :active (:lifecycle @state))
        {:success? true :already-initialized? true}
        (let [{:keys [cfg declared]} (merged-config seed runtime-config {})
              settings (build-settings cfg)]
          (swap! state assoc :runtime-config runtime-config :declared declared)
          (if (r/err? settings)
            (do (swap! state assoc :lifecycle :error :errors [(pr-str settings)])
                {:success? false :errors [(str "invalid hive.compose config: " (pr-str settings))]})
            (try
              (start! a cfg (:ok settings))
              (catch Exception e
                (swap! state assoc :lifecycle :error :errors [(ex-message e)])
                {:success? false :errors [(ex-message e)]}))))))))

(defn- shutdown-addon! [{:keys [state] :as a}]
  (locking state
    (stop-api! a)
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
     :compose/tick! (fn [] (tick! this))
     :compose/claim! (fn [target agent opts]
                       (if-let [ctx (:ctx @state)]
                         (r/bind (ops/subject ctx {:target target}) #(swarm/claim! ctx % agent (or opts {})))
                         (r/err :compose/not-initialized)))
     :compose/release! (fn [target agent opts]
                         (if-let [ctx (:ctx @state)]
                           (r/bind (ops/subject ctx {:target target}) #(swarm/release-claim! ctx % agent (or opts {})))
                           (r/err :compose/not-initialized)))
     :compose/nrepl-for (fn [path]
                          (if-let [ctx (:ctx @state)]
                            (swarm/nrepl ctx {:path path})
                            (r/err :compose/not-initialized)))
     :compose/who (fn [] (if-let [ctx (:ctx @state)] (swarm/who ctx) (r/err :compose/not-initialized)))
     ;; hc-8's suite runner records each run result here (see pipeline.flaky).
     :compose/record-run! (fn [run]
                            (if-let [ctx (:ctx @state)]
                              (flaky/record-run! ctx run)
                              (r/err :compose/not-initialized)))}))

(defn make-addon
  "Uninitialized IAddon. No thread, file or container is touched."
  ([] (make-addon {}))
  ([seed] (->HiveComposeAddon (atom {:lifecycle :new :busy (AtomicBoolean. false)})
                              (or seed {}))))

(defn addon-ctor
  "hive-addon.mount constructor: config -> uninitialized IAddon."
  [config]
  (make-addon config))
