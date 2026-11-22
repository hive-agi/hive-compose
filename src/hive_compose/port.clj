(ns hive-compose.port
  "The compose engine port. Every method answers a Result.")

;; SPDX-License-Identifier: MIT

(defprotocol IComposeEngine
  (-version [e]
    "Result<string>: the engine's version, or why it is unavailable.")
  (-closure [e profile]
    "Result<[service]>: the services `profile` needs, its depends_on closure included.")
  (-listed [e profile]
    "Result<[service]>: services enabled for `profile`: the always-on ones plus those of its native compose profiles.")
  (-profiles [e profile]
    "Result<#{name}>: native compose profiles declared by the profile's project.")
  (-running [e profile]
    "Result<#{service}>: services of the profile's project whose containers are running.")
  (-projects [e]
    "Result<[{:name :status :config-files}]>: every compose project on the host.")
  (-up! [e profile services]
    "Result<{:out}>: start `services` detached. An empty `services` is a no-op.")
  (-stop! [e profile services]
    "Result<{:out}>: stop `services`, keeping containers and volumes. Empty is a no-op.")
  (-down! [e profile services]
    "Result<{:out}>: remove the containers of `services`, never volumes. Empty is a no-op.")
  (-restart! [e profile services recreate?]
    "Result<{:out}>: restart the containers of `services` in place, or, when
     `recreate?`, recreate them (never their depends_on, never volumes). Empty is a no-op.")
  (-logs [e profile services tail]
    "Result<{:out}>: the last `tail` log lines of `services` (all when empty)."))

(defprotocol IDependencyGraph
  "Optional engine capability: the depends_on edges of a project, so `up` can
   leave out what the compose file marks `required: false`. An engine without
   it closes over every depends_on entry, as before."
  (-dependencies [e profile]
    "Result<{service {dep {:required? bool}}}>: the depends_on edges of the services the profile's project defines."))

(defprotocol IContainerControl
  "Optional engine capability: release a project's containers by name, found
   by their compose labels, without loading its compose files. What an
   adopted section runs may come from a file its project does not name, and
   a file that needs a variable nobody set cannot be loaded at all; neither
   keeps a release by name from running."
  (-stop-containers! [e profile services action]
    "Result<{:out :containers [name]}>: stop (`action` :stop) or remove
     (:down, never volumes) the containers of `services` in the profile's
     compose project, addressed by container name. A service with no
     container is skipped. Empty `services` is a no-op."))

(defprotocol IPublishedPorts
  "Optional engine capability: the host ports the running containers of a
   project publish, so connections to them can count as use."
  (-published [e profile]
    "Result<{service #{port}}>: the host ports published by the running containers of the profile's project."))

(defprotocol IContainerAddresses
  "Optional engine capability: the addresses the running containers of a
   project hold on their networks, so the connections they open to the
   project's own published ports are not taken for use of it."
  (-addresses [e profile]
    "Result<#{ip}>: the network addresses of the running containers of the profile's project."))

(defprotocol IContainerLoad
  "Optional engine capability: how busy the running containers of a project
   are, so a section working with no client connected (a batch job, a queue
   worker) can count as use."
  (-cpu [e profile]
    "Result<{service percent}>: the CPU use, in percent of one core, of each
     running service of the profile's project (the busiest of its containers)."))

(defprotocol ISwarm
  "The agents working beside the addon and the channel that reaches them."
  (-agents [s]
    "Result<[{:agent/id :agent/status :agent/cwd}]>: the live agents of the
     swarm, or an error when no swarm answers (no host).")
  (-notify! [s event data]
    "Result<{:channel :direct|:broadcast ..}>: tell the swarm about `event` (a
     keyword) with `data` ({:message string ..}). With `:to` (an agent id) in
     `data` the notice is meant for that agent alone: :direct when it went to
     it, :broadcast (with :fallback? true) when no directed channel resolved.
     A notice that cannot be delivered is an error, never a throw."))

(defprotocol IActivity
  "Who talks to a local port right now."
  (-peers [a ports]
    "{port #{peer}}: the remote endpoints (\"addr:port\") of the TCP
     connections accepted on each of local `ports`, listening sockets left
     out. A port nobody is connected to is absent."))

(defprotocol IProgramRunner
  "Host programs: the processes a section runs outside compose."
  (-facts [r dir]
    "Result<{file-name parsed}>: the build files of `dir` (shadow-cljs.edn, deps.edn, bb.edn as EDN; project.clj as true).")
  (-start! [r program log]
    "Result<{:pid}>: start resolved `program` detached, leading its own process group, output appended to `log`.")
  (-alive? [r pid]
    "True while the process `pid` lives.")
  (-listening? [r port]
    "True when something accepts connections on local `port`.")
  (-nrepl-port [r dir]
    "The port in `dir`'s .nrepl-port file, nil when absent.")
  (-halt! [r pid]
    "Result<{:pid :stopped?}>: terminate the process group `pid` leads. Halting a dead pid succeeds."))

(defprotocol IProgramOutcome
  "Optional runner capability: what a started program left behind once its
   process is gone, so a program that dies right after start can say why. A
   runner without it reports the death alone."
  (-exit-code [r pid]
    "The exit code of `pid`, started by this runner and since exited; nil while
     it lives or when this runner did not start it (a pid kept across a restart).")
  (-log-tail [r log n]
    "[line]: the last `n` non-blank lines of the program log `log`, [] when it is absent."))

(defprotocol IProcessTable
  "The host's processes, as a value: what a dead rootless dockerd left
   running is read from it (see `hive-compose.promote.orphan`)."
  (-snapshot [t]
    "Result<Snapshot>: {:manager-pid :subuid :procs}, see
     `hive-compose.promote.orphan`. An error when the table cannot be read
     (another OS, no /proc), never a throw."))

(defprotocol ITraceQuery
  "The traces a stack's services exported, read back from the trace store
   (Tempo, ClickHouse ..), so a failed test can be explained by the spans
   that carry its test.id. See `hive-compose.promote.trace` for the data."
  (-find-traces [q query]
    "Result<[{:trace/id :trace/root-service :trace/root-name :trace/start-ms
     :trace/duration-ms}]>: the traces with a span matching every entry of
     `(:attrs query)` ({\"hive.run.id\" \"r1\" \"test.id\" \"t\"}) that started
     between `:start-ms` and `:end-ms` (epoch ms, both optional), at most
     `:limit` of them. None found is an empty vector, not an error.")
  (-trace [q trace-id]
    "Result<{:trace/id :trace/spans [{:span/id :span/parent-id :span/name
     :span/service :span/start-ms :span/duration-ms :span/status
     :span/status-message :span/attrs}]}>: every span of the trace `trace-id`
     (hex). :span/status is :ok, :error or :unset. A trace the store does not
     hold is :trace/not-found.")
  (-health [q]
    "Result<{:ready? true}>: the trace store answers, or why it does not."))

(defprotocol IHostMemory
  "The host's memory, so a stack plan can be admitted against what is free
   and the reaper can relieve pressure (hc-16a). Stubbed in tests."
  (-mem-available [m]
    "Result<long>: MemAvailable in bytes (Linux /proc/meminfo), or why it
     cannot be read. Never a throw."))

(defprotocol IContainerOwnership
  "Optional engine capability: which containers the daemon runs and what a
   section's services hold, so a process can be told apart from the
   containers that own it, and its holdings matched to a section."
  (-live-containers [e]
    "Result<#{id}>: full ids of every container the daemon reports running.")
  (-resources [e profile services]
    "Result<{:volumes #{name} :ports #{port}}>: the docker volumes (their real
     names) mounted by `services` of the profile's project and the host ports
     they publish."))

(defprotocol ISuiteRunner
  "Runs one suite runner to its end under its resource caps (hc-16b): a host
   command inside a memory-capped systemd user scope (ulimit -v when no user
   manager answers), a container with docker's memory and cpu flags. See
   `hive-compose.promote.oom` for the data."
  (-run! [r spec]
    "Result<{:exit n-or-nil :started-at ms :finished-at ms :output [line]
     :timed-out? bool :cap {:mode :systemd|:ulimit|:docker|:none :mem bytes?
     :cpus n? :note s?} :oom Finding?}>: run `spec` (a runner spec, see
     `hive-compose.promote.suite/runner-spec`: :runner :command|:container,
     :command, :dir, :env, :timeout-ms, :test-id, :image, :name, :mounts,
     :network, :labels, plus :resources {:mem bytes :cpus n}) and wait for its
     end. A runner that exits non-zero, is OOM-killed or times out is an ok
     answer; an error only when it could not be started at all."))

(defprotocol IStackEngine
  "Optional engine capability: what a stack run needs beyond sections. View
   networks (one per consumer and run), compose projects brought up with
   labels and networks a stack adds, and what runs under given labels.

   A ProjectSpec is {:project name :dir path :files [path] :services [svc]
   :env {VAR EnvValue} :container-env [VAR] :labels {k v} :networks [name]
   :wait? bool :wait-timeout-s n :all? bool}. `:env` is the environment of
   the compose process (interpolation; its secret references read only when
   containers are created); `:container-env` names the variables of `:env`
   passed into every service of `:services`; `:labels` land on every
   container of `:services`; with `:networks` those services attach to these
   external networks instead of the project's default one (so Docker DNS on
   them is never ambiguous; list every service that must reach another). An engine applies labels, networks and
   container-env through a generated compose override (compose has no label
   flag); a spec with none of them runs the project's own files untouched."
  (-network-create! [e network labels]
    "Result<{:network :created? bool}>: create bridge network `network` with
     `labels` ({k v}). Idempotent: an existing network answers :created? false.")
  (-network-rm! [e network]
    "Result<{:network :removed? bool :disconnected [container]}>: disconnect
     every container still attached to `network`, then remove it. An absent
     network answers :removed? false.")
  (-network-connect! [e network target aliases]
    "Result<{:network :containers [name]}>: connect every container of
     `target` ({:project :service}) to `network` under `aliases`. Idempotent: a
     container already attached counts as connected.")
  (-up-project! [e spec]
    "Result<{:out}>: `compose up -d` (`--wait` when :wait?) of ProjectSpec
     `spec`. Empty `:services` is a no-op.")
  (-down-project! [e spec]
    "Result<{:out}>: remove the containers of `spec`'s `:services`, or with
     `:all?` the whole compose project (its containers and own networks).
     Never volumes.")
  (-running-labelled [e labels]
    "Result<[{:name :project :service :labels {k v} :state :health}]>: every
     container, running or not, carrying all `labels` ({k v}). :state is the
     docker state string (\"running\", \"exited\"), :health one of :healthy
     :unhealthy :starting :none.")
  (-networks-labelled [e labels]
    "Result<[{:name :labels {k v}}]>: every network carrying all `labels`."))

(defprotocol IDataExec
  "Optional engine capability: run a data command (migrate, seed, reset)
   inside a running container of a stack instance (hc-9). Never removes a
   container or a volume."
  (-exec! [e target argv opts]
    "Result<{:exit 0 :out}>: `docker exec` of `argv` in the running container of
     `target` ({:project compose-project :service svc}), with `opts` {:env
     {VAR EnvValue} :timeout-ms n}; secret references in :env are read only
     now. A non-zero exit is :compose/command-failed carrying :exit and
     :stderr; no running container is :compose/no-container."))

(defprotocol ICommandRunner
  "Optional runner capability: run a host command to completion (hc-9 data
   commands declared as `:command`), unlike `IProgramRunner/-start!`, which
   detaches a long-lived program."
  (-run-command! [r argv opts]
    "Result<{:exit 0 :out}>: run `argv` with `opts` {:dir path :env {VAR
     EnvValue} :timeout-ms n} and wait for it; secret references in :env are
     read only now. A non-zero exit is :compose/command-failed carrying :exit
     and :stderr."))

(defprotocol IToxiproxyClient
  "A Toxiproxy server's REST API (hc-12 fault injection). A proxy is
   {:name :listen :upstream :enabled :toxics [toxic]}, a toxic {\"name\"
   \"type\" \"stream\" \"toxicity\" \"attributes\"} as toxiproxy writes it.
   Every method answers a Result; an unreachable server is an error, never a
   throw."
  (-create-proxy! [c proxy]
    "Result<Proxy>: create `proxy` ({\"name\" \"listen\" \"upstream\"
     \"enabled\"}). Idempotent: an existing proxy of that name with the same
     listen and upstream answers itself; a different one is an error.")
  (-add-toxic! [c proxy-name toxic]
    "Result<toxic>: add `toxic` to proxy `proxy-name`, replacing a toxic of
     the same name.")
  (-remove-toxic! [c proxy-name toxic-name]
    "Result<{:removed? bool}>: remove toxic `toxic-name`; an absent one
     answers :removed? false.")
  (-reset! [c]
    "Result<{:reset true}>: remove every toxic and enable every proxy.")
  (-list [c]
    "Result<{name Proxy}>: every proxy with its toxics."))
