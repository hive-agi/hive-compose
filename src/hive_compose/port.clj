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

(defprotocol IPublishedPorts
  "Optional engine capability: the host ports the running containers of a
   project publish, so connections to them can count as use."
  (-published [e profile]
    "Result<{service #{port}}>: the host ports published by the running containers of the profile's project."))

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
