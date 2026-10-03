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
  (-logs [e profile services tail]
    "Result<{:out}>: the last `tail` log lines of `services` (all when empty)."))

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
