(ns hive-compose.schema
  "Value objects: Profile, Active entry, Settings, Release step, CaseOutcome."
  (:require [malli.core :as m]
            [malli.error :as me]))

;; SPDX-License-Identifier: MIT

(def ProfileId
  "A preset name, or a section id `<project>/<target>[+<target>..]`."
  [:re #"^[a-zA-Z0-9][a-zA-Z0-9_.+/-]*$"])

(def IdleAction
  "What the reaper does to an idle profile. :stop keeps containers, :down removes
   them (never volumes), :none exempts the profile."
  [:enum :stop :down :none])

(def SecretRef
  "An env value read from a secret store when a process is spawned, never
   before, and shown everywhere else as `<scheme>:<path>`. One entry, its
   scheme: `{:pass \"<store path>\"}` is the first line of `pass <path>`. A new
   scheme is one more map here and a backend of that id in
   `hive-compose.adapter.secrets`."
  [:map {:closed true} [:pass [:string {:min 1}]]])

(def EnvValue
  "A literal string, or a SecretRef."
  [:or :string SecretRef])

(def Env
  [:map-of :string EnvValue])

(def Program
  "A host process a section needs beside its containers. `:program/with` pairs
   it with compose services: it starts with a section running one of them, or
   with every section of its project when it names none. A directory with a
   Clojure build file needs no `:program/command`: the command and the nREPL
   port are derived from it (see `hive-compose.promote.program`)."
  [:map
   [:program/id [:re #"^[a-zA-Z0-9][a-zA-Z0-9_.-]*$"]]
   [:program/dir [:string {:min 1}]]
   [:program/command {:optional true} [:or [:string {:min 1}] [:vector {:min 1} [:string {:min 1}]]]]
   [:program/kind {:optional true} [:enum :shadow :deps :lein :bb :plain]]
   [:program/nrepl-port {:optional true} [:int {:min 1 :max 65535}]]
   [:program/with {:optional true} [:vector [:string {:min 1}]]]
   [:program/env {:optional true} Env]
   [:program/description {:optional true} :string]])

(def Running
  "A started Program: the pid leading its process group, where its output goes
   and, for a Clojure kind, the nREPL port to connect to."
  [:map
   [:program/id :string]
   [:pid :int]
   [:log :string]
   [:dir :string]
   [:started-at :int]
   [:kind {:optional true} :keyword]
   [:nrepl-port {:optional true} :int]])

(def Activity
  "What counts as use of a section: new connections to :nrepl (the shared
   nREPLs of its programs) and :published (the host ports its containers
   publish, a browser or any host-side client), and :cpu (a running service
   above :compose/cpu-busy-percent, busy with no client connected)."
  [:set [:enum :nrepl :published :cpu]])

(def Profile
  [:map
   [:profile/id ProfileId]
   [:profile/dir [:string {:min 1}]]
   [:profile/files {:optional true} [:vector [:string {:min 1}]]]
   [:profile/project {:optional true} [:re #"^[a-z0-9][a-z0-9_-]*$"]]
   [:profile/compose-profiles {:optional true} [:vector ProfileId]]
   [:profile/services {:optional true} [:vector [:string {:min 1}]]]
   [:profile/programs {:optional true} [:vector Program]]
   [:profile/env {:optional true} Env]
   [:profile/wait? {:optional true} :boolean]
   [:profile/no-deps? {:optional true} :boolean]
   [:profile/ttl-minutes {:optional true} [:int {:min 1}]]
   [:profile/idle-action {:optional true} IdleAction]
   [:profile/activity {:optional true} Activity]
   [:profile/description {:optional true} :string]])

(def Claim
  "An agent's declaration that it works on a section: since when, when it was
   last renewed, what for, and whether the agent was in the swarm when it
   claimed (then the claim ends with the agent)."
  [:map
   [:since :int]
   [:seen :int]
   [:note {:optional true} :string]
   [:swarm? {:optional true} :boolean]])

(def Usage
  "What the last reaper pass saw using a section. A reason is {:claim agent},
   {:agent id}, {:port p :peers n :source :nrepl|:published} or {:service s
   :cpu percent :source :cpu}; :sources names once each what the reasons came
   from (:claim :agent :nrepl :published :cpu)."
  [:map
   [:at :int]
   [:in-use? :boolean]
   [:by [:vector :map]]
   [:sources {:optional true} [:vector :keyword]]])

(def Active
  "A profile this addon brought up (or adopted) and still answers for. `:profile`
   is the Profile it ran with, so it can be released after leaving the config.
   `:needs` is the depends_on closure of `:services` when it is wider (a section
   started with no-deps, an adopted one); release spares what any other entry
   needs. `:programs` are the host programs it holds, by program id."
  [:map
   [:profile/id ProfileId]
   [:project :string]
   [:services [:vector :string]]
   [:needs {:optional true} [:vector :string]]
   [:started-at :int]
   [:last-touch :int]
   [:programs {:optional true} [:map-of :string Running]]
   [:profile {:optional true} Profile]
   [:claims {:optional true} [:map-of :string Claim]]
   [:orphaned-at {:optional true} :int]
   [:usage {:optional true} Usage]])

(def ActiveMap [:map-of :string Active])

(def Project
  "Where a compose project lives. Its YAML is the source of truth: sections are
   resolved from its services, native profiles and depends_on. `:project/programs`
   are the host programs its sections start beside their containers."
  [:map
   [:project/id [:re #"^[a-zA-Z0-9][a-zA-Z0-9_.-]*$"]]
   [:project/dir [:string {:min 1}]]
   [:project/files {:optional true} [:vector [:string {:min 1}]]]
   [:project/name {:optional true} [:re #"^[a-z0-9][a-z0-9_-]*$"]]
   [:project/programs {:optional true} [:vector Program]]
   [:project/env {:optional true} Env]
   [:project/wait? {:optional true} :boolean]
   [:project/ttl-minutes {:optional true} [:int {:min 1}]]
   [:project/idle-action {:optional true} IdleAction]
   [:project/activity {:optional true} Activity]])

(def CaseStatus
  "How one test case ended in one run, as JUnit XML and TAP report it."
  [:enum :passed :failed :error :skipped])

(def RunIdLocal
  "A run id: hc-2's RunId rule (PR #37), copied so flake history does not
   depend on that branch.
   TODO(hc-2): once #37 lands, replace this with schema/RunId."
  [:re #"^[a-z0-9]{3,40}\z"])

(def CaseOutcome
  "One row of flake history: how case `:case/id` of suite `:suite/id` ended in
   run `:run/id` against `:project`, recorded at `:at` (epoch ms). `:run/sha`,
   when the runner knows the commit under test, lets a regression name the
   first failing commit.

   A minimal local shape of what hc-8's run result carries (its Report cases,
   ids `<suite>/<classname>.<name>`), so this layer can be reconciled with it
   later without depending on it."
  [:map
   [:run/id RunIdLocal]
   [:suite/id [:string {:min 1}]]
   [:project {:optional true} [:string {:min 1}]]
   [:case/id [:string {:min 1}]]
   [:case/status CaseStatus]
   [:case/time-s {:optional true} [:maybe number?]]
   [:at :int]
   [:run/sha {:optional true} [:string {:min 1}]]])

(def FlakyConfig
  "Thresholds of `hive-compose.promote.flaky`, every key optional (defaults in
   `flaky/defaults`). `:window` is how many of a case's latest runs are
   judged, `:min-runs` how many it needs before it is called anything but
   :stable, `:flip-rate` the share of status changes between consecutive runs
   of the window at or above which it is :flaky, and `:streak` how many
   consecutive fails (passes) closing the window make a :regression (a :fixed)."
  [:map
   [:window {:optional true} [:int {:min 2}]]
   [:min-runs {:optional true} [:int {:min 1}]]
   [:flip-rate {:optional true} [:and number? [:>= 0] [:<= 1]]]
   [:streak {:optional true} [:int {:min 1}]]])

(def Settings
  [:map
   [:compose/projects [:map-of :string Project]]
   [:compose/profiles [:map-of :string Profile]]
   [:compose/tick-seconds [:int {:min 5}]]
   [:compose/default-ttl-minutes [:int {:min 1}]]
   [:compose/default-idle-action IdleAction]
   [:compose/state-file [:string {:min 1}]]
   [:compose/log-dir [:string {:min 1}]]
   [:compose/timeout-ms [:int {:min 1000}]]
   [:compose/up-timeout-ms [:int {:min 1000}]]
   [:compose/adopt? :boolean]
   [:compose/programs? :boolean]
   [:compose/swarm? :boolean]
   [:compose/notify? :boolean]
   [:compose/activity Activity]
   [:compose/cpu-busy-percent {:optional true} [:and number? [:>= 0]]]
   [:compose/claim-ttl-minutes [:int {:min 1}]]
   [:compose/orphan-ttl-minutes [:int {:min 1}]]
   [:compose/program-grace-ms {:optional true} [:int {:min 0}]]
   [:compose/history-file {:optional true} [:string {:min 1}]]
   [:compose/flaky {:optional true} FlakyConfig]])

(def Release
  "One step of a plan: act on `services` of `project` on behalf of a profile."
  [:map
   [:profile/id ProfileId]
   [:project :string]
   [:action [:enum :stop :down]]
   [:services [:vector :string]]])

(defn explain
  "nil when `v` conforms to `schema`, else humanized problems."
  [schema v]
  (some-> (m/explain schema v) me/humanize))
