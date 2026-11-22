(ns hive-compose.schema
  "Value objects: Profile, Active entry, Settings, Release step; and for
   stacks Provide, Need, Stack, Instance, Resolution, Wiring, Suite, RunId with
   the pure Docker name builders slug, view-network and private-project."
  (:require [clojure.string :as str]
            [malli.core :as m]
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

;; ---------------------------------------------------------------------------
;; Stacks: capabilities a project provides and needs, the instances a stack
;; resolves them to, and how each consumer is wired to its instances.
;; Decision 20261122141106-2d375c4f. Every new key is optional on Project and
;; Settings: a config without them is valid and behaves as before.
;; Patterns anchor with \z, not $, so a trailing newline is refused.

(def CapabilityId
  "A capability name: what a consumer needs and a provider offers, e.g.
   \"kafka\", \"postgres\", \"otel\". Never a concrete service of another project."
  [:re #"^[a-z0-9][a-z0-9_.-]*\z"])

(def ^:private version-pattern
  "`<major>[.<minor>[.<patch>]]` with an optional semver pre-release/build or
   image suffix."
  "[0-9]+(?:\\.[0-9]+){0,2}(?:[-+][0-9A-Za-z.+-]+)?")

(def Version
  "A provider's concrete version, `<major>[.<minor>[.<patch>]]` with an
   optional suffix: \"3.7\", \"16\", \"3.7.1-alpine\", \"1.0.0-rc.1+build.5\".
   The resolver orders by the numeric part, and a `-` suffix below the bare
   release as semver orders pre-releases; `+` build suffixes do not order."
  [:re (re-pattern (str "^" version-pattern "\\z"))])

(def VersionRange
  "What versions a consumer accepts: \"*\" (any), an exact or prefix version
   \"3\" / \"3.7\", caret \"^3\" / \"^3.7\" (same major), tilde \"~3\" (same
   major) / \"~3.7\" (same minor), or space-separated comparators \">=3.5 <4\".
   Every version in it may carry a Version suffix. Interpreted only by the
   pure resolver (hive-compose.promote.capability)."
  (let [cmp (str "(?:>=|<=|>|<|=)" version-pattern)]
    [:re (re-pattern (str "^(?:\\*|[~^]?" version-pattern "|" cmp "(?: +" cmp ")*)\\z"))]))

(def RunId
  "Names one stack run: 3-40 lowercase letters and digits, no `-`, so it is
   the unambiguous delimiter in the names built from it (`view-network`,
   `private-project`), and fits the docker label `hive.run`. Telemetry carries
   it as the OTLP resource attribute `hive.run.id`; services see it as
   HIVE_RUN_ID."
  [:re #"^[a-z0-9]{3,40}\z"])

(def ConsumerId
  "Who needs capabilities: a project id, a section id `<project>/<target>`,
   or a suite `suite:<id>`. Docker names use its `slug`."
  [:re #"^[a-zA-Z0-9][a-zA-Z0-9_.+/:-]*\z"])

(defn slug
  "`s` as a Docker name part: lower-cased, every char outside [a-z0-9-] made
   `-`, leading and trailing `-` dropped. Total but lossy (\"billing/api\" and
   \"billing-api\" share a slug), so the stack plan refuses a run in which two
   consumers, or two private instances, share a name."
  [s]
  (-> (str s)
      str/lower-case
      (str/replace #"[^a-z0-9-]" "-")
      (str/replace #"^-+|-+\z" "")))

(defn view-network
  "The view network of `consumer` in `run`: `hive-<run>-<slug consumer>`.
   RunId holds no `-`, so two runs never share a view network."
  [run consumer]
  (str "hive-" run "-" (slug consumer)))

(defn private-project
  "The compose project a private instance runs under:
   `<provider-project>-<slug consumer>-<run>`, `provider-project` being the
   provider's own compose project name. RunId holds no `-`, so two runs never
   share a private project; within a run the stack plan refuses collisions."
  [provider-project consumer run]
  (str provider-project "-" (slug consumer) "-" run))

;; ---------------------------------------------------------------------------
;; Resource caps (hc-16a). A cap bounds one instance's containers (compose
;; mem_limit / cpus); the stack plan sums the capped memory of its instances
;; and refuses a run over a budget or over the host's MemAvailable minus
;; :compose/host-reserve. Precedence, high to low, per key: the stack's
;; :stack/resources for the capability, the consumers' Need :resources (a
;; shared instance takes the largest), the Provide's :resources,
;; :compose/default-resources. :unbounded at the winning level opts out: no
;; cap is written and the instance is left out of the sum (listed under
;; :budget/uncapped). Interpreted only by hive-compose.promote.resources.

(def MemSize
  "Memory as bytes (a positive int) or docker's notation: digits with an
   optional k|m|g|t unit (binary, case-insensitive, optional trailing b):
   \"512m\", \"2g\", \"1536M\", \"64kb\"."
  [:or [:int {:min 1}] [:re #"^[0-9]+(?:[kKmMgGtT][bB]?|[bB])?\z"]])

(def Caps
  "Caps on one instance's containers. `:mem` its compose mem_limit, `:cpus`
   its compose cpus (fraction of cores), `:jvm?` true injects
   JAVA_TOOL_OPTIONS=-XX:MaxRAMPercentage=75 (merged per flag, the consumer's
   own flags win); a java image (openjdk, temurin, corretto, clojure ..) is
   detected without it, and `:jvm? false` turns the detection off."
  [:map {:closed true}
   [:mem {:optional true} MemSize]
   [:cpus {:optional true} [:and number? [:> 0]]]
   [:jvm? {:optional true} :boolean]])

(def Resources
  "Caps, or :unbounded (explicit opt-out: no cap, not summed)."
  [:or [:= :unbounded] Caps])

(def Provide
  "One capability a project offers. `:service` is the compose service that
   answers it (its depends_on closure comes along). `:env` is what a consumer
   wired to it receives, written with the service's own hostname: instances
   join each consumer's view network under their original service names, so
   `kafka:9092` stays right for shared and private instances alike.
   `:tenancy` names the env var a consumer's namespace prefix (topics,
   schemas, buckets) is passed in, so consumers of a shared instance do not
   cross-talk."
  [:map
   [:service [:string {:min 1}]]
   [:version {:optional true} Version]
   [:env {:optional true} Env]
   [:tenancy {:optional true} [:map [:env [:string {:min 1}]]]]
   [:resources {:optional true} Resources]
   [:description {:optional true} :string]])

(def Need
  "One capability a consumer requires. `:version` defaults to \"*\". `:share?`
   defaults to true: compatible needs of every consumer in a stack share one
   instance; false gives this consumer a private instance nobody else uses.
   `:provider` pins a project id when several provide the capability.
   `:services` narrows which of the consumer's services get the wiring (all
   by default). `:via` interposes a capability between consumer and instance
   (e.g. :toxiproxy, hc-12); config gives it as a string or keyword.
   `:resources` overrides the Provide's caps on the instance answering it; a
   shared instance takes, per key, the largest cap its consumers would have
   had alone (a consumer with no cap has no opinion); one consumer's explicit
   :unbounded unbounds it."
  [:map
   [:version {:optional true} VersionRange]
   [:share? {:optional true} :boolean]
   [:provider {:optional true} [:re #"^[a-zA-Z0-9][a-zA-Z0-9_.-]*\z"]]
   [:services {:optional true} [:vector [:string {:min 1}]]]
   [:via {:optional true} :keyword]
   [:resources {:optional true} Resources]])

(def Provides [:map-of CapabilityId Provide])

(def Needs [:map-of CapabilityId Need])

(def Stack
  "A named set of consumers brought up together with every capability they
   need. `:stack/needs` adds needs of the stack itself (e.g. otel).
   `:stack/resources` overrides caps per capability for this stack, above
   every other level. `:stack/budget` bounds this stack's capped memory
   (besides :compose/budget). `:stack/priority` orders the reaper's pressure
   guard: under memory pressure the idle unclaimed stack with the LOWEST
   priority is stopped first (default 0)."
  [:map
   [:stack/id [:re #"^[a-zA-Z0-9][a-zA-Z0-9_.-]*\z"]]
   [:stack/consumers [:vector {:min 1} ConsumerId]]
   [:stack/needs {:optional true} Needs]
   [:stack/resources {:optional true} [:map-of CapabilityId Resources]]
   [:stack/budget {:optional true} MemSize]
   [:stack/priority {:optional true} :int]
   [:stack/description {:optional true} :string]])

(def ComposeProjectName
  "A docker compose project name."
  [:re #"^[a-z0-9][a-z0-9_-]*\z"])

(defn- distinct-items? [xs] (or (empty? xs) (apply distinct? xs)))

(def Instance
  "One running copy of a capability, as resolved for a stack. A shared
   instance (`:instance/share? true`) serves every consumer listed and runs
   under the provider's own compose project name (as `up` of that project
   runs it, derived from its dir, not its id); a private one has exactly one
   consumer and runs under `private-project`. `:instance/compose-project` is
   filled by the stack plan and required for a private instance. It joins
   each of its consumers' view networks under `:instance/aliases`, its
   original service names. Ids: `<provider>/<capability>` when shared,
   `<provider>/<capability>@<consumer>` when private."
  [:and
   [:map
    [:instance/id [:string {:min 1}]]
    [:instance/capability CapabilityId]
    [:instance/provider [:string {:min 1}]]
    [:instance/service [:string {:min 1}]]
    [:instance/version {:optional true} Version]
    [:instance/share? :boolean]
    [:instance/consumers [:vector {:min 1} ConsumerId]]
    [:instance/aliases [:vector {:min 1} [:string {:min 1}]]]
    [:instance/compose-project {:optional true} ComposeProjectName]]
   [:fn {:error/message "a private instance has exactly one consumer and a compose project"}
    (fn [i] (or (:instance/share? i)
                (and (= 1 (count (:instance/consumers i)))
                     (contains? i :instance/compose-project))))]
   [:fn {:error/message "consumers and aliases are each distinct"}
    (fn [i] (and (distinct-items? (:instance/consumers i))
                 (distinct-items? (:instance/aliases i))))]])

(def Resolution
  "What the pure resolver answers: the instances by id, and for every
   consumer which instance answers each capability it needs."
  [:map
   [:resolution/instances [:map-of :string Instance]]
   [:resolution/bindings [:map-of ConsumerId [:map-of CapabilityId :string]]]])

(def Wiring
  "How one consumer is connected in a run. Its view network
   `view-network` is the only network outside its own compose project it
   attaches to, so Docker DNS is never ambiguous: its own services still
   reach each other on their project's networks, and every instance in
   `:wiring/attach` joins the view network under its aliases, distinct across
   the attach list. `:wiring/services` names which of the consumer's services
   attach and get the env (all when absent, as Need `:services`); the stack
   plan refuses an alias that equals one of those services' names. The env is
   the providers' `:env`, HIVE_RUN_ID, tenancy prefixes and OTLP vars;
   `:wiring/prefix` is the consumer's namespace prefix. Secret references
   stay references until spawn."
  [:and
   [:map
    [:wiring/consumer ConsumerId]
    [:wiring/run RunId]
    [:wiring/network [:re #"^hive-[a-z0-9]{3,40}-[a-z0-9][a-z0-9-]*\z"]]
    [:wiring/services {:optional true} [:vector {:min 1} [:string {:min 1}]]]
    [:wiring/attach [:vector [:map
                              [:instance [:string {:min 1}]]
                              [:aliases [:vector {:min 1} [:string {:min 1}]]]]]]
    [:wiring/env Env]
    [:wiring/prefix [:string {:min 1}]]]
   [:fn {:error/message "the network is the view network of this consumer in this run"}
    (fn [w] (= (:wiring/network w) (view-network (:wiring/run w) (:wiring/consumer w))))]
   [:fn {:error/message "instances and their aliases are distinct across the attach list"}
    (fn [w] (let [att (:wiring/attach w)]
              (and (distinct-items? (map :instance att))
                   (distinct-items? (mapcat :aliases att)))))]])

(def ReportFormat
  "How a suite reports test cases: JUnit XML, TAP, or :auto (by extension)."
  [:enum :junit :tap :auto])

(def Suite
  "A test runner in any language: a container `:suite/image` (optional
   `:suite/command` as its args) or a host `:suite/command`. It needs
   capabilities itself (`:suite/needs`) and runs against `:suite/projects`.
   It writes reports into `:suite/reports-dir` (inside the container for an
   image, else relative to `:suite/dir`); its exit code is the verdict when
   no report is found."
  [:and
   [:map
    [:suite/id [:re #"^[a-zA-Z0-9][a-zA-Z0-9_.-]*\z"]]
    [:suite/image {:optional true} [:string {:min 1}]]
    [:suite/command {:optional true} [:or [:string {:min 1}] [:vector {:min 1} [:string {:min 1}]]]]
    [:suite/dir {:optional true} [:string {:min 1}]]
    [:suite/needs {:optional true} Needs]
    [:suite/projects {:optional true} [:vector {:min 1} [:string {:min 1}]]]
    [:suite/env {:optional true} Env]
    [:suite/reports-dir {:optional true} [:string {:min 1}]]
    [:suite/report-format {:optional true} ReportFormat]
    [:suite/timeout-ms {:optional true} [:int {:min 1000}]]
    [:suite/description {:optional true} :string]]
   [:fn {:error/message "a suite needs :suite/image or :suite/command"}
    (fn [s] (or (contains? s :suite/image) (contains? s :suite/command)))]])

(def docker-labels
  "Labels a stack run puts on what it creates (containers, networks), the
   authority on ownership over any name: `hive.run` the RunId, `hive.stack`
   the stack id, `hive.instance` the Instance id, `hive.consumer` the
   consumer a view network or private instance belongs to."
  {:run "hive.run" :stack "hive.stack" :instance "hive.instance" :consumer "hive.consumer"})


(def Project
  "Where a compose project lives. Its YAML is the source of truth: sections are
   resolved from its services, native profiles and depends_on. `:project/programs`
   are the host programs its sections start beside their containers.
   `:project/provides` and `:project/needs` place it in the capability graph."
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
   [:project/activity {:optional true} Activity]
   [:project/provides {:optional true} Provides]
   [:project/needs {:optional true} Needs]])

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
   [:compose/stacks {:optional true} [:map-of :string Stack]]
   [:compose/suites {:optional true} [:map-of :string Suite]]
   [:compose/default-resources {:optional true} Resources]
   [:compose/budget {:optional true} MemSize]
   [:compose/host-reserve {:optional true} MemSize]])

(def InstanceCaps
  "The caps the stack plan resolved for one instance: `:mem` in bytes and
   `:cpus` (each absent when no level sets it), `:jvm?` whether
   JAVA_TOOL_OPTIONS is injected (absent: detected from the image),
   `:unbounded?` the explicit opt-out, and `:source` the level the memory cap
   came from."
  [:map
   [:mem {:optional true} [:int {:min 1}]]
   [:cpus {:optional true} [:and number? [:> 0]]]
   [:jvm? {:optional true} :boolean]
   [:unbounded? :boolean]
   [:source [:enum :stack :need :provide :default :none]]])

(def Budget
  "What the stack plan sums: the caps of each instance, the total capped
   memory (a shared instance counted once), what one private copy per
   consumer would have summed, the difference sharing saved, and the
   instances left out of the sum (unbounded or without a mem cap)."
  [:map
   [:budget/instances [:map-of :string InstanceCaps]]
   [:budget/total-mem [:int {:min 0}]]
   [:budget/unshared-mem [:int {:min 0}]]
   [:budget/saved-mem [:int {:min 0}]]
   [:budget/uncapped [:vector :string]]])

(def OverBudget
  "The data of the Result error :compose/over-budget. `:basis` names the
   limit that refused: :stack (:stack/budget), :budget (:compose/budget) or
   :host (MemAvailable minus :compose/host-reserve, both then given). `:limit`
   is that limit in bytes, `:total-mem` the plan's capped sum, `:over` their
   difference; `:instances` each capped instance by memory, largest first."
  [:map
   [:basis [:enum :stack :budget :host]]
   [:limit [:int {:min 0}]]
   [:total-mem [:int {:min 0}]]
   [:over [:int {:min 1}]]
   [:available {:optional true} [:int {:min 0}]]
   [:reserve {:optional true} [:int {:min 0}]]
   [:instances [:vector [:map [:instance :string] [:mem [:int {:min 0}]]]]]
   [:message :string]])

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
