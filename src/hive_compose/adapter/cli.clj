(ns hive-compose.adapter.cli
  "IComposeEngine over the docker compose CLI. Argv construction and output
   parsing are pure; the process runs through an injected `exec` with
   hive-system's `shell.core/exec!` contract (argv, opts) -> Result.

   Secret references in a profile's env are read only by the calls that create
   containers (`up`, a recreating `restart`), through the injected resolver
   (see `hive-compose.adapter.secrets`). Every other call is given their
   renderings, so it interpolates and reads no store."
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [hive-compose.adapter.secrets :as secrets]
            [hive-compose.port :as port]
            [hive-compose.promote.env :as env]
            [hive-compose.promote.isolated :as isolated]
            [hive-compose.promote.profile :as profile]
            [hive-dsl.result :as r]
            [hive-system.shell.core :as shell]))

;; SPDX-License-Identifier: MIT

(def ^:private max-output-chars 20000)

(defn- clip [s]
  (let [s (or s "")]
    (if (> (count s) max-output-chars) (subs s (- (count s) max-output-chars)) s)))

;; ---------------------------------------------------------------------------
;; argv

(defn base-argv
  "`docker compose` addressed at profile `p`: project dir, project name,
   files, the generated override of an isolated run (`:profile/override`,
   last) and native compose profiles. A profile with no override gives the
   argv it always gave."
  [p]
  (-> ["docker" "compose" "--project-directory" (:profile/dir p) "-p" (profile/project p)]
      (into (mapcat (fn [f] ["-f" f])) (:profile/files p))
      (cond-> (:profile/override p) (into ["-f" (:profile/override p)]))
      (into (mapcat (fn [cp] ["--profile" cp])) (:profile/compose-profiles p))))

(defn model-argv
  "The merged model of `p`'s project, every native profile enabled, its
   override applied when it names one (callers computing the override pass
   `p` without it)."
  [p]
  (-> (base-argv (dissoc p :profile/compose-profiles))
      (into ["--profile" "*" "config" "--format" "json"])))

(declare lines parse-json-rows parse-labels)

(defn image-ls-argv
  "Every local image as `<repo>:<tag>\\t<full id>\\t<repo>@<digest>`, one a line."
  []
  ["docker" "image" "ls" "--no-trunc" "--digests" "--format" "{{.Repository}}:{{.Tag}}\t{{.ID}}\t{{.Repository}}@{{.Digest}}"])

(defn image-ids
  "{image id} of those of `images` (refs normalised: `docker.io/library/x:t`,
   a tagless ref as `:latest`, `x@sha256:..` by its repo digest) that
   `image-ls-argv` output `s` lists."
  [s images]
  (let [have (into {} (for [l (lines s)
                            :let [[ref id dig] (str/split l #"\t" 3)]
                            :when (seq id)
                            k [ref dig]
                            :when (and (seq k) (not (str/includes? k "<none>")))]
                        [k id]))]
    (into (sorted-map) (keep (fn [i] (when-let [id (have (isolated/normalize-ref i))] [i id]))) images)))

(defn project-ids-argv
  "Full ids of the running containers of compose project `cp`, by label."
  [cp]
  ["docker" "ps" "-q" "--no-trunc" "--filter" (str "label=com.docker.compose.project=" cp)
   "--filter" "status=running"])

(defn volume-ls-argv
  "Every volume labelled with compose project `cp`, one JSON object a line."
  [cp]
  ["docker" "volume" "ls" "--filter" (str "label=com.docker.compose.project=" cp) "--format" "{{json .}}"])

(defn volume-rm-argv
  "`docker volume rm` of exactly one volume, never -f."
  [name]
  ["docker" "volume" "rm" name])

(defn volume-rows
  "[{:name :labels}] of `volume-ls-argv` output."
  [s]
  (vec (sort-by :name (for [row (parse-json-rows s)]
                        {:name (get row "Name") :labels (parse-labels (get row "Labels"))}))))

(defn in-use?
  "True when docker refused a `volume rm` because a container uses it."
  [stderr]
  (boolean (re-find #"(?i)volume is in use" (str stderr))))

(defn closure-argv [p]
  (-> (base-argv p) (into ["config" "--format" "json"]) (into (:profile/services p))))

(defn ps-argv [p]
  (into (base-argv p) ["ps" "--format" "json"]))

(defn ids-argv
  "The ids of the running containers of `p`'s project."
  [p]
  (into (base-argv p) ["ps" "-q"]))

(defn inspect-argv [ids]
  (into ["docker" "inspect" "--type" "container"] ids))

(defn listed-argv [p]
  (into (base-argv p) ["config" "--services"]))

(defn profiles-argv [p]
  (into (base-argv p) ["config" "--profiles"]))

(defn lines
  "Non-blank trimmed lines of `s`."
  [s]
  (vec (remove str/blank? (map str/trim (str/split-lines (or s ""))))))

(defn up-argv [p services]
  (-> (base-argv p)
      (into ["up" "-d"])
      (cond-> (:profile/wait? p) (conj "--wait"))
      (cond-> (:profile/no-deps? p) (conj "--no-deps"))
      (into services)))

(defn stop-argv [p services]
  (-> (base-argv p) (conj "stop") (into services)))

(defn down-argv [p services]
  (-> (base-argv p) (conj "down") (into services)))

(defn restart-argv
  "`compose restart` of `services`, or when `recreate?` an `up -d
   --force-recreate --no-deps` of exactly them: their dependencies are left
   as they are."
  [p services recreate?]
  (-> (base-argv p)
      (into (if recreate? ["up" "-d" "--force-recreate" "--no-deps"] ["restart"]))
      (into services)))

(defn logs-argv
  "`compose logs` of `services` (all when empty), the last `tail` lines each;
   with `:profile/timestamps?` every line carries docker's RFC 3339 time, so
   a time window can be cut from it (hc-10 why)."
  [p services tail]
  (-> (base-argv p)
      (into ["logs" "--no-color" "--tail" (str tail)])
      (cond-> (:profile/timestamps? p) (conj "--timestamps"))
      (cond-> (:profile/since p) (into ["--since" (str (:profile/since p))]))
      (cond-> (:profile/until p) (into ["--until" (str (:profile/until p))]))
      (into services)))

(defn containers-argv
  "Every container of `p`'s compose project, running or not, found by its
   compose labels, one `<name>\t<service>` line each. Loads no compose file."
  [p]
  ["docker" "ps" "-a" "--filter" (str "label=com.docker.compose.project=" (profile/project p))
   "--format" "{{.Names}}\t{{.Label \"com.docker.compose.service\"}}"])

(defn container-names
  "Sorted names of the containers of `services` in `containers-argv` output."
  [s services]
  (let [wanted (set services)]
    (vec (sort (for [line (lines s)
                     :let [[nm svc] (str/split line #"\t" 2)]
                     :when (and (seq nm) (wanted svc))]
                 nm)))))

(defn running-containers-argv
  "The running containers of `p`'s compose project, as `containers-argv`."
  [p]
  ["docker" "ps" "--filter" (str "label=com.docker.compose.project=" (profile/project p))
   "--filter" "status=running"
   "--format" "{{.Names}}\t{{.Label \"com.docker.compose.service\"}}"])

(defn container-services
  "{name service} of `containers-argv` output."
  [s]
  (into (sorted-map)
        (for [line (lines s)
              :let [[nm svc] (str/split line #"\t" 2)]
              :when (and (seq nm) (seq svc))]
          [nm svc])))

(defn stats-argv
  "One `docker stats` sample of container `names`, one `<name>\t<cpu%>` line each."
  [names]
  (into ["docker" "stats" "--no-stream" "--format" "{{.Name}}\t{{.CPUPerc}}"] names))

(defn cpu-percent
  "The number in a `docker stats` CPU column (\"12.5%\"), nil when there is
   none (\"--\" for a container that just stopped)."
  [s]
  (let [t (str/replace (str/trim (str s)) #"%$" "")]
    (when (re-matches #"\d+(\.\d+)?" t)
      (Double/parseDouble t))))

(defn service-cpu
  "{service percent} of `stats-argv` output `s` for containers `names`
   ({name service}): the busiest container of each service. A container
   whose reading is no number is left out."
  [names s]
  (reduce (fn [acc line]
            (let [[nm col] (str/split line #"\t" 2)
                  svc (get names nm)
                  pct (cpu-percent col)]
              (if (and svc pct)
                (update acc svc (fnil max 0.0) pct)
                acc)))
          (sorted-map)
          (lines s)))

(defn stop-containers-argv
  "`docker stop` of container `names`."
  [names]
  (into ["docker" "stop"] names))

(defn rm-containers-argv
  "`docker rm` of stopped container `names`: their anonymous volumes stay."
  [names]
  (into ["docker" "rm"] names))

(def ls-argv ["docker" "compose" "ls" "--all" "--format" "json"])

(def version-argv ["docker" "compose" "version" "--short"])

;; ---------------------------------------------------------------------------
;; parsing

(defn parse-json-rows
  "Rows of compose `--format json` output, which is a JSON array or one JSON
   object per line depending on the compose version."
  [s]
  (let [t (str/trim (or s ""))]
    (cond
      (str/blank? t) []
      (str/starts-with? t "[") (vec (json/read-str t))
      :else (mapv json/read-str (remove str/blank? (str/split-lines t))))))

(defn running-services
  "Sorted set of services with a running container in `ps` rows."
  [rows]
  (into (sorted-set)
        (comp (filter #(= "running" (get % "State")))
              (keep #(get % "Service")))
        rows))

(defn published-ports
  "{service #{host-port}} of the running containers in `ps` rows: the
   PublishedPort of each of their Publishers that publishes one."
  [rows]
  (reduce (fn [acc row]
            (let [svc (get row "Service")
                  ports (into #{} (keep (fn [pub]
                                          (let [p (get pub "PublishedPort")]
                                            (when (and (int? p) (pos? p)) (long p)))))
                              (get row "Publishers"))]
              (if (and svc (= "running" (get row "State")) (seq ports))
                (update acc svc (fnil into #{}) ports)
                acc)))
          {}
          rows))

(defn container-addresses
  "#{ip} of `docker inspect` rows: the IPv4 and global IPv6 address each
   running container holds on each of its networks."
  [rows]
  (into (sorted-set)
        (comp (filter #(true? (get-in % ["State" "Running"])))
              (mapcat #(vals (get-in % ["NetworkSettings" "Networks"])))
              (mapcat (juxt #(get % "IPAddress") #(get % "GlobalIPv6Address")))
              (remove str/blank?))
        rows))

(defn closure-services
  "Sorted services of a `config --format json` document."
  [s]
  (vec (sort (keys (get (json/read-str s) "services")))))

(defn dependency-graph
  "{service {dep {:required? bool}}} of a `config --format json` document.
   Map-form depends_on reads each entry's \"required\" (absent = true);
   list-form depends_on makes every entry required."
  [s]
  (letfn [(edge [e]
            (if (map? e)
              (not (false? (get e "required" true)))
              true))
          (edges [deps]
            (if (map? deps)
              (into {} (for [[d e] deps] [d {:required? (edge e)}]))
              (into {} (for [d deps] [d {:required? true}]))))]
    (into {} (for [[svc m] (get (json/read-str s) "services")]
               [svc (edges (get m "depends_on"))]))))

(defn projects
  "Project summaries of `ls` rows."
  [rows]
  (mapv (fn [m] {:name (get m "Name")
                 :status (get m "Status")
                 :config-files (get m "ConfigFiles")})
        rows))

(def live-ids-argv
  "Full ids of every running container the daemon knows, any project."
  ["docker" "ps" "-q" "--no-trunc"])

(defn service-facts
  "{service {:image :java-tool-options :mem-limit}} of `services` in a
   `config --format json` document: the image each runs, the
   JAVA_TOOL_OPTIONS its file sets (environment as a map or a list of
   VAR=value) and its own memory limit (mem_limit or
   deploy.resources.limits.memory), when set. Pure."
  [s services]
  (let [svcs (get (json/read-str s) "services")
        jto (fn [envm] (cond
                         (map? envm) (get envm "JAVA_TOOL_OPTIONS")
                         (sequential? envm) (some #(when (str/starts-with? (str %) "JAVA_TOOL_OPTIONS=")
                                                     (subs % (count "JAVA_TOOL_OPTIONS=")))
                                                  envm)))]
    (into (sorted-map)
          (for [sv services
                :let [m (get svcs sv)]
                :when m]
            [sv (let [lim (or (get m "mem_limit") (get-in m ["deploy" "resources" "limits" "memory"]))]
                  (cond-> {:image (str (get m "image" ""))}
                    (string? (jto (get m "environment"))) (assoc :java-tool-options (jto (get m "environment")))
                    (some? lim) (assoc :mem-limit (if (number? lim) (long lim) (str lim)))))]))))

(defn service-resources
  "{:volumes #{name} :ports #{port}} of `services` in a `config --format json`
   document: the real names of the named volumes they mount (the top-level
   `volumes` entry's name, else the key) and the host ports they publish.
   Pure."
  [s services]
  (let [doc (json/read-str s)
        vols (get doc "volumes")
        wanted (set services)
        svcs (filter (comp wanted key) (get doc "services"))
        port-of (fn [p] (let [v (get p "published")]
                          (cond (int? v) (long v)
                                (and (string? v) (re-matches #"\d+" v)) (Long/parseLong v)
                                :else nil)))]
    {:volumes (into (sorted-set)
                    (for [[_ m] svcs
                          v (get m "volumes")
                          :when (= "volume" (get v "type"))
                          :let [src (get v "source")]
                          :when (seq src)]
                      (or (get-in vols [src "name"]) src)))
     :ports (into (sorted-set) (for [[_ m] svcs p (get m "ports") :let [n (port-of p)] :when n] n))}))

;; ---------------------------------------------------------------------------
;; stacks: networks, labelled projects, labelled containers (IStackEngine)
;;
;; compose has no CLI flag putting labels on a service's containers, nor one
;; attaching it to an extra network. A stack run therefore adds a generated
;; override file (JSON, which compose reads as YAML) as the last `-f`: it
;; carries the labels, the external view networks and the names of the
;; variables passed into the containers. Values never go in the file: a
;; variable is listed with a null value, which compose fills from its own
;; process environment, so a secret read at spawn stays in memory. Instances
;; that already run join a view network with `docker network connect
;; --alias`, which recreates nothing.

(defn label-args
  "`--label k=v` pairs of `labels`, sorted by key."
  [labels]
  (vec (mapcat (fn [[k v]] ["--label" (str k "=" v)]) (sort-by key labels))))

(defn label-filter-args
  "`--filter label=k=v` pairs of `labels`, sorted by key."
  [labels]
  (vec (mapcat (fn [[k v]] ["--filter" (str "label=" k "=" v)]) (sort-by key labels))))

(defn network-ls-argv
  "The network named exactly `network`, its name on one line when present."
  [network]
  ["docker" "network" "ls" "--filter" (str "name=^" network "$") "--format" "{{.Name}}"])

(defn network-create-argv [network labels]
  (-> ["docker" "network" "create" "--driver" "bridge"] (into (label-args labels)) (conj network)))

(defn network-containers-argv
  "The containers attached to `network`, one name per line."
  [network]
  ["docker" "network" "inspect" network "--format" "{{range .Containers}}{{.Name}}\n{{end}}"])

(defn network-disconnect-argv [network container]
  ["docker" "network" "disconnect" "-f" network container])

(defn network-rm-argv [network]
  ["docker" "network" "rm" network])

(defn service-containers-argv
  "Every container of `service` in compose project `project`, running or not,
   one name per line. Loads no compose file."
  [project service]
  ["docker" "ps" "-a"
   "--filter" (str "label=com.docker.compose.project=" project)
   "--filter" (str "label=com.docker.compose.service=" service)
   "--format" "{{.Names}}"])

(defn network-connect-argv
  "`docker network connect` of `container` to `network` under every alias."
  [network container aliases]
  (-> ["docker" "network" "connect"]
      (into (mapcat (fn [a] ["--alias" a])) aliases)
      (conj network container)))

(defn already-attached?
  "True when docker refused a `network connect` because the container is
   already on the network."
  [stderr]
  (boolean (re-find #"(?i)already exists|already attached" (str stderr))))

(defn override?
  "True when ProjectSpec `spec` needs a generated override file."
  [spec]
  (boolean (or (seq (:labels spec)) (seq (:networks spec)) (seq (:container-env spec))
               (some seq (vals (:service-overrides spec))))))

(defn override-doc
  "The compose override of ProjectSpec `spec` as data: each of its
   `:services` gets `:labels`, joins the external `:networks` (its project's
   default network is then left out, so names resolve on those networks
   alone; networks a service declares in its own files stay) and receives the
   variables `:container-env` names, as null so compose reads their values
   from its own environment. `:service-overrides` ({service {key value}},
   hc-16a resource caps: mem_limit, cpus, environment {VAR value}) is
   merged into the entry of each service it names, environment per
   variable; a service it names that `:services` does not is added. Pure."
  [{:keys [services labels networks container-env service-overrides]}]
  (let [svc (cond-> {}
              (seq labels) (assoc "labels" (into (sorted-map) labels))
              (seq networks) (assoc "networks" (into (sorted-map) (map (fn [n] [n {}])) networks))
              (seq container-env) (assoc "environment" (into (sorted-map) (map (fn [v] [v nil])) container-env)))
        entry (fn [s]
                (let [o (get service-overrides s)
                      e (get o "environment")]
                  (cond-> (merge (if (some #{s} services) svc {}) (dissoc o "environment"))
                    (seq e) (update "environment" #(into (sorted-map) (merge % e))))))
        named (distinct (concat services (sort (keys (into {} (filter (comp seq val)) service-overrides)))))]
    (cond-> {"services" (into (sorted-map) (map (fn [s] [s (entry s)])) named)}
      (seq networks) (assoc "networks" (into (sorted-map)
                                             (map (fn [n] [n {"external" true "name" n}]))
                                             networks)))))

(defn override-json
  "`override-doc` of `spec` as JSON text."
  [spec]
  (json/write-str (override-doc spec) :escape-slash false))

(defn override-path
  "Where the override of compose project `project` is written under `dir`."
  [dir project]
  (str dir "/" project ".override.json"))

(defn project-argv
  "`docker compose` addressed at ProjectSpec `spec`, `override` (a path, or
   nil) as the last `-f`."
  [spec override]
  (-> ["docker" "compose"]
      (cond-> (:dir spec) (into ["--project-directory" (:dir spec)]))
      (into ["-p" (:project spec)])
      (into (mapcat (fn [f] ["-f" f])) (:files spec))
      (cond-> override (into ["-f" override]))))

(defn up-project-argv [spec override]
  (-> (project-argv spec override)
      (into ["up" "-d"])
      (cond-> (:wait? spec) (conj "--wait"))
      (cond-> (and (:wait? spec) (:wait-timeout-s spec)) (into ["--wait-timeout" (str (:wait-timeout-s spec))]))
      (cond-> (:no-deps? spec) (conj "--no-deps"))
      (into (:services spec))))

(defn down-project-argv
  "Remove the containers of `spec`'s services, or with `:all?` the whole
   project and its own networks. Never `-v`."
  [spec override]
  (if (:all? spec)
    (into (project-argv spec override) ["down" "--remove-orphans"])
    (-> (project-argv spec override) (conj "down") (into (:services spec)))))

(defn labelled-containers-argv
  "Every container, running or not, carrying all `labels`, one JSON object a line."
  [labels]
  (-> ["docker" "ps" "-a" "--no-trunc"] (into (label-filter-args labels)) (into ["--format" "{{json .}}"])))

(defn labelled-networks-argv
  "Every network carrying all `labels`, one JSON object a line."
  [labels]
  (-> ["docker" "network" "ls"] (into (label-filter-args labels)) (into ["--format" "{{json .}}"])))

(defn parse-labels
  "{k v} of docker's `Labels` column, \"k=v,k2=v2\". A value holding a comma
   is cut there: docker joins with no escape."
  [s]
  (into (sorted-map)
        (for [kv (str/split (str s) #",")
              :let [[k v] (str/split kv #"=" 2)]
              :when (seq k)]
          [k (or v "")])))

(defn health-of
  "The health in a docker `Status` column: \"Up 2 minutes (healthy)\" ->
   :healthy, \"(unhealthy)\" -> :unhealthy, \"(health: starting)\" -> :starting,
   anything else :none (no healthcheck)."
  [status]
  (let [s (str status)]
    (cond (str/includes? s "(unhealthy)") :unhealthy
          (str/includes? s "(healthy)") :healthy
          (str/includes? s "health: starting") :starting
          :else :none)))

(defn labelled-containers
  "Rows of `labelled-containers-argv` output: {:name :project :service
   :labels :state :health}, sorted by name."
  [s]
  (vec (sort-by :name
                (for [row (parse-json-rows s)
                      :let [labels (parse-labels (get row "Labels"))]]
                  {:name (get row "Names")
                   :project (get labels "com.docker.compose.project")
                   :service (get labels "com.docker.compose.service")
                   :labels labels
                   :state (get row "State")
                   :health (health-of (get row "Status"))}))))

(defn labelled-networks
  "Rows of `labelled-networks-argv` output: {:name :labels}, sorted by name."
  [s]
  (vec (sort-by :name
                (for [row (parse-json-rows s)]
                  {:name (get row "Name") :labels (parse-labels (get row "Labels"))}))))

;; ---------------------------------------------------------------------------
;; boundary

(defn- run
  "Result<stdout> of `argv` in `p`'s directory. With no `env`, `p`'s own with
   its references rendered, never read; `scrub` cleans what docker printed
   before it reaches a result."
  ([exec p argv timeout-ms]
   (run exec p argv timeout-ms (env/rendered (:profile/env p)) identity))
  ([exec p argv timeout-ms env scrub]
   (r/bind (exec argv (cond-> {:timeout-ms timeout-ms}
                        (:profile/dir p) (assoc :dir (:profile/dir p))
                        (seq env) (assoc :env env)))
           (fn [{:keys [exit stdout stderr]}]
             (if (zero? exit)
               (r/ok (scrub (or stdout "")))
               (r/err :compose/command-failed {:argv argv :exit exit :stderr (clip (scrub stderr))}))))))

(defn- parsed [res category f]
  (r/bind res (fn [out] (r/try-effect* category (f out)))))

(defn- act
  "Run `argv`, which creates no container, over `services`: references in the
   env stay their renderings, nothing is read from a store."
  [exec p argv timeout-ms services]
  (if (empty? services)
    (r/ok {:out "" :skipped :no-services})
    (r/map-ok (run exec p argv timeout-ms) (fn [out] {:out (clip out)}))))

(defn- spawn
  "Run `argv`, which creates containers, over `services`: the env's references
   are read from their stores now, and only now. One that cannot be read fails
   the call before docker runs. What docker prints has every resolved value
   replaced by its reference's rendering."
  [exec resolver p argv timeout-ms services]
  (if (empty? services)
    (r/ok {:out "" :skipped :no-services})
    (let [declared (:profile/env p)]
      (r/let-ok [resolved (secrets/resolve-env resolver declared)]
        (r/map-ok (run exec p argv timeout-ms resolved #(env/scrub declared resolved %))
                  (fn [out] {:out (clip out)}))))))

(defrecord CliEngine [exec resolver timeout-ms up-timeout-ms]
  port/IComposeEngine
  (-version [_]
    (r/map-ok (run exec nil version-argv timeout-ms) str/trim))
  (-closure [_ p]
    (parsed (run exec p (closure-argv p) timeout-ms) :compose/unparseable closure-services))
  (-listed [_ p]
    (r/map-ok (run exec p (listed-argv p) timeout-ms) lines))
  (-profiles [_ p]
    (r/map-ok (run exec p (profiles-argv p) timeout-ms) (comp set lines)))
  (-running [_ p]
    (parsed (run exec p (ps-argv p) timeout-ms) :compose/unparseable
            (comp running-services parse-json-rows)))
  (-projects [_]
    (parsed (run exec nil ls-argv timeout-ms) :compose/unparseable
            (comp projects parse-json-rows)))
  (-up! [_ p services]
    (spawn exec resolver p (up-argv p services) up-timeout-ms services))
  (-stop! [_ p services]
    (act exec p (stop-argv p services) timeout-ms services))
  (-down! [_ p services]
    (act exec p (down-argv p services) timeout-ms services))
  (-restart! [_ p services recreate?]
    (if recreate?
      (spawn exec resolver p (restart-argv p services true) up-timeout-ms services)
      (act exec p (restart-argv p services false) timeout-ms services)))
  (-logs [_ p services tail]
    (r/map-ok (run exec p (logs-argv p services tail) timeout-ms) (fn [out] {:out (clip out)})))

  port/IDependencyGraph
  ;; The same targeted query as -closure: compose keeps an explicitly targeted
  ;; service behind a native profile only when it is named, so an untargeted
  ;; query would drop that service and its edges.
  (-dependencies [_ p]
    (parsed (run exec p (closure-argv p) timeout-ms)
            :compose/unparseable dependency-graph))

  port/IContainerControl
  ;; By label and name only: no compose file is loaded, so neither a service
  ;; the project's files no longer define nor a variable nobody set stops it.
  (-stop-containers! [_ p services action]
    (if (empty? services)
      (r/ok {:out "" :skipped :no-services :containers []})
      (r/let-ok [names (r/map-ok (run exec nil (containers-argv p) timeout-ms)
                                 #(container-names % services))]
        (if (empty? names)
          (r/ok {:out "" :skipped :no-containers :containers []})
          (r/let-ok [stopped (run exec nil (stop-containers-argv names) timeout-ms)
                     removed (if (= :down action)
                               (run exec nil (rm-containers-argv names) timeout-ms)
                               (r/ok ""))]
            (r/ok {:out (clip (str stopped removed)) :containers names}))))))

  port/IPublishedPorts
  (-published [_ p]
    (parsed (run exec p (ps-argv p) timeout-ms) :compose/unparseable
            (comp published-ports parse-json-rows)))

  port/IServiceImages
  (-service-facts [_ p services]
    (if (empty? services)
      (r/ok {})
      (parsed (run exec p (closure-argv (assoc p :profile/services (vec services))) timeout-ms)
              :compose/unparseable #(service-facts % services))))

  port/IContainerOwnership
  (-live-containers [_]
    (r/map-ok (run exec nil live-ids-argv timeout-ms) (comp set lines)))
  ;; Every service named, not a targeted query: a service behind a native
  ;; profile is shown only when named, so name exactly the ones asked for.
  (-resources [_ p services]
    (if (empty? services)
      (r/ok {:volumes #{} :ports #{}})
      (parsed (run exec p (closure-argv (assoc p :profile/services (vec services))) timeout-ms)
              :compose/unparseable #(service-resources % services))))

  port/IContainerAddresses
  (-addresses [_ p]
    (r/let-ok [ids (r/map-ok (run exec p (ids-argv p) timeout-ms) lines)]
      (if (empty? ids)
        (r/ok #{})
        (parsed (run exec p (inspect-argv ids) timeout-ms) :compose/unparseable
                (comp container-addresses parse-json-rows)))))

  port/IContainerLoad
  ;; By label, like -stop-containers!: no compose file is loaded.
  (-cpu [_ p]
    (r/let-ok [names (r/map-ok (run exec nil (running-containers-argv p) timeout-ms)
                               container-services)]
      (if (empty? names)
        (r/ok {})
        (r/map-ok (run exec nil (stats-argv (keys names)) timeout-ms)
                  #(service-cpu names %))))))

(defn container-ids-argv
  "Every container of `p`'s compose project, running or not, by label, one
   `<id>\t<service>` line each. Loads no compose file."
  [p]
  ["docker" "ps" "-a" "--no-trunc" "--filter" (str "label=com.docker.compose.project=" (profile/project p))
   ;; a `compose run` one-off carries the same service label: never the service's container
   "--filter" "label=com.docker.compose.oneoff=False"
   "--format" "{{.ID}}\t{{.Label \"com.docker.compose.service\"}}"])

(defn service-container-ids
  "{service id} of `container-ids-argv` output for `services`. Pure."
  [s services]
  (let [wanted (set services)]
    (into (sorted-map)
          (for [line (lines s)
                :let [[id svc] (str/split line #"\t" 2)]
                :when (and (seq id) (wanted svc))]
            [svc id]))))

(extend-type CliEngine
  port/IDirProbe
  (-dir-exists? [_ dir] (.isDirectory (io/file (str dir)))))

(extend-type CliEngine
  port/IContainerIds
  (-container-ids [{:keys [exec timeout-ms]} p services]
    (if (empty? services)
      (r/ok {})
      (r/map-ok (run exec nil (container-ids-argv p) timeout-ms)
                #(service-container-ids % services)))))

(defn- write-override!
  "Result<path-or-nil>: the override of `spec` written under `dir`, nil when
   `spec` needs none. Holds no value, only names (see `override-doc`)."
  [dir spec]
  (if (override? spec)
    (r/try-effect* :compose/override-unwritable
      (let [path (override-path dir (:project spec))
            f (io/file path)]
        (io/make-parents f)
        (spit f (override-json spec))
        path))
    (r/ok nil)))

(defn- run-argv
  "Result<stdout> of a docker call addressed by argv alone."
  [exec argv timeout-ms]
  (run exec nil argv timeout-ms nil identity))

(def compose-default-files
  "The files compose reads when given no -f, in its own order."
  ["compose.yaml" "compose.yml" "docker-compose.yaml" "docker-compose.yml"])

(defn default-file
  "The first of `compose-default-files` for which `exists?` is true, in
   compose's own order; nil when none. Pure over the probe."
  [exists?]
  (first (filter exists? compose-default-files)))

(defn- with-default-files
  "Result<spec>: `spec` naming its project's default compose file when it
   names none. An override passed as `-f` replaces compose's default lookup,
   so the base file must be named before it; a project dir holding none of
   `compose-default-files` is :compose/no-compose-file. The file probe is
   here, at the boundary; `project-argv` stays pure."
  [spec]
  (if (or (seq (:files spec)) (nil? (:dir spec)))
    (r/ok spec)
    (if-let [f (default-file #(.exists (io/file (:dir spec) %)))]
      (r/ok (assoc spec :files [f]))
      (r/err :compose/no-compose-file
             {:project (:project spec) :dir (:dir spec) :looked-for compose-default-files
              :hint "set :project/files, or add a compose file to the project dir"}))))

(defn- stack-up!
  [{:keys [exec resolver up-timeout-ms override-dir]} spec]
  (if (empty? (:services spec))
    (r/ok {:out "" :skipped :no-services})
    (r/let-ok [spec (if (override? spec) (with-default-files spec) (r/ok spec))
               override (write-override! override-dir spec)
               resolved (secrets/resolve-env resolver (:env spec))]
      (let [declared (:env spec)
            argv (up-project-argv spec override)]
        (r/map-ok (run exec {:profile/dir (:dir spec)} argv up-timeout-ms resolved
                       #(env/scrub declared resolved %))
                  (fn [out] {:out (clip out) :override override}))))))

(defn- stack-down!
  [{:keys [exec timeout-ms override-dir]} spec]
  (if (and (not (:all? spec)) (empty? (:services spec)))
    (r/ok {:out "" :skipped :no-services})
    (let [override (let [p (override-path override-dir (:project spec))]
                     (when (.exists (io/file p)) p))
          ;; a base file that has since gone still lets the down run on the override
          spec (if override (let [x (with-default-files spec)] (if (r/ok? x) (:ok x) spec)) spec)]
      (r/map-ok (run exec {:profile/dir (:dir spec)} (down-project-argv spec override) timeout-ms
                     (env/rendered (:env spec)) identity)
                (fn [out] {:out (clip out)})))))

(defn- connect-one
  "Result<name>: `container` connected to `network`; already attached is ok."
  [exec timeout-ms network container aliases]
  (let [argv (network-connect-argv network container aliases)]
    (r/bind (exec argv {:timeout-ms timeout-ms})
            (fn [{:keys [exit stderr]}]
              (if (or (zero? exit) (already-attached? stderr))
                (r/ok container)
                (r/err :compose/command-failed {:argv argv :exit exit :stderr (clip stderr)}))))))

(defn- results
  "Result<[v]> of a seq of Results, the first error winning."
  [rs]
  (reduce (fn [acc x] (if (r/err? x) (reduced x) (r/map-ok acc #(conj % (:ok x)))))
          (r/ok [])
          rs))

(extend-type CliEngine
  port/IStackEngine
  (-network-create! [{:keys [exec timeout-ms]} network labels]
    (r/let-ok [present (r/map-ok (run-argv exec (network-ls-argv network) timeout-ms) lines)]
      (if (some #{network} present)
        (r/ok {:network network :created? false})
        (r/map-ok (run-argv exec (network-create-argv network labels) timeout-ms)
                  (fn [_] {:network network :created? true})))))
  (-network-rm! [{:keys [exec timeout-ms]} network]
    (r/let-ok [present (r/map-ok (run-argv exec (network-ls-argv network) timeout-ms) lines)]
      (if-not (some #{network} present)
        (r/ok {:network network :removed? false :disconnected []})
        (r/let-ok [attached (r/map-ok (run-argv exec (network-containers-argv network) timeout-ms) lines)
                   gone (results (for [c attached]
                                   (r/map-ok (run-argv exec (network-disconnect-argv network c) timeout-ms)
                                             (constantly c))))
                   _ (run-argv exec (network-rm-argv network) timeout-ms)]
          (r/ok {:network network :removed? true :disconnected gone})))))
  (-network-connect! [{:keys [exec timeout-ms]} network {:keys [project service]} aliases]
    (r/let-ok [names (r/map-ok (run-argv exec (service-containers-argv project service) timeout-ms) lines)]
      (if (empty? names)
        (r/err :compose/no-container {:project project :service service :network network})
        (r/map-ok (results (for [c names] (connect-one exec timeout-ms network c aliases)))
                  (fn [cs] {:network network :containers cs})))))
  (-up-project! [e spec] (stack-up! e spec))
  (-down-project! [e spec] (stack-down! e spec))
  (-running-labelled [{:keys [exec timeout-ms]} labels]
    (parsed (run-argv exec (labelled-containers-argv labels) timeout-ms)
            :compose/unparseable labelled-containers))
  (-networks-labelled [{:keys [exec timeout-ms]} labels]
    (parsed (run-argv exec (labelled-networks-argv labels) timeout-ms)
            :compose/unparseable labelled-networks)))

(extend-type CliEngine
  port/IIsolatedRuns
  (-model [{:keys [exec timeout-ms]} p]
    (parsed (run exec p (model-argv p) timeout-ms) :compose/unparseable json/read-str))
  (-image-ids [{:keys [exec timeout-ms]} images]
    (if (empty? images)
      (r/ok {})
      (r/map-ok (run-argv exec (image-ls-argv) timeout-ms) #(image-ids % images))))
  ;; By label: no compose file is loaded; one inspect gives ports and images.
  (-run-facts [{:keys [exec timeout-ms]} p]
    (r/let-ok [ids (r/map-ok (run-argv exec (project-ids-argv (profile/project p)) timeout-ms) lines)]
      ;; m10: one inspect per container, so one that exits between ps and
      ;; inspect ("No such object") drops out instead of failing the row
      (let [rows (into [] (mapcat (fn [id]
                                    (let [x (run-argv exec (inspect-argv [id]) timeout-ms)]
                                      (if (r/ok? x) (try (parse-json-rows (:ok x)) (catch Exception _ [])) []))))
                       ids)]
        (r/ok (isolated/container-facts rows)))))
  (-run-volumes [{:keys [exec timeout-ms]} cp]
    (parsed (run-argv exec (volume-ls-argv cp) timeout-ms) :compose/unparseable volume-rows))
  ;; The single volume-removing call (see the port's docstring): exact names,
  ;; one `docker volume rm` each, never -f, never a prune or a `down -v`.
  (-remove-volumes! [{:keys [exec timeout-ms]} names]
    (r/ok (reduce (fn [acc nm]
                    (let [res (exec (volume-rm-argv nm) {:timeout-ms timeout-ms})
                          {:keys [exit stderr]} (:ok res)]
                      (cond
                        (and (r/ok? res) (zero? exit)) (update acc :removed conj nm)
                        ;; m5: already gone (another pass removed it): not an error
                        (and (r/ok? res) (re-find #"(?i)no such volume" (str stderr))) (update acc :gone (fnil conj []) nm)
                        (and (r/ok? res) (in-use? stderr)) (update acc :kept conj {:name nm :reason :in-use})
                        :else (update acc :kept conj {:name nm :reason :failed
                                                      :stderr (clip (str (or stderr (:message res))))}))))
                  {:removed [] :kept []}
                  (distinct names)))))

(defn stack-dir
  "Where stack overrides are written: `stacks/` beside the state file, else
   under the temp dir."
  [opts]
  (if-let [sf (:compose/state-file opts)]
    (str (or (.getParent (io/file sf)) ".") "/stacks")
    (str (System/getProperty "java.io.tmpdir") "/hive-compose-stacks")))

(defn make-engine
  "CliEngine. `opts`: :exec (default hive-system `shell.core/exec!`),
   :compose/resolver ({scheme ISecretBackend}, default the `pass` one over
   that exec), :compose/timeout-ms, :compose/up-timeout-ms,
   :compose/stack-dir (where stack overrides go, default `stack-dir`)."
  [opts]
  (let [exec (or (:exec opts) shell/exec!)]
    (map->CliEngine {:exec exec
                     :resolver (or (:compose/resolver opts) (secrets/make-resolver {:exec exec}))
                     :timeout-ms (or (:compose/timeout-ms opts) 60000)
                     :up-timeout-ms (or (:compose/up-timeout-ms opts) 600000)
                     :override-dir (or (:compose/stack-dir opts) (stack-dir opts))})))
