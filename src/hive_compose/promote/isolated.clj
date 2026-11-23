(ns hive-compose.promote.isolated
  "Pure: an ISOLATED RUN of sections (FUN-D). The same project, targets and
   depends_on closures as the shared sections, started as a second universe
   `docker compose -p <project>-<run>` with a generated override
   (`promote.override`), so nothing clashes with the shared project: no
   container_name, host ports per policy, its own networks and its own data
   volumes (compose names them `<project>-<run>_<key>`).

   Several sections joined to one run id share ONE compose project, exactly
   as sections of the shared project share services: the section logic of
   `pipeline.ops` runs inside the run unchanged; only the compose project
   name, the override file and the state key (`<id>@<run>`) differ.

   A run is recorded under `:runs {run Run}` in the state (see `Run`), kept
   after it ends: the record is the proof a volume belongs to a run
   hive-compose created (`removable`)."
  (:require [clojure.string :as str]
            [hive-compose.promote.override :as override]
            [hive-compose.promote.profile :as profile]
            [hive-compose.schema :as schema]
            [malli.core :as m]))

;; SPDX-License-Identifier: MIT

(def Run
  "One isolated run as the state records it."
  [:map
   [:run schema/RunId]
   [:base-project schema/ComposeProjectName]
   [:compose-project schema/ComposeProjectName]
   [:ports schema/PortPolicy]
   [:isolated? [:= true]]
   [:started-at :int]
   [:ended-at {:optional true} :int]
   [:ended-by {:optional true} [:enum :down :reaper]]])

(def separator "@")

(defn run-id
  "A new RunId for a run of compose project `base`: its name with every char
   outside [a-z0-9] dropped, cut to 30, then `hex`."
  [base hex]
  (let [a (str/replace (str/lower-case (str base)) #"[^a-z0-9]" "")
        a (if (str/blank? a) "run" a)]
    (str (subs a 0 (min 30 (count a))) hex)))

(defn compose-project
  "The compose project of run `run` of shared project `base`."
  [base run]
  (str base "-" run))

(defn section-id
  "The state key of section `id` in run `run`: `<id>@<run>`."
  [id run]
  (str id separator run))

(defn split-id
  "[id run] of an isolated section id, nil for any other id."
  [sid]
  (when-let [[_ id run] (re-matches #"(.+)@([a-z0-9]{3,40})" (str sid))]
    [id run]))

(defn valid-run? [run] (m/validate schema/RunId run))

(defn port-policy
  "PortPolicy of a call param: \"strip\" \"ephemeral\" \"keep\" (or the
   keywords), {\"offset\" n}, {\"publish\" {svc {\"5432\" 15432}}}, nil for
   absent. Anything else is answered as is, for the schema to refuse."
  [v]
  (cond
    (nil? v) nil
    (or (string? v) (keyword? v)) (keyword (name v))
    (map? v) (let [kv (into {} (map (fn [[k x]] [(keyword (name k)) x])) v)]
               (cond
                 (contains? kv :offset) {:offset (:offset kv)}
                 (contains? kv :publish)
                 {:publish (into {} (for [[s m] (:publish kv)]
                                      [(name s) (into {} (for [[c h] m]
                                                           [(if (number? c) (long c) (parse-long (name c)))
                                                            (if (number? h) (long h) h)]))]))}
                 :else kv))
    :else v))

(defn spec
  "OverrideSpec of run `run` of shared project `base` over its merged model
   `model`, ports per `policy`: every container_name reset, the build-only
   services pinned to the shared project's images, and the non-external
   volumes with an explicit name (shared by every copy whatever its -p)
   renamed per run. Compose-derived volume names need nothing: the new -p
   makes them fresh."
  [model base run {:keys [ports images shared-volumes]}]
  (override/spec-of {:run run :names :all :ports (or ports :strip)
                     :fresh-volumes (vec (remove (set shared-volumes) (override/explicit-volumes model)))
                     :images-from base :images images :shared-volumes shared-volumes}))

(defn build-images
  "{service tag} the build-only services of `model` reuse: the tag the shared
   project `base` built (`<base>-<service>`)."
  [model base]
  (into (sorted-map) (for [s (override/build-only model)] [s (override/pinned-image base s)])))

(defn pin-images
  "Result-like {:ok {service id}} | {:error ..}: the image ids a run pins
   for its build-only services `tags` ({service tag}), given what the run
   recorded (`recorded` {service id}) and what the host holds now (`ids`
   {tag id}). A recorded service keeps its id, unless the host tag now names
   another id (:image-changed); a service new to the run is pinned to the
   id its tag names now (:image-missing when the host has none)."
  [tags recorded ids]
  (reduce (fn [acc [s tag]]
            (let [now (get ids tag) was (get recorded s)]
              (cond
                (and was now (not= was now))
                (reduced {:error {:reason :image-changed :service s :image tag :was was :now now}})
                was (assoc-in acc [:ok s] was)
                now (assoc-in acc [:ok s] now)
                :else (reduced {:error {:reason :image-missing :service s :image tag
                                        :hint "up (or build) the shared section first: an isolated run never builds"}}))))
          {:ok (sorted-map)}
          (sort-by key tags)))

(defn port-var
  "HIVE_PORT_<SERVICE>_<PORT> of service `svc` and port key `k` (\"8080\",
   \"53/udp\"): upper-cased, every non-alphanumeric char `_`."
  [svc k]
  (str "HIVE_PORT_" (str/replace (str/upper-case (str svc "_" k)) #"[^A-Z0-9]" "_")))

(def reserved-var-re #"^HIVE_(RUN|RUN_PROJECT|PORT_OFFSET|PORT_.+)$")

(defn run-env
  "The interpolation variables every compose call of run `run` gets (G6):
   HIVE_RUN, HIVE_RUN_PROJECT, HIVE_PORT_OFFSET and one HIVE_PORT_<..> per
   port `planned` ({service {port-key host}}, nil for ephemeral: none)."
  [base run policy planned]
  (into (sorted-map)
        (concat [["HIVE_RUN" run]
                 ["HIVE_RUN_PROJECT" (compose-project base run)]
                 ["HIVE_PORT_OFFSET" (str (or (:offset policy) 0))]]
                (for [[s m] planned [k h] m] [(port-var s k) (str h)]))))

(defn reserved-collisions
  "Sorted keys of a call's `env` that name a variable an isolated run sets."
  [env]
  (vec (sort (filter #(re-matches reserved-var-re (str %)) (keys env)))))

(defn digest-config
  "The run-independent form of the resolved compose config `model` of run
   `run` of `base` (with its override applied), for :config-digest (G3):
   the project name, every occurrence of the run id and of `<base>-<run>`
   replaced by placeholders, and every published host port dropped (they
   differ per run under :offset or :ephemeral and are reported apart under
   :ports). What remains is what the containers run."
  [model base run]
  (let [cp (compose-project base run)
        scrub (fn scrub [x]
                (cond
                  (string? x) (-> x (str/replace cp "<project>") (str/replace run "<run>"))
                  (map? x) (into (sorted-map) (for [[k v] x :when (not= "published" k)] [(scrub k) (scrub v)]))
                  (sequential? x) (mapv scrub x)
                  :else x))]
    (scrub (dissoc model "name"))))

(defn container-facts
  "{:ports {service {port-key host}} :images {service image-id}} of `docker
   inspect` rows of a run's running containers."
  [rows]
  (reduce (fn [acc row]
            (let [svc (get-in row ["Config" "Labels" "com.docker.compose.service"])
                  ports (for [[k bs] (get-in row ["NetworkSettings" "Ports"])
                              :let [[t proto] (str/split (str k) #"/" 2)
                                    h (some #(some-> (get % "HostPort") not-empty parse-long) bs)]
                              :when (and h (pos? h))]
                          [(override/port-key t proto) h])]
              (if-not (and svc (true? (get-in row ["State" "Running"])))
                acc
                (cond-> (assoc-in acc [:images svc] (get row "Image"))
                  (seq ports) (update-in [:ports svc] (fnil into (sorted-map)) ports)))))
          {:ports (sorted-map) :images (sorted-map)}
          rows))

(defn isolate
  "Profile `p` (a preset or a section of shared project `base`) as its
   section in run `run`: id `<id>@<run>`, compose project `<base>-<run>`,
   the override at `override-path` (appended as the last -f by the engine),
   `files` the compose files it names (its own, or the defaults the engine
   resolved, since an extra -f replaces compose's default lookup). Host
   programs stay with the shared section (their ports would clash), and an
   idle isolated section is taken down, never only stopped."
  [p run override-path files]
  (let [base (profile/project p)]
    (cond-> (-> p
                (assoc :profile/id (section-id (:profile/id p) run)
                       :profile/project (compose-project base run)
                       :profile/run run
                       :profile/base-project base
                       :profile/override override-path
                       :profile/idle-action :down)
                (dissoc :profile/programs))
      (seq files) (assoc :profile/files (vec files)))))

(defn isolated? [p] (some? (:profile/run p)))

(def default-files
  "The files compose reads when given no -f, in its own order. An isolated
   section names them explicitly: an extra -f (the override) replaces
   compose's default lookup."
  ["compose.yaml" "compose.yml" "docker-compose.yaml" "docker-compose.yml"])

(defn override-path
  "Where the override of compose project `cp` is written: `isolated/` beside
   the state file."
  [state-file cp]
  (let [s (str state-file)
        i (str/last-index-of s "/")
        dir (if (and i (pos? i)) (subs s 0 i) ".")]
    (str dir "/isolated/" cp ".override.yaml")))

(defn demand
  "{service bytes}: the memory each of `services` of `model` takes for
   admission: its own mem_limit (or deploy.resources.limits.memory), else
   `default-mem` (nil: uncounted)."
  [model services default-mem parse-mem]
  (into (sorted-map)
        (for [s services
              :let [sm (get-in model ["services" s])
                    lim (or (get sm "mem_limit") (get-in sm ["deploy" "resources" "limits" "memory"]))
                    b (or (parse-mem (if (number? lim) (long lim) lim)) default-mem)]
              :when b]
          [s b])))

(defn budget
  "A Budget (schema/Budget) of an isolated run's services `mem` ({service
   bytes}) under compose project `cp`, so the stack admission (hc-16a)
   judges it unchanged. `floor` (bytes) is the least it counts, so a run of
   uncapped services is still refused when the host is already under its
   reserve."
  [cp mem floor]
  (let [total (max floor (reduce + 0 (vals mem)))]
    {:budget/instances (into (sorted-map) (for [[s b] mem] [(str cp "/" s) {:mem b :unbounded? false :source :default}]))
     :budget/total-mem total
     :budget/unshared-mem total
     :budget/saved-mem 0
     :budget/uncapped []}))

(defn env-src
  "{VAR value} of the variables of rendered env `env` whose name ends in
   _SRC (paths, safe to print); no other value is ever shown."
  [env]
  (into (sorted-map) (filter (fn [[k _]] (str/ends-with? (str k) "_SRC"))) env))

(defn section-row
  "What `status` adds to the row of an active isolated section whose profile
   is `p` (R2): the hcf-env-dir keys :dir :files :env-keys :env-src (env
   rendered: a pass ref shows as pass:<path>), and :run. Values only for
   *_SRC variables, never another."
  [p rendered-env]
  {:run (:profile/run p)
   :dir (:profile/dir p)
   :files (vec (:profile/files p))
   :env-keys (vec (sort (keys rendered-env)))
   :env-src (env-src rendered-env)})

(defn run-entry
  "The Run recorded for run `run` of `base` at `now`."
  [base run policy now]
  {:run run :base-project base :compose-project (compose-project base run)
   :ports (or policy :strip) :isolated? true :started-at now})

(defn run-sections
  "Sorted ids of the active entries of run `run`."
  [active run]
  (vec (sort (for [[id a] active :when (= run (get-in a [:profile :profile/run]))] id))))

(defn ended-runs
  "Runs of `runs` not ended whose sections are all gone from `active`, among
   `candidates` (run ids)."
  [runs active candidates]
  (vec (sort (for [r (distinct candidates)
                   :let [e (get runs r)]
                   :when (and e (not (:ended-at e)) (empty? (run-sections active r)))]
               r))))

;; ---------------------------------------------------------------------------
;; failures

(defn classify
  "{:reason :service? :port?} of a failed `up` of an isolated run, from what
   compose printed (`stderr`)."
  [stderr]
  (let [s (str stderr)
        svc (some->> (re-find #"(?:container|Container) ([A-Za-z0-9_.-]+)" s) second)
        port (some->> (re-find #"(?:0\.0\.0\.0|127\.0\.0\.1|\[::\]|::)?:(\d+)(?: failed|: bind)" s) second parse-long)
        port (or port (some->> (re-find #"port (\d+)" s) second parse-long))]
    (cond-> {:reason (cond
                       (re-find #"(?i)port is already allocated|address already in use" s) :port-in-use
                       (re-find #"(?i)is unhealthy|dependency failed to start" s) :unhealthy
                       (re-find #"(?i)no such image|pull access denied|image .* not found|failed to resolve reference" s) :image-missing
                       (re-find #"(?i)invalid|yaml|validating|interpolat|is missing a value|not supported" s) :config-invalid
                       :else :up-failed)}
      svc (assoc :service svc)
      port (assoc :port port))))

;; ---------------------------------------------------------------------------
;; ports as data

(defn bindings
  "{service {\"<target>/<proto>\" host-port}} of `ps --format json` rows
   (running containers): the host port docker bound to each container port."
  [rows]
  (reduce (fn [acc row]
            (let [svc (get row "Service")]
              (reduce (fn [acc pub]
                        (let [h (get pub "PublishedPort") t (get pub "TargetPort")]
                          (if (and svc (int? h) (pos? h) (int? t))
                            (assoc-in acc [svc (str t "/" (get pub "Protocol" "tcp"))] (long h))
                            acc)))
                      acc (get row "Publishers"))))
          (sorted-map)
          (filter #(= "running" (get % "State")) rows)))

;; ---------------------------------------------------------------------------
;; volumes (Pedro, 2026-11-23: the single exception to "never remove a volume")

(def project-label "com.docker.compose.project")
(def volume-label "com.docker.compose.volume")

(defn removable
  "The volumes of `vols` ([{:name :labels {k v}}], what docker lists) that the
   removal of run `run` may take: ONLY when `runs` (the state's :runs)
   records `run` as an isolated run hive-compose created, and then only the
   volumes compose created for that run's own compose project (label
   com.docker.compose.project equal to it, a compose volume label present)
   whose name carries the run id. Never a volume of the shared project nor
   of any other project, never an external volume (compose puts no label on
   one), never a fixed `name:` without the run. Pure; empty when anything
   does not hold."
  [runs run vols]
  (let [e (get runs run)
        cp (:compose-project e)
        shared (set (for [k (:shared-volumes e)] (str (:base-project e) "_" k)))]
    (if-not (and e (true? (:isolated? e)) (valid-run? run)
                 (= cp (compose-project (:base-project e) run))
                 (not= cp (:base-project e)))
      []
      (vec (sort-by :name
                    (for [v vols
                          :let [ls (:labels v)]
                          :when (= cp (get ls project-label))
                          :when (seq (get ls volume-label))
                          :when (not (shared (:name v)))
                          :when (str/includes? (str (:name v)) run)]
                      v))))))

(defn stale-volumes
  "[{:run :name}] of every ended run's volumes still present: `listed`
   {run [{:name :labels}]} of the runs, kept by `removable`."
  [runs listed]
  (vec (for [[run e] (sort-by key runs)
             :when (:ended-at e)
             v (removable runs run (get listed run))]
         {:run run :name (:name v)})))
