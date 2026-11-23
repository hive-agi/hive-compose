(ns hive-compose.promote.config
  "Addon config -> Settings. Pure: the profiles file's contents and the home
   directory arrive as values.

   Config keys (manifest `:addon/config`, then config.edn `:addons {\"hive.compose\" {..}}`):
     :compose/profiles             [Profile ..] or {id Profile}, inline
     :compose/profiles-file        EDN file of the same shape
                                   (default ~/.config/hive-mcp/compose-profiles.edn)
     :compose/projects             {id Project}: compose projects whose services
                                   and native profiles are addressable as targets
                                   (`<project>/<target>`)
     :compose/default-ttl-minutes  idle minutes before the reaper acts (default 60)
     :compose/default-idle-action  :stop | :down | :none (default :stop)
     :compose/tick-seconds         reaper period (default 60)
     :compose/state-file           default ~/.local/state/hive-compose/state.edn
     :compose/timeout-ms           per compose call (default 60000)
     :compose/up-timeout-ms        per `up`, pulls included (default 600000)
     :compose/adopt?               adopt running stacks of configured profiles (default true)
     :compose/swarm?               count agents working in a project's directories as
                                   use of its sections (default true)
     :compose/notify?              shout nREPL routes and reaps to the swarm (default true)
     :compose/activity             #{:nrepl :published}: which new connections count as
                                   use; :published (the host ports containers publish:
                                   a browser, any host-side client) is opt-in (default
                                   #{:nrepl}). A project overrides it for its sections
                                   with :project/activity, a preset with
                                   :profile/activity. Connections from the project's
                                   own containers never count. :cpu (opt-in too)
                                   counts a running service busier than
                                   :compose/cpu-busy-percent, client or not
     :compose/cpu-busy-percent     CPU use, in percent of one core, above which a
                                   service of a :cpu section is busy (default 5)
     :compose/claim-ttl-minutes    a claim not renewed for this long ends (default 30)
     :compose/claim-max-ttl-minutes the longest wave claim (claim ttl_minutes=..) an
                                   agent or coordinator may hold; a longer request
                                   is refused, never clamped (default 480)
     :compose/orphan-ttl-minutes   idle minutes before the reaper acts on a section
                                   whose last claim ended, when shorter than its TTL
                                   (default 10)
     :compose/program-grace-ms     how long a host program is watched after its
                                   start; one that exits within it is reported
                                   :exited with its code and log tail (default 3000)
     :compose/history-file         flake history, EDN lines (default
                                   flake-history.ednl beside the state file)
     :compose/flaky                flake thresholds {:window :min-runs
                                   :flip-rate :streak}, each optional (defaults
                                   in `hive-compose.promote.flaky/defaults`)
     :compose/stacks               [Stack ..] or {id Stack}: consumers brought up with
                                   every capability they need (optional)
     :compose/suites               [Suite ..] or {id Suite}: test runners in any
                                   language (optional)
   A project places itself in the capability graph with :project/provides
   {capability Provide} and :project/needs {capability Need} (see schema)."
  (:require [clojure.string :as str]
            [hive-compose.schema :as schema]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def defaults
  {:compose/tick-seconds 60
   :compose/default-ttl-minutes 60
   :compose/default-idle-action :stop
   :compose/timeout-ms 60000
   :compose/up-timeout-ms 600000
   :compose/adopt? true
   :compose/programs? true
   :compose/swarm? true
   :compose/notify? true
   :compose/activity #{:nrepl}
   :compose/cpu-busy-percent 5
   :compose/claim-ttl-minutes 30
   :compose/claim-max-ttl-minutes 480
   :compose/orphan-ttl-minutes 10
   :compose/program-grace-ms 3000})

(def default-profiles-file "~/.config/hive-mcp/compose-profiles.edn")
(def default-state-file "~/.local/state/hive-compose/state.edn")

(defn expand-home
  "`s` with a leading ~ replaced by `home`."
  [home s]
  (if (and home (string? s) (str/starts-with? s "~"))
    (str home (subs s 1))
    s))

(defn- as-seq [id-key coll]
  (if (map? coll)
    (map (fn [[k v]] (update v id-key #(or % (name k)))) coll)
    coll))

(def default-log-dir "~/.local/state/hive-compose/logs")

(defn under
  "`dir` as an absolute path: itself when absolute or under ~, else resolved
   against `base`."
  [home base dir]
  (cond
    (not (string? dir)) dir
    (str/starts-with? dir "~") (expand-home home dir)
    (str/starts-with? dir "/") dir
    (str/ends-with? base "/") (str base dir)
    :else (str base "/" dir)))

(defn normalize-programs
  "Programs with string ids and their dirs made absolute against `base`, the
   directory of the project or profile that declares them."
  [home base programs]
  (mapv (fn [p]
          (cond-> p
            (:program/id p) (update :program/id name)
            (:program/dir p) (update :program/dir #(under home base %))))
        (as-seq :program/id programs)))

(defn activity-set
  "A configured activity (a set, or a list of keywords or names, as JSON
   gives) as a set of keywords; a value that is no collection stays as is,
   for the schema to refuse."
  [v]
  (if (coll? v) (into #{} (map keyword) v) v))

(defn capability-keys
  "`m` ({capability v}) with its capability names as strings, as JSON and EDN
   keyword keys give them; a key that is no name stays as is, for the schema
   to refuse."
  [m]
  (into {} (map (fn [[k v]] [(if (ident? k) (name k) k) v])) m))

(defn normalize-resources
  "Resources as JSON or EDN give them: \"unbounded\" (or the keyword) as
   :unbounded, anything else as is, for the schema to judge."
  [v]
  (if (and (or (string? v) (keyword? v)) (= "unbounded" (name v))) :unbounded v))

(defn- normalize-in
  "`m` with `k` normalized by `f` when `m` is a map holding it."
  [m k f]
  (if (and (map? m) (contains? m k)) (update m k f) m))

(defn normalize-provides
  "Provides ({capability Provide}) with string capability names and
   normalized `:resources`."
  [provides]
  (into {}
        (map (fn [[k p]] [k (normalize-in p :resources normalize-resources)]))
        (capability-keys provides)))

(defn normalize-needs
  "Needs ({capability Need}) with string capability names, each `:via` a
   keyword, as JSON gives it a string, and normalized `:resources`."
  [needs]
  (if (map? needs)
    (into {}
          (map (fn [[k n]]
                 [k (-> n
                        (cond-> (and (map? n) (string? (:via n))) (update :via keyword))
                        (normalize-in :resources normalize-resources))]))
          (capability-keys needs))
    needs))

(defn- keyed-by
  "`xs` ([X] or {id X}) normalized with `f` and keyed by `id-key`, or `xs`
   untouched when it is no collection of maps, for the schema to refuse."
  [id-key f xs]
  (let [xs (if (and (map? xs) (every? map? (vals xs))) (as-seq id-key xs) xs)]
    (if (and (sequential? xs) (every? map? xs))
      (into {} (map (fn [x] (let [x (f x)] [(get x id-key) x]))) xs)
      xs)))

(defn normalize-profile
  "Profile with a string id, its dir expanded against `home` and its programs
   resolved against that dir."
  [home p]
  (let [p (cond-> p
            (:profile/id p) (update :profile/id name)
            (:profile/dir p) (update :profile/dir #(expand-home home %))
            (contains? p :profile/activity) (update :profile/activity activity-set))]
    (cond-> p
      (:profile/programs p) (update :profile/programs #(normalize-programs home (:profile/dir p) %)))))

(defn normalize-project
  "Project with a string id, its dir expanded against `home`, its programs
   resolved against that dir and its capability names as strings."
  [home p]
  (let [p (cond-> p
            (:project/id p) (update :project/id name)
            (:project/dir p) (update :project/dir #(expand-home home %))
            (contains? p :project/activity) (update :project/activity activity-set)
            (map? (:project/provides p)) (update :project/provides normalize-provides)
            (map? (:project/needs p)) (update :project/needs normalize-needs))]
    (cond-> p
      (:project/programs p) (update :project/programs #(normalize-programs home (:project/dir p) %)))))

(defn- id-name [v] (if (ident? v) (name v) v))

(defn normalize-stacks
  "Stacks ([Stack] or {id Stack}) as {id Stack} with string ids and string
   capability names in their needs. A malformed value (no collection of maps,
   a stack without id) is left for the schema to refuse, never thrown on."
  [stacks]
  (keyed-by :stack/id
            (fn [s]
              (cond-> s
                (contains? s :stack/id) (update :stack/id id-name)
                (map? (:stack/needs s)) (update :stack/needs normalize-needs)
                (map? (:stack/resources s)) (update :stack/resources
                                                    #(into {} (map (fn [[k v]] [k (normalize-resources v)]))
                                                           (capability-keys %)))))
            stacks))

(defn normalize-suites
  "Suites ([Suite] or {id Suite}) as {id Suite} with string ids, `:suite/dir`
   expanded against `home`, string capability names in their needs and a
   keyword report format. A malformed value is left for the schema to refuse."
  [home suites]
  (keyed-by :suite/id
            (fn [s]
              (cond-> s
                (contains? s :suite/id) (update :suite/id id-name)
                (string? (:suite/dir s)) (update :suite/dir #(expand-home home %))
                (map? (:suite/needs s)) (update :suite/needs normalize-needs)
                (string? (:suite/report-format s)) (update :suite/report-format keyword)
                (contains? s :suite/resources) (update :suite/resources normalize-resources)))
            suites))


(defn profiles-file
  "Path of the profiles file `cfg` names, expanded against `home`."
  [cfg home]
  (expand-home home (or (:compose/profiles-file cfg) default-profiles-file)))

(defn history-file
  "Path of the flake history file: `:compose/history-file` expanded against
   `home`, else `flake-history.ednl` beside the state file `state-file`."
  [cfg home state-file]
  (if-let [f (:compose/history-file cfg)]
    (expand-home home f)
    (let [parent (.getParent (java.io.File. ^String state-file))]
      (if parent (str parent "/flake-history.ednl") "flake-history.ednl"))))

(defn settings
  "Result<Settings> from merged addon config `cfg`, the profiles read from the
   profiles file (`file-profiles`, nil when absent) and `home`. Inline profiles
   override file profiles with the same id. Stacks and suites appear only when
   configured, so a config without them yields the Settings it always did."
  [cfg file-profiles home]
  (let [profiles (map #(normalize-profile home %)
                      (concat (as-seq :profile/id file-profiles)
                              (as-seq :profile/id (:compose/profiles cfg))))
        projects (map #(normalize-project home %) (as-seq :project/id (:compose/projects cfg)))
        state-file (expand-home home (or (:compose/state-file cfg) default-state-file))
        s (merge defaults
                 (cond-> (select-keys cfg (keys defaults))
                   (contains? cfg :compose/activity)
                   (update :compose/activity activity-set))
                 {:compose/state-file state-file
                  :compose/history-file (history-file cfg home state-file)
                  :compose/log-dir (expand-home home (or (:compose/log-dir cfg) default-log-dir))
                  :compose/projects (into {} (map (juxt :project/id identity)) projects)
                  :compose/profiles (into {} (map (juxt :profile/id identity)) profiles)})
        s (cond-> s
            (some? (:compose/flaky cfg)) (assoc :compose/flaky (:compose/flaky cfg))
            (contains? cfg :compose/stacks) (assoc :compose/stacks (normalize-stacks (:compose/stacks cfg)))
            (contains? cfg :compose/suites) (assoc :compose/suites (normalize-suites home (:compose/suites cfg)))
            (contains? cfg :compose/default-resources)
            (assoc :compose/default-resources (normalize-resources (:compose/default-resources cfg)))
            (contains? cfg :compose/budget) (assoc :compose/budget (:compose/budget cfg))
            (contains? cfg :compose/host-reserve) (assoc :compose/host-reserve (:compose/host-reserve cfg)))]
    (if-let [problems (schema/explain schema/Settings s)]
      (r/err :compose/invalid-config {:problems problems})
      (r/ok s))))

(defn- keyed-diff
  "{:added :removed :changed} ids between maps `old` and `new`, each sorted;
   empty keys left out."
  [old new]
  (let [ks (fn [m] (set (keys m)))
        added (sort (remove (ks old) (keys new)))
        removed (sort (remove (ks new) (keys old)))
        changed (sort (for [[k v] new :when (and (contains? old k) (not= v (get old k)))] k))]
    (cond-> {}
      (seq added) (assoc :added (vec added))
      (seq removed) (assoc :removed (vec removed))
      (seq changed) (assoc :changed (vec changed)))))

(def ^:private live-only
  "Settings a running addon keeps from init: the reaper period and where its
   state lives. A change to them applies at the next initialize!."
  [:compose/tick-seconds :compose/state-file :compose/history-file])

(defn settings-diff
  "What differs between Settings `old` and `new`: projects and profiles by id,
   other settings by key, and among those the ones a reload cannot apply
   (`:restart-required`). Pure."
  [old new]
  (let [others (remove #{:compose/projects :compose/profiles} (distinct (concat (keys old) (keys new))))
        changed (vec (sort (filter #(not= (get old %) (get new %)) others)))
        pinned (vec (filter (set live-only) changed))]
    (cond-> {}
      (seq (keyed-diff (:compose/projects old) (:compose/projects new)))
      (assoc :projects (keyed-diff (:compose/projects old) (:compose/projects new)))
      (seq (keyed-diff (:compose/profiles old) (:compose/profiles new)))
      (assoc :profiles (keyed-diff (:compose/profiles old) (:compose/profiles new)))
      (seq changed) (assoc :settings changed)
      (seq pinned) (assoc :restart-required pinned))))
