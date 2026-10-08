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
     :compose/adopt?               adopt running stacks of configured profiles (default true)"
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
   :compose/programs? true})

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

(defn normalize-profile
  "Profile with a string id, its dir expanded against `home` and its programs
   resolved against that dir."
  [home p]
  (let [p (cond-> p
            (:profile/id p) (update :profile/id name)
            (:profile/dir p) (update :profile/dir #(expand-home home %)))]
    (cond-> p
      (:profile/programs p) (update :profile/programs #(normalize-programs home (:profile/dir p) %)))))

(defn normalize-project
  "Project with a string id, its dir expanded against `home` and its programs
   resolved against that dir."
  [home p]
  (let [p (cond-> p
            (:project/id p) (update :project/id name)
            (:project/dir p) (update :project/dir #(expand-home home %)))]
    (cond-> p
      (:project/programs p) (update :project/programs #(normalize-programs home (:project/dir p) %)))))

(defn profiles-file
  "Path of the profiles file `cfg` names, expanded against `home`."
  [cfg home]
  (expand-home home (or (:compose/profiles-file cfg) default-profiles-file)))

(defn settings
  "Result<Settings> from merged addon config `cfg`, the profiles read from the
   profiles file (`file-profiles`, nil when absent) and `home`. Inline profiles
   override file profiles with the same id."
  [cfg file-profiles home]
  (let [profiles (map #(normalize-profile home %)
                      (concat (as-seq :profile/id file-profiles)
                              (as-seq :profile/id (:compose/profiles cfg))))
        projects (map #(normalize-project home %) (as-seq :project/id (:compose/projects cfg)))
        s (merge defaults
                 (select-keys cfg (keys defaults))
                 {:compose/state-file (expand-home home (or (:compose/state-file cfg) default-state-file))
                  :compose/log-dir (expand-home home (or (:compose/log-dir cfg) default-log-dir))
                  :compose/projects (into {} (map (juxt :project/id identity)) projects)
                  :compose/profiles (into {} (map (juxt :profile/id identity)) profiles)})]
    (if-let [problems (schema/explain schema/Settings s)]
      (r/err :compose/invalid-config {:problems problems})
      (r/ok s))))
