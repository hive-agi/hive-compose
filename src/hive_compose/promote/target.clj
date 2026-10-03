(ns hive-compose.promote.target
  "Pure: a target names services or native compose profiles of a configured
   Project; a section is the Profile that runs exactly what it names, closed
   over the compose file's depends_on."
  (:require [clojure.string :as str]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn parse
  "{:project id-or-nil :names [name ..]} of \"sisf-web\", \"sisf/sisf-web\" or
   \"sisf/sisf-web,sisf-crm\" (\"+\" separates too)."
  [s]
  (let [[a b] (str/split (str/trim (or s "")) #"/" 2)
        [project names] (if b [a b] [nil a])]
    {:project (when-not (str/blank? project) project)
     :names (vec (distinct (remove str/blank? (map str/trim (str/split (or names "") #"[,+]")))))}))

(defn section-id
  "`<project>/<name>[+<name>..]`, names sorted so one section has one id."
  [project-id names]
  (str project-id "/" (str/join "+" (sort names))))

(defn project-profile
  "Profile addressing the whole of `project`, with no service subset."
  [project]
  (cond-> {:profile/id (:project/id project)
           :profile/dir (:project/dir project)}
    (:project/files project) (assoc :profile/files (:project/files project))
    (:project/name project) (assoc :profile/project (:project/name project))
    (seq (:project/programs project)) (assoc :profile/programs (vec (:project/programs project)))
    (:project/env project) (assoc :profile/env (:project/env project))
    (some? (:project/wait? project)) (assoc :profile/wait? (:project/wait? project))
    (:project/ttl-minutes project) (assoc :profile/ttl-minutes (:project/ttl-minutes project))
    (:project/idle-action project) (assoc :profile/idle-action (:project/idle-action project))))

(defn section
  "The Profile of the section of `project` that `names` resolved to `services`."
  [project names services]
  (assoc (project-profile project)
         :profile/id (section-id (:project/id project) names)
         :profile/services (vec (distinct services))))

(defn profile-members
  "Services a native compose profile adds over the always-on ones."
  [with-profile always-on]
  (vec (sort (remove (set always-on) with-profile))))

(defn unknown-service?
  "True when `res` is compose refusing a service name it does not define."
  [res]
  (and (r/err? res) (str/includes? (str (:stderr res)) "no such service")))
