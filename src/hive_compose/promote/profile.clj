(ns hive-compose.promote.profile
  "Pure reads over a Profile value and the Settings that default it."
  (:require [clojure.string :as str]))

;; SPDX-License-Identifier: MIT

(defn default-project
  "The project name compose derives from `dir`: its basename, lower-cased,
   reduced to [a-z0-9_-]."
  [dir]
  (-> (str/replace (or dir "") #"/+$" "")
      (str/split #"/")
      last
      (or "")
      str/lower-case
      (str/replace #"[^a-z0-9_-]" "")))

(defn project
  "The compose project name `p` runs under."
  [p]
  (or (:profile/project p) (default-project (:profile/dir p))))

(defn ttl-ms
  "Idle time after which the reaper acts on `p`, in milliseconds."
  [settings p]
  (* 60000 (or (:profile/ttl-minutes p) (:compose/default-ttl-minutes settings))))

(defn idle-action
  "What the reaper does to `p` once idle: :stop, :down or :none."
  [settings p]
  (or (:profile/idle-action p) (:compose/default-idle-action settings)))
