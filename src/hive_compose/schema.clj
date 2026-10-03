(ns hive-compose.schema
  "Value objects: Profile, Active entry, Settings, Release step."
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
   [:program/env {:optional true} [:map-of :string :string]]
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

(def Profile
  [:map
   [:profile/id ProfileId]
   [:profile/dir [:string {:min 1}]]
   [:profile/files {:optional true} [:vector [:string {:min 1}]]]
   [:profile/project {:optional true} [:re #"^[a-z0-9][a-z0-9_-]*$"]]
   [:profile/compose-profiles {:optional true} [:vector ProfileId]]
   [:profile/services {:optional true} [:vector [:string {:min 1}]]]
   [:profile/programs {:optional true} [:vector Program]]
   [:profile/env {:optional true} [:map-of :string :string]]
   [:profile/wait? {:optional true} :boolean]
   [:profile/ttl-minutes {:optional true} [:int {:min 1}]]
   [:profile/idle-action {:optional true} IdleAction]
   [:profile/description {:optional true} :string]])

(def Active
  "A profile this addon brought up (or adopted) and still answers for. `:profile`
   is the Profile it ran with, so it can be released after leaving the config.
   `:programs` are the host programs it holds, by program id."
  [:map
   [:profile/id ProfileId]
   [:project :string]
   [:services [:vector :string]]
   [:started-at :int]
   [:last-touch :int]
   [:programs {:optional true} [:map-of :string Running]]
   [:profile {:optional true} Profile]])

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
   [:project/env {:optional true} [:map-of :string :string]]
   [:project/wait? {:optional true} :boolean]
   [:project/ttl-minutes {:optional true} [:int {:min 1}]]
   [:project/idle-action {:optional true} IdleAction]])

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
   [:compose/programs? :boolean]])

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
