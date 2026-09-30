(ns hive-compose.schema
  "Value objects: Profile, Active entry, Settings, Release step."
  (:require [malli.core :as m]
            [malli.error :as me]))

;; SPDX-License-Identifier: MIT

(def ProfileId
  [:re #"^[a-zA-Z0-9][a-zA-Z0-9_.-]*$"])

(def IdleAction
  "What the reaper does to an idle profile. :stop keeps containers, :down removes
   them (never volumes), :none exempts the profile."
  [:enum :stop :down :none])

(def Profile
  [:map
   [:profile/id ProfileId]
   [:profile/dir [:string {:min 1}]]
   [:profile/files {:optional true} [:vector [:string {:min 1}]]]
   [:profile/project {:optional true} [:re #"^[a-z0-9][a-z0-9_-]*$"]]
   [:profile/compose-profiles {:optional true} [:vector ProfileId]]
   [:profile/services {:optional true} [:vector [:string {:min 1}]]]
   [:profile/env {:optional true} [:map-of :string :string]]
   [:profile/wait? {:optional true} :boolean]
   [:profile/ttl-minutes {:optional true} [:int {:min 1}]]
   [:profile/idle-action {:optional true} IdleAction]
   [:profile/description {:optional true} :string]])

(def Active
  "A profile this addon brought up (or adopted) and still answers for. `:profile`
   is the Profile it ran with, so it can be released after leaving the config."
  [:map
   [:profile/id ProfileId]
   [:project :string]
   [:services [:vector :string]]
   [:started-at :int]
   [:last-touch :int]
   [:profile {:optional true} Profile]])

(def ActiveMap [:map-of :string Active])

(def Settings
  [:map
   [:compose/profiles [:map-of :string Profile]]
   [:compose/tick-seconds [:int {:min 5}]]
   [:compose/default-ttl-minutes [:int {:min 1}]]
   [:compose/default-idle-action IdleAction]
   [:compose/state-file [:string {:min 1}]]
   [:compose/timeout-ms [:int {:min 1000}]]
   [:compose/up-timeout-ms [:int {:min 1000}]]
   [:compose/adopt? :boolean]])

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
