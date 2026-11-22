(ns hive-compose.promote.names
  "Naming of what a stack run creates, in one place so the stack plan (hc-4),
   the wiring (hc-5) and the ops (hc-6) always agree. The Docker name builders
   live in hive-compose.schema (slug, view-network, private-project); this ns
   delegates to them, so there is one implementation, and adds the tenancy
   prefix."
  (:require [hive-compose.schema :as schema]))

;; SPDX-License-Identifier: MIT

(defn slug
  "`s` as a Docker name part (schema/slug)."
  [s]
  (schema/slug s))

(defn private-project
  "Compose project of a private instance of the provider whose compose project
   name is `provider-project`, for `consumer` in `run` (schema/private-project):
   `<provider-project>-<slug consumer>-<run>`."
  [provider-project consumer run]
  (schema/private-project provider-project consumer run))

(defn view-network
  "The view network of `consumer` in `run` (schema/view-network):
   `hive-<run>-<slug consumer>`. The only network outside its own compose
   project the consumer attaches to."
  [run consumer]
  (schema/view-network run consumer))

(defn tenancy-prefix
  "Namespace prefix of `consumer` in `run` for topics, schemas and buckets on a
   shared instance: `<run>.<slug consumer>.`."
  [run consumer]
  (str (slug run) "." (slug consumer) "."))

(defn run-tenancy-prefix
  "Namespace prefix shared by every consumer of `run`, for a provider whose
   `:tenancy` has `:scope :run` (a producer and a consumer of one topic):
   `<run>.`. Runs on a shared instance stay isolated."
  [run]
  (str (slug run) "."))

(defn tenancy-prefix-for
  "The prefix `consumer` in `run` receives from a provider with `tenancy`
   ({:env VAR :scope :consumer|:run}, the scope a keyword or a string): the
   run's when `:scope` is :run, the consumer's otherwise (the default)."
  [tenancy run consumer]
  (if (= "run" (some-> (:scope tenancy) name))
    (run-tenancy-prefix run)
    (tenancy-prefix run consumer)))
