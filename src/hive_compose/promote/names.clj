(ns hive-compose.promote.names
  "Pure naming of what a stack run creates: compose project names of private
   instances, view network names, tenancy prefixes. One place, so the stack
   plan (hc-4), the wiring (hc-5) and the ops (hc-6) always agree."
  (:require [clojure.string :as str]))

;; SPDX-License-Identifier: MIT

(defn slug
  "`s` as a compose/docker name fragment: lowercase, every run of characters
   outside [a-z0-9] turned into one '-', no leading or trailing '-'.
   \"billing/API\" -> \"billing-api\", \"suite:smoke\" -> \"suite-smoke\"."
  [s]
  (-> (str/lower-case (str s))
      (str/replace #"[^a-z0-9]+" "-")
      (str/replace #"^-+|-+$" "")))

(defn private-project
  "Compose project name of a private instance of `provider` for `consumer` in
   `run`: `<provider>-<consumer>-<run>`, slugged."
  [provider consumer run]
  (str (slug provider) "-" (slug consumer) "-" (slug run)))

(defn view-network
  "The view network of `consumer` in `run`: `hive-<run>-<consumer>`. The only
   network the consumer attaches to; every instance it resolved joins it under
   its original service names."
  [run consumer]
  (str "hive-" (slug run) "-" (slug consumer)))

(defn tenancy-prefix
  "Namespace prefix of `consumer` in `run` for topics, schemas and buckets on a
   shared instance: `<run>.<consumer>.`."
  [run consumer]
  (str (slug run) "." (slug consumer) "."))
