(ns hive-compose.promote.env
  "Pure: an environment is {name value}, a value either a string or a secret
   reference, a one-entry map naming its scheme: `{:pass \"<store path>\"}`.

   A reference is read from its store only when a process is spawned (see
   `hive-compose.adapter.secrets`). Everything else, a compose query, an error,
   a rendered surface, sees it as its rendering `<scheme>:<path>`, never the
   value it stands for."
  (:require [clojure.string :as str]))

;; SPDX-License-Identifier: MIT

(defn ref?
  "True when `v` is a secret reference: a one-entry map of a keyword scheme to
   a string."
  [v]
  (and (map? v)
       (= 1 (count v))
       (keyword? (key (first v)))
       (string? (val (first v)))))

(defn scheme
  "The scheme of reference `ref`: :pass."
  [ref]
  (key (first ref)))

(defn path
  "Where in its store reference `ref` points."
  [ref]
  (val (first ref)))

(defn render
  "`ref` as `<scheme>:<path>`, the only form a reference is shown in."
  [ref]
  (str (name (scheme ref)) ":" (path ref)))

(defn rendered
  "`env` with every reference replaced by its rendering. What a process that
   creates nothing (a compose query, stop, down) is given: interpolation still
   finds every variable, and no secret is read."
  [env]
  (into {} (map (fn [[k v]] [k (if (ref? v) (render v) v)])) env))

(defn refs
  "{name ref} of the references in `env`."
  [env]
  (into {} (filter (comp ref? val)) env))

(defn scrub
  "`s` with every value a reference of `env` resolved to (`resolved` is the
   spawned environment) replaced by the reference's rendering, so a process
   echoing its environment does not carry a secret into a result."
  [env resolved s]
  (reduce (fn [s [k ref]]
            (let [v (get resolved k)]
              (if (str/blank? v) s (str/replace s v (render ref)))))
          (or s "")
          (refs env)))

(defn from-param
  "An env value as sent over MCP: an object {\"pass\" path} is a reference,
   anything else its string."
  [v]
  (if (map? v)
    (into {} (map (fn [[k x]] [(keyword (name k)) (str x)])) v)
    (str v)))
