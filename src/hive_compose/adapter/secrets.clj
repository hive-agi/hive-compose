(ns hive-compose.adapter.secrets
  "Secret references read from their stores at spawn. The port is hive-system's
   `ISecretBackend`; a resolver is {scheme backend}, one backend per scheme a
   reference may name. `:pass` is hive-system's PassBackend: `pass <path>`
   through the injected `exec`, its first non-blank line, trimmed.

   A failure names the variable, the reference and the store's reason, never a
   value. A new scheme (`{:env \"VAR\"}`) is a backend of that id here and a map
   in `hive-compose.schema/SecretRef`."
  (:require [clojure.string :as str]
            [hive-compose.promote.env :as env]
            [hive-dsl.result :as r]
            [hive-system.secrets.core :as sec]
            [hive-system.secrets.pass :as pass]
            [hive-system.secrets.protocols :as proto]))

;; SPDX-License-Identifier: MIT

(def ^:private max-detail-chars 500)

(defn make-resolver
  "{scheme ISecretBackend}. `opts`: :exec, the shell `pass` runs through
   (default hive-system `shell.core/exec!`)."
  [opts]
  {:pass (pass/make-pass-backend {:shell (:exec opts)})})

(defn- reason
  "The store's own words for failed fetch `res`: its category and, when it
   printed one, its stderr (pass and gpg never print the entry there)."
  [res]
  (let [detail (str/trim (str (:stderr res)))]
    (cond-> {:reason (:error res)}
      (seq detail) (assoc :detail (subs detail 0 (min max-detail-chars (count detail)))))))

(defn- resolve-ref
  "Result<string>: what variable `var`'s reference `ref` stands for."
  [resolver var ref]
  (let [backend (get resolver (env/scheme ref))
        res (if backend
              (proto/fetch backend (env/path ref) {:line-only? true})
              (r/err :secrets/unknown-backend {}))
        value (when (r/ok? res) (some-> (:ok res) sec/expose str/trim))]
    (cond
      (r/err? res) (r/err :compose/secret-unresolved
                          (merge {:var var :ref (env/render ref)
                                  :hint (str "the store could not answer " (env/render ref)
                                             "; nothing was started")}
                                 (reason res)
                                 (when-not backend {:known (vec (sort (map name (keys resolver))))})))
      (str/blank? value) (r/err :compose/secret-unresolved
                                {:var var :ref (env/render ref) :reason :pass/empty
                                 :hint "the entry is blank; nothing was started"})
      :else (r/ok value))))

(defn resolve-env
  "Result<{name string}>: `env` with every reference read from its store, in
   name order; the first that cannot be read is the error. Plain strings pass
   through untouched, and an env without references touches no store."
  [resolver env]
  (reduce (fn [acc [k v]]
            (if (env/ref? v)
              (let [x (resolve-ref resolver k v)]
                (if (r/err? x) (reduced x) (r/map-ok acc #(assoc % k (:ok x)))))
              (r/map-ok acc #(assoc % k v))))
          (r/ok {})
          (sort-by key env)))
