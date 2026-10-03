(ns hive-compose.adapter.cli
  "IComposeEngine over the docker compose CLI. Argv construction and output
   parsing are pure; the process runs through an injected `exec` with
   hive-system's `shell.core/exec!` contract (argv, opts) -> Result."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [hive-compose.port :as port]
            [hive-compose.promote.profile :as profile]
            [hive-dsl.result :as r]
            [hive-system.shell.core :as shell]))

;; SPDX-License-Identifier: MIT

(def ^:private max-output-chars 20000)

(defn- clip [s]
  (let [s (or s "")]
    (if (> (count s) max-output-chars) (subs s (- (count s) max-output-chars)) s)))

;; ---------------------------------------------------------------------------
;; argv

(defn base-argv
  "`docker compose` addressed at profile `p`: project dir, project name, files
   and native compose profiles."
  [p]
  (-> ["docker" "compose" "--project-directory" (:profile/dir p) "-p" (profile/project p)]
      (into (mapcat (fn [f] ["-f" f])) (:profile/files p))
      (into (mapcat (fn [cp] ["--profile" cp])) (:profile/compose-profiles p))))

(defn closure-argv [p]
  (-> (base-argv p) (into ["config" "--format" "json"]) (into (:profile/services p))))

(defn ps-argv [p]
  (into (base-argv p) ["ps" "--format" "json"]))

(defn listed-argv [p]
  (into (base-argv p) ["config" "--services"]))

(defn profiles-argv [p]
  (into (base-argv p) ["config" "--profiles"]))

(defn lines
  "Non-blank trimmed lines of `s`."
  [s]
  (vec (remove str/blank? (map str/trim (str/split-lines (or s ""))))))

(defn up-argv [p services]
  (-> (base-argv p)
      (into ["up" "-d"])
      (cond-> (:profile/wait? p) (conj "--wait"))
      (cond-> (:profile/no-deps? p) (conj "--no-deps"))
      (into services)))

(defn stop-argv [p services]
  (-> (base-argv p) (conj "stop") (into services)))

(defn down-argv [p services]
  (-> (base-argv p) (conj "down") (into services)))

(defn logs-argv [p services tail]
  (-> (base-argv p) (into ["logs" "--no-color" "--tail" (str tail)]) (into services)))

(def ls-argv ["docker" "compose" "ls" "--all" "--format" "json"])

(def version-argv ["docker" "compose" "version" "--short"])

;; ---------------------------------------------------------------------------
;; parsing

(defn parse-json-rows
  "Rows of compose `--format json` output, which is a JSON array or one JSON
   object per line depending on the compose version."
  [s]
  (let [t (str/trim (or s ""))]
    (cond
      (str/blank? t) []
      (str/starts-with? t "[") (vec (json/read-str t))
      :else (mapv json/read-str (remove str/blank? (str/split-lines t))))))

(defn running-services
  "Sorted set of services with a running container in `ps` rows."
  [rows]
  (into (sorted-set)
        (comp (filter #(= "running" (get % "State")))
              (keep #(get % "Service")))
        rows))

(defn closure-services
  "Sorted services of a `config --format json` document."
  [s]
  (vec (sort (keys (get (json/read-str s) "services")))))

(defn projects
  "Project summaries of `ls` rows."
  [rows]
  (mapv (fn [m] {:name (get m "Name")
                 :status (get m "Status")
                 :config-files (get m "ConfigFiles")})
        rows))

;; ---------------------------------------------------------------------------
;; boundary

(defn- run [exec p argv timeout-ms]
  (r/bind (exec argv (cond-> {:timeout-ms timeout-ms}
                       (:profile/dir p) (assoc :dir (:profile/dir p))
                       (seq (:profile/env p)) (assoc :env (:profile/env p))))
          (fn [{:keys [exit stdout stderr]}]
            (if (zero? exit)
              (r/ok (or stdout ""))
              (r/err :compose/command-failed {:argv argv :exit exit :stderr (clip stderr)})))))

(defn- parsed [res category f]
  (r/bind res (fn [out] (r/try-effect* category (f out)))))

(defn- act [exec p argv timeout-ms services]
  (if (empty? services)
    (r/ok {:out "" :skipped :no-services})
    (r/map-ok (run exec p argv timeout-ms) (fn [out] {:out (clip out)}))))

(defrecord CliEngine [exec timeout-ms up-timeout-ms]
  port/IComposeEngine
  (-version [_]
    (r/map-ok (run exec nil version-argv timeout-ms) str/trim))
  (-closure [_ p]
    (parsed (run exec p (closure-argv p) timeout-ms) :compose/unparseable closure-services))
  (-listed [_ p]
    (r/map-ok (run exec p (listed-argv p) timeout-ms) lines))
  (-profiles [_ p]
    (r/map-ok (run exec p (profiles-argv p) timeout-ms) (comp set lines)))
  (-running [_ p]
    (parsed (run exec p (ps-argv p) timeout-ms) :compose/unparseable
            (comp running-services parse-json-rows)))
  (-projects [_]
    (parsed (run exec nil ls-argv timeout-ms) :compose/unparseable
            (comp projects parse-json-rows)))
  (-up! [_ p services]
    (act exec p (up-argv p services) up-timeout-ms services))
  (-stop! [_ p services]
    (act exec p (stop-argv p services) timeout-ms services))
  (-down! [_ p services]
    (act exec p (down-argv p services) timeout-ms services))
  (-logs [_ p services tail]
    (r/map-ok (run exec p (logs-argv p services tail) timeout-ms) (fn [out] {:out (clip out)}))))

(defn make-engine
  "CliEngine. `opts`: :exec (default hive-system `shell.core/exec!`),
   :compose/timeout-ms, :compose/up-timeout-ms."
  [opts]
  (->CliEngine (or (:exec opts) shell/exec!)
               (or (:compose/timeout-ms opts) 60000)
               (or (:compose/up-timeout-ms opts) 600000)))
