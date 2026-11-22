(ns hive-compose.adapter.exec
  "hc-9 data commands at the boundary: `port/IDataExec` on the docker CLI
   engine (`docker exec` into a running container of a stack instance) and
   `port/ICommandRunner` on the host runner (a host command run to
   completion). Both extend the existing records, so their constructors and
   namespaces stay as they are; requiring this ns is what extends them.

   Secret references in a command's env are read at spawn through the
   record's resolver and passed in the process environment only: `docker
   exec` gets `-e VAR` by NAME, so no value is ever in an argv, and what the
   process prints is scrubbed of resolved values."
  (:require [hive-compose.adapter.cli :as cli]
            [hive-compose.adapter.host]
            [hive-compose.adapter.secrets :as secrets]
            [hive-compose.port :as port]
            [hive-compose.promote.env :as env]
            [hive-dsl.result :as r]
            [hive-compose.promote.data :as data])
  (:import (hive_compose.adapter.cli CliEngine)
           (hive_compose.adapter.host HostRunner)))

;; SPDX-License-Identifier: MIT

(def ^:private max-output-chars 20000)

(defn- clip [s]
  (let [s (or s "")]
    (if (> (count s) max-output-chars) (subs s (- (count s) max-output-chars)) s)))

(defn running-container-argv
  "The RUNNING containers of `service` in compose project `project`, one name
   a line. Loads no compose file."
  [project service]
  ["docker" "ps"
   "--filter" (str "label=com.docker.compose.project=" project)
   "--filter" (str "label=com.docker.compose.service=" service)
   "--filter" "status=running"
   "--format" "{{.Names}}"])

(defn exec-argv
  "`docker exec` of `argv` in `container`, each of `env-names` passed by name
   (`-e VAR`, its value read from the docker process's environment)."
  [container env-names argv]
  (-> ["docker" "exec"]
      (into (mapcat (fn [k] ["-e" k])) (sort env-names))
      (conj container)
      (into argv)))

(defn- finish
  "Result<{:exit 0 :out}> of an exec Result, failures as :compose/command-failed."
  [res argv scrub]
  (r/bind res
          (fn [{:keys [exit stdout stderr]}]
            (if (zero? exit)
              (r/ok {:exit 0 :out (clip (scrub stdout))})
              (r/err :compose/command-failed {:argv argv :exit exit :stderr (clip (scrub stderr))})))))

(defn- run-with-env
  "Result of `argv` through `exec` with `declared` env read through
   `resolver`; output scrubbed of resolved values. An argv that could remove
   a volume is refused here too (defence in depth beside the plan's check)."
  [exec resolver argv declared opts]
  (r/let-ok [_ (if (data/removes-volumes? argv)
                 (r/err :compose/volume-removal-refused {:argv argv})
                 (r/ok argv))
             resolved (secrets/resolve-env resolver (or declared {}))]
    (finish (exec argv (cond-> (select-keys opts [:timeout-ms :dir])
                         (seq resolved) (assoc :env resolved)))
            argv
            #(env/scrub declared resolved (or % "")))))

(extend-type CliEngine
  port/IDataExec
  (-exec! [{:keys [exec resolver timeout-ms]} {:keys [project service]} argv opts]
    (r/let-ok [names (r/bind (exec (running-container-argv project service) {:timeout-ms timeout-ms})
                             (fn [{:keys [exit stdout stderr]}]
                               (if (zero? exit)
                                 (r/ok (cli/lines stdout))
                                 (r/err :compose/command-failed {:exit exit :stderr (clip stderr)}))))]
      (if-let [c (first (sort names))]
        (run-with-env exec resolver (exec-argv c (keys (:env opts)) argv) (:env opts)
                      {:timeout-ms (or (:timeout-ms opts) timeout-ms)})
        (r/err :compose/no-container {:project project :service service})))))

(extend-type HostRunner
  port/ICommandRunner
  (-run-command! [{:keys [exec resolver]} argv opts]
    (run-with-env exec resolver (vec argv) (:env opts)
                  (cond-> {:timeout-ms (or (:timeout-ms opts) 600000)}
                    (:dir opts) (assoc :dir (:dir opts))))))
