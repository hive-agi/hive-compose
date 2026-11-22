(ns hive-compose.adapter.runner
  "ISuiteRunner over docker and the host (hc-8). Argv construction is pure;
   processes run through an injected `exec` with hive-system's
   `shell.core/exec!` contract (argv, opts) -> Result.

   A container runs as `docker create` (name, view network, reports mount,
   labels, `-e VAR` by NAME only), `docker network connect` to any extra
   network, `docker start -a`, then `docker rm -f`, which also runs after a
   timeout or a failed start. Values never reach argv: the env of the
   docker process carries them, and docker copies the named ones in.

   A host command runs in its `:dir` with its env, as `sh -c` for a string,
   as is for a vector. Either way, secret references in the env are read
   here, at spawn, through the injected resolver (`hive-compose.adapter.secrets`),
   and every value read is scrubbed from the log tail."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [hive-compose.adapter.cli :as cli]
            [hive-compose.adapter.secrets :as secrets]
            [hive-compose.port :as port]
            [hive-compose.promote.env :as env]
            [hive-dsl.result :as r]
            [hive-system.shell.core :as shell]))

;; SPDX-License-Identifier: MIT

(def default-timeout-ms
  "How long a runner may take when its suite sets no :suite/timeout-ms."
  (* 30 60 1000))

(def tail-lines 50)

;; ---------------------------------------------------------------------------
;; argv (pure)

(defn env-args
  "`-e VAR` for every variable of `env`, sorted: names only."
  [env]
  (vec (mapcat (fn [k] ["-e" k]) (sort (keys env)))))

(defn mount-args [mounts]
  (vec (mapcat (fn [{:keys [host container]}] ["-v" (str host ":" container)]) mounts)))

(defn create-argv
  "`docker create` of container RunnerSpec `spec`, on its view network."
  [spec]
  (-> ["docker" "create" "--name" (:name spec) "--network" (:network spec)]
      (into (cli/label-args (:labels spec)))
      (into (mount-args (:mounts spec)))
      (into (env-args (:env spec)))
      (conj (:image spec))
      (into (let [c (:command spec)] (cond (nil? c) [] (string? c) ["sh" "-c" c] :else c)))))

(defn connect-argv [network container]
  ["docker" "network" "connect" network container])

(defn start-argv [container]
  ["docker" "start" "-a" container])

(defn rm-argv [container]
  ["docker" "rm" "-f" container])

(defn command-argv
  "The argv of a host command: `sh -c` for a string, the vector as is."
  [command]
  (if (string? command) ["sh" "-c" command] (vec command)))

(defn log-tail
  "The last `n` non-blank lines of `s`."
  [s n]
  (vec (take-last n (remove str/blank? (str/split-lines (or s ""))))))

;; ---------------------------------------------------------------------------
;; boundary

(defn- timeout-ms [spec] (or (:timeout-ms spec) default-timeout-ms))

(defn- outcome
  "The runner answer of a finished exec Result `res`: its exit and tail, both
   streams scrubbed of every resolved secret, and `:scrub` (fn [s]), which
   does the same to any text the caller reads afterwards (the reports), so
   no resolved value ever leaves the adapter. A timeout or a process that
   could not start is an error carrying :exit nil."
  [res declared resolved started ended]
  (let [scrub #(env/scrub declared resolved %)]
    (if (r/ok? res)
      (let [{:keys [exit stdout stderr]} (:ok res)]
        (r/ok {:exit exit :started-ms started :ended-ms ended :scrub scrub
               :log-tail (log-tail (scrub (str stdout "\n" stderr)) tail-lines)}))
      (assoc (select-keys res [:error :timeout-ms])
             :exit nil :started-ms started :ended-ms ended :scrub scrub
             :message (scrub (str (or (:message res) (name (:error res)))))))))

(defn- ensure-dir! [dir]
  (r/try-effect* :compose/reports-unwritable (do (.mkdirs (io/file dir)) dir)))

(defn- step!
  "Result<stdout> of a docker step that must succeed."
  [exec argv opts]
  (r/bind (exec argv (merge {:timeout-ms 60000} opts))
          (fn [{:keys [exit stdout stderr]}]
            (if (zero? exit)
              (r/ok stdout)
              (r/err :compose/command-failed {:argv argv :exit exit :stderr (str stderr)})))))

(defrecord SuiteRunner [exec resolver now]
  port/ISuiteRunner
  (-run-container! [_ spec]
    (let [declared (:env spec)
          nm (:name spec)]
      (r/let-ok [_ (ensure-dir! (:reports-dir spec))
                 resolved (secrets/resolve-env resolver declared)]
        ;; everything from the first docker call on sits inside try/finally:
        ;; a create that fails or times out half way still has its container removed
        (try
          ;; a leftover of an interrupted run under this name: best effort
          (exec (rm-argv nm) {:timeout-ms 60000})
          (r/let-ok [_ (r/map-err (step! exec (create-argv spec) {:env resolved})
                                  #(update % :stderr (fn [s] (env/scrub declared resolved s))))
                     _ (reduce (fn [acc n] (if (r/err? acc) (reduced acc) (step! exec (connect-argv n nm) {})))
                               (r/ok nil) (:networks spec))]
            (let [t0 (now)
                  res (exec (start-argv nm) {:timeout-ms (timeout-ms spec)})]
              (outcome res declared resolved t0 (now))))
          (catch Throwable t
            (r/err :compose/runner-threw {:exit nil :message (env/scrub declared resolved (str t))}))
          (finally
            (try (exec (rm-argv nm) {:timeout-ms 60000})
                 (catch Throwable _best-effort-removal nil)))))))
  (-run-command! [_ spec]
    (let [declared (:env spec)]
      (r/let-ok [_ (ensure-dir! (:reports-dir spec))
                 resolved (secrets/resolve-env resolver declared)]
        (try
          (let [t0 (now)
                res (exec (command-argv (:command spec))
                          (cond-> {:timeout-ms (timeout-ms spec) :env resolved}
                            (:dir spec) (assoc :dir (:dir spec))))]
            (outcome res declared resolved t0 (now)))
          (catch Throwable t
            (r/err :compose/runner-threw {:exit nil :message (env/scrub declared resolved (str t))})))))))

(defn make-runner
  "SuiteRunner. `opts`: :exec (default hive-system `shell.core/exec!`),
   :compose/resolver ({scheme ISecretBackend}, default `pass` over that
   exec), :now (0-arity epoch ms)."
  [opts]
  (let [exec (or (:exec opts) shell/exec!)]
    (->SuiteRunner exec
                   (or (:compose/resolver opts) (secrets/make-resolver {:exec exec}))
                   (or (:now opts) #(System/currentTimeMillis)))))
