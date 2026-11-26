(ns hive-compose.adapter.suite-runner
  "ISuiteRunner over the host and docker (hc-8, hc-16b): one suite runner run
   to its end under its resource caps, its OOM kill diagnosed, every secret
   it resolved scrubbed from what it answers.

   A host command runs as
     systemd-run --user --scope --quiet --unit=hc-suite-<test>-<ms>
                 -p MemoryMax=<cap> -p MemorySwapMax=0 -- <command>
   so a runaway is OOM-killed inside its own scope and cannot take the host
   down; the scope's Result (oom-kill) is read after the run and the unit
   reset. Without a systemd user manager the command runs under `ulimit -v`
   and the answer's :cap :note says so.

   A container runs as `docker image inspect`, then `docker pull` when the
   image is absent (under its own `:pull-timeout-ms`, default 10 min, never
   the run's), `docker create` (name, view network, `--mount` binds, labels,
   `-e VAR` by NAME only, `--memory <cap> --memory-swap <cap> --cpus n`),
   `docker network connect` to each of `:extra-networks`, `docker start -a`
   with its output discarded (never drained into this heap) and read back
   through `docker logs --tail`, `docker inspect` (State.OOMKilled,
   ExitCode), then `docker rm -f`, which also runs after a failed step, a
   timeout or a throw. `rm -f` is never `-v`: volumes are never removed, so
   an image that declares a VOLUME leaves its anonymous volume behind, one
   per cell (`docker volume ls -f dangling=true` lists them).

   Env values never reach an argv: secret references in `:env` are read here,
   at spawn, through the injected resolver (`hive-compose.adapter.secrets`),
   and travel in the env of the process (docker copies the named ones in).
   Every answer, ok or error, carries `:scrub` (fn [text]) -> text, which
   replaces every value resolved from a reference by that reference's
   rendering; the output, the OOM finding and every error message are
   already scrubbed.

   Everything outside is injected: `:exec` (hive-system `shell.core/exec!`
   shape: argv, {:dir :env :timeout-ms} -> Result<{:exit :stdout :stderr}>),
   `:now` (epoch ms), `:systemd?` and `:resolver`."
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [hive-compose.adapter.host :as host]
            [hive-compose.adapter.secrets :as secrets]
            [hive-compose.port :as port]
            [hive-compose.promote.env :as env]
            [hive-compose.promote.oom :as oom]
            [hive-dsl.result :as r]
            [hive-system.shell.core :as shell]))

;; SPDX-License-Identifier: MIT

(def ^:private output-lines 50)

(def default-timeout-ms
  "A runner with no :timeout-ms of its own may run this long (1 h)."
  3600000)

(def default-pull-timeout-ms
  "How long a missing image may take to pull."
  (* 10 60 1000))

(defn- last-lines [& texts]
  (->> texts
       (mapcat #(str/split-lines (str %)))
       (remove str/blank?)
       (take-last output-lines)
       vec))

(defn- caps
  "{:mem bytes? :cpus n?} of a spec's :resources (mem as bytes or notation)."
  [spec]
  (let [{:keys [mem cpus]} (:resources spec)]
    (cond-> {}
      (oom/mem-bytes mem) (assoc :mem (oom/mem-bytes mem))
      (and (number? cpus) (pos? cpus)) (assoc :cpus cpus))))

(defn- service-name
  "What the why line names: the test id, else the container name."
  [spec]
  (or (:test-id spec) (:name spec) "suite"))

(defn- ran
  "{:exit :output :timed-out?} of an exec Result, or nil when the command
   could not start."
  [res]
  (cond
    (r/ok? res) {:exit (:exit (:ok res))
                 :output (last-lines (:stdout (:ok res)) (:stderr (:ok res)))
                 :timed-out? false}
    (= :shell/timeout (:error res)) {:exit nil :output [] :timed-out? true}
    :else nil))

(defn- scrub-finding
  "OOM finding `f` with every text scrubbed."
  [f scrub]
  (when f (into {} (map (fn [[k v]] [k (if (string? v) (scrub v) v)])) f)))

(defn- interrupted!
  "Restore the interrupt flag a caught InterruptedException cleared."
  [t]
  (when (instance? InterruptedException t) (.interrupt (Thread/currentThread))))

(defn- threw
  "The error answer of a runner that threw: scrubbed, with :scrub."
  [t scrub]
  (interrupted! t)
  (r/err :compose/runner-threw {:exit nil :scrub scrub :message (scrub (str t))}))

(defn- not-started
  "The error answer of a runner that could not start, from exec Result `res`."
  [spec res what scrub]
  (r/err :compose/runner-failed {:test-id (:test-id spec) :exit nil :scrub scrub
                                 :cause (update (select-keys res [:error :message]) :message #(some-> % str scrub))
                                 :message (scrub (str what " did not start: " (or (:message res) (:error res))))}))

(defn- ensure-dir! [dir]
  (if dir
    (r/try-effect* :compose/reports-unwritable (do (.mkdirs (io/file dir)) dir))
    (r/ok nil)))

;; ---------------------------------------------------------------------------
;; host command

(defn- temp-log []
  (str (java.io.File/createTempFile "hc-suite-" ".log")))

(defn- tail-lines
  "The last non-blank lines of file `log`, read from its last 16 KiB; [] when
   it is absent or unreadable. Never the whole file: a runner's output is
   unbounded."
  [log]
  (let [f (java.io.File. (str log))]
    (if-not (.isFile f)
      []
      (try (with-open [raf (java.io.RandomAccessFile. f "r")]
             (let [len (.length raf)
                   start (max 0 (- len 16384))
                   buf (byte-array (- len start))]
               (.seek raf start)
               (.readFully raf buf)
               (last-lines (String. buf "UTF-8"))))
           (catch java.io.IOException _unreadable-log-has-no-tail [])))))

(defn- scope-result
  "The Result of scope `unit`, then the unit reset; nil when unreadable."
  [exec unit]
  (let [res (exec (oom/scope-result-argv unit) {:timeout-ms 10000})]
    (exec (oom/scope-reset-argv unit) {:timeout-ms 10000})
    (when (r/ok? res) (str/trim (str (:stdout (:ok res)))))))

(defn run-command!
  "Run the host command of `spec` under its caps, its env `resolved`."
  [{:keys [exec now systemd?]} spec resolved scrub]
  (let [{:keys [mem cpus]} (caps spec)
        mode (oom/cap-mode mem systemd?)
        started (now)
        unit (oom/scope-unit (service-name spec) started)
        log (or (:log spec) (temp-log))]
    (try
      (let [argv (oom/capped-argv (oom/logged-command (:command spec) log)
                                  {:mode mode :mem mem :cpus cpus :unit unit})
            res (exec argv (cond-> {:timeout-ms (or (:timeout-ms spec) default-timeout-ms)}
                             (:dir spec) (assoc :dir (:dir spec))
                             (seq resolved) (assoc :env resolved)))
            out (some-> (ran res) (update :output into (tail-lines log)))]
        (if-not out
          (not-started spec res "suite runner" scrub)
          (let [result (when (= :systemd mode) (scope-result exec unit))
                finding (oom/host-finding (service-name spec)
                                          {:mode mode :mem mem :exit (:exit out)
                                           :scope-result result
                                           :output (str/join "\n" (:output out))})]
            (r/ok (cond-> (assoc out
                                 :output (mapv scrub (:output out))
                                 :scrub scrub
                                 :started-at started
                                 :finished-at (now)
                                 :cap (cond-> {:mode mode}
                                        mem (assoc :mem mem)
                                        cpus (assoc :cpus cpus)
                                        (oom/cap-note mode mem) (assoc :note (oom/cap-note mode mem))
                                        (= :systemd mode) (assoc :unit unit)))
                    finding (assoc :oom (scrub-finding finding scrub)))))))
      (catch Throwable t (threw t scrub))
      (finally
        (when-not (:log spec) (.delete (java.io.File. (str log))))))))

;; ---------------------------------------------------------------------------
;; container (argv pure)

(defn env-args
  "`-e VAR` for every variable of `env`, sorted: names only."
  [env]
  (vec (mapcat (fn [k] ["-e" k]) (sort (keys env)))))

(defn mount-args
  "`--mount type=bind,...` per mount: unlike `-v`, a `:` in a path is no separator."
  [mounts]
  (vec (mapcat (fn [{:keys [host container]}]
                 ["--mount" (str "type=bind,source=" host ",target=" container)])
               mounts)))

(defn create-argv
  "The `docker create` argv of container `spec` under `cs` {:mem :cpus}. Not
   --rm: the container is inspected after it ends, then removed."
  [spec cs]
  (-> ["docker" "create" "--name" (:name spec)]
      (cond-> (:network spec) (into ["--network" (:network spec)]))
      (into (mapcat (fn [[k v]] ["--label" (str k "=" v)]) (sort (:labels spec))))
      (into (mount-args (:mounts spec)))
      (into (env-args (:env spec)))
      (into (oom/container-cap-args cs))
      (conj (:image spec))
      (into (let [c (:command spec)] (cond (nil? c) [] (string? c) ["sh" "-c" c] :else c)))))

(defn inspect-image-argv [image] ["docker" "image" "inspect" "--format" "{{.Id}}" image])
(defn pull-argv [image] ["docker" "pull" image])
(defn connect-argv [network container] ["docker" "network" "connect" network container])
(defn rm-argv [container] ["docker" "rm" "-f" container])

(defn start-argv
  "`docker start -a`, its output discarded: it is unbounded, and read back
   through `docker logs --tail` instead."
  [container]
  ["sh" "-c" "exec \"$@\" >/dev/null 2>&1 </dev/null" "sh" "docker" "start" "-a" container])

(defn- step!
  "Result<stdout> of a docker step that must succeed; its stderr scrubbed."
  [exec argv opts scrub]
  (let [res (exec argv (merge {:timeout-ms 60000} opts))]
    (if (r/err? res)
      (-> res (dissoc :stdout) (update :stderr #(some-> % str scrub)) (update :message #(some-> % str scrub)))
      (let [{:keys [exit stdout stderr]} (:ok res)]
        (if (zero? exit)
          (r/ok stdout)
          (r/err :compose/command-failed {:argv argv :exit exit :stderr (scrub (str stderr))}))))))

(defn- ensure-image!
  "Result<image>: `image` present locally, pulled when it is not, the pull
   bounded by `pull-ms`, not by the run's own timeout."
  [exec image pull-ms scrub]
  (let [have (exec (inspect-image-argv image) {:timeout-ms 60000})]
    (if (and (r/ok? have) (zero? (get-in have [:ok :exit])))
      (r/ok image)
      (r/map-ok (step! exec (pull-argv image) {:timeout-ms pull-ms} scrub) (constantly image)))))

(defn- inspect [exec nm]
  (let [res (exec ["docker" "inspect" nm] {:timeout-ms 15000})]
    (when (r/ok? res)
      (try (first (json/read-str (:stdout (:ok res))))
           (catch Exception _unparseable-inspect nil)))))

(defn run-container!
  "Run the container of `spec` under its caps, its env `resolved`."
  [{:keys [exec now]} spec resolved scrub]
  (let [cs (caps spec)
        nm (:name spec)]
    ;; everything from the first docker call on sits inside try/finally: a
    ;; pull, create or connect that fails or times out half way, or a throw,
    ;; still has its container removed
    (try
      (let [prep (r/let-ok [_ (ensure-image! exec (:image spec) (or (:pull-timeout-ms spec) default-pull-timeout-ms) scrub)
                            ;; a leftover of an interrupted run under this name: best effort
                            _ (do (exec (rm-argv nm) {:timeout-ms 60000}) (r/ok nil))
                            _ (step! exec (create-argv spec cs) (cond-> {} (seq resolved) (assoc :env resolved)) scrub)]
                   (reduce (fn [acc n] (if (r/err? acc) (reduced acc) (step! exec (connect-argv n nm) {} scrub)))
                           (r/ok nil) (:extra-networks spec)))]
        (if (r/err? prep)
          (assoc prep :exit nil :scrub scrub)
          (let [started (now)
                res (exec (start-argv nm) {:timeout-ms (or (:timeout-ms spec) default-timeout-ms)})
                out (some-> (ran res)
                            (assoc :output
                                   (let [lg (exec ["docker" "logs" "--tail" (str output-lines) nm]
                                                  {:timeout-ms 15000})]
                                     (if (r/ok? lg) (mapv scrub (last-lines (:stdout (:ok lg)) (:stderr (:ok lg)))) []))))]
            (if-not out
              (not-started spec res "suite container" scrub)
              (let [info (inspect exec nm)
                    finding (oom/container-finding (service-name spec) info)
                    exit (or (get-in info ["State" "ExitCode"]) (:exit out))]
                (r/ok (cond-> (assoc out
                                     :exit (if (:timed-out? out) nil exit)
                                     :scrub scrub
                                     :started-at started
                                     :finished-at (now)
                                     :cap (cond-> {:mode (if (:mem cs) :docker :none)}
                                            (:mem cs) (assoc :mem (:mem cs))
                                            (:cpus cs) (assoc :cpus (:cpus cs))))
                        finding (assoc :oom (scrub-finding finding scrub)))))))))
      (catch Throwable t (threw t scrub))
      (finally
        (try (exec (rm-argv nm) {:timeout-ms 60000})
             (catch Throwable _best-effort-removal nil))))))

(defrecord SuiteRunner [exec now systemd? resolver]
  port/ISuiteRunner
  (-run! [this spec]
    (if-not (#{:container :command} (:runner spec))
      (r/err :compose/invalid-param {:param "runner"
                                     :message (str "not a runner kind: " (pr-str (:runner spec)))})
      (let [declared (:env spec)]
        (r/let-ok [_ (ensure-dir! (:reports-dir spec))
                   resolved (secrets/resolve-env resolver declared)]
          (let [scrub #(env/scrub declared resolved %)]
            (if (= :container (:runner spec))
              (run-container! this spec resolved scrub)
              (run-command! this spec resolved scrub))))))))

(defn make-runner
  "SuiteRunner. `opts`: :exec (default hive-system `shell.core/exec!`), :now
   (default `System/currentTimeMillis`), :compose/systemd-scope? (default:
   whether a systemd user manager answers; false falls back to ulimit -v),
   :compose/resolver ({scheme ISecretBackend}, default `pass` over that exec)."
  [opts]
  (let [exec (or (:exec opts) shell/exec!)]
    (->SuiteRunner exec
                   (or (:now opts) #(System/currentTimeMillis))
                   (if (contains? opts :compose/systemd-scope?)
                     (boolean (:compose/systemd-scope? opts))
                     (host/systemd-user-scope?))
                   (or (:compose/resolver opts) (secrets/make-resolver {:exec exec})))))
