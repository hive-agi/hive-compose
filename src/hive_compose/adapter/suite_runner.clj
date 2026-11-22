(ns hive-compose.adapter.suite-runner
  "ISuiteRunner over the host (hc-16b): one suite runner run to its end under
   its resource caps, its OOM kill diagnosed.

   A host command runs as
     systemd-run --user --scope --quiet --unit=hc-suite-<test>-<ms>
                 -p MemoryMax=<cap> -p MemorySwapMax=0 -- <command>
   so a runaway is OOM-killed inside its own scope and cannot take the host
   down; the scope's Result (oom-kill) is read after the run and the unit
   reset. Without a systemd user manager the command runs under `ulimit -v`
   and the answer's :cap :note says so. A container runs with
   `--memory <cap> --memory-swap <cap> --cpus n`, is inspected once it ends
   (State.OOMKilled, ExitCode, HostConfig.Memory) and removed.

   Env values never reach an argv: a container gets `-e NAME` and docker
   reads the value from its own environment.

   Everything outside is injected: `:exec` (hive-system `shell.core/exec!`
   shape: argv, {:dir :env :timeout-ms} -> Result<{:exit :stdout :stderr}>),
   `:now` (epoch ms) and `:systemd?`."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [hive-compose.adapter.host :as host]
            [hive-compose.port :as port]
            [hive-compose.promote.oom :as oom]
            [hive-dsl.result :as r]
            [hive-system.shell.core :as shell]))

;; SPDX-License-Identifier: MIT

(def ^:private output-lines 50)

(def ^:private default-timeout-ms
  "A runner with no :timeout-ms of its own may run this long (1 h)."
  3600000)

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
  "{:exit :output :timed-out?} of an exec Result, or the error itself when the
   command could not start."
  [res]
  (cond
    (r/ok? res) {:exit (:exit (:ok res))
                 :output (last-lines (:stdout (:ok res)) (:stderr (:ok res)))
                 :timed-out? false}
    (= :shell/timeout (:error res)) {:exit nil :output [] :timed-out? true}
    :else nil))

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
  "Run the host command of `spec` under its caps."
  [{:keys [exec now systemd?]} spec]
  (let [{:keys [mem cpus]} (caps spec)
        mode (oom/cap-mode mem systemd?)
        started (now)
        unit (oom/scope-unit (service-name spec) started)
        log (or (:log spec) (temp-log))
        argv (oom/capped-argv (oom/logged-command (:command spec) log)
                              {:mode mode :mem mem :cpus cpus :unit unit})
        res (exec argv (cond-> {:timeout-ms (or (:timeout-ms spec) default-timeout-ms)}
                         (:dir spec) (assoc :dir (:dir spec))
                         (seq (:env spec)) (assoc :env (:env spec))))
        out (some-> (ran res) (update :output into (tail-lines log)))
        _ (when-not (:log spec) (.delete (java.io.File. (str log))))]
    (if-not out
      (r/err :compose/runner-failed {:test-id (:test-id spec) :cause (select-keys res [:error :message])
                                     :message (str "suite runner did not start: " (or (:message res) (:error res)))})
      (let [result (when (= :systemd mode) (scope-result exec unit))
            finding (oom/host-finding (service-name spec)
                                      {:mode mode :mem mem :exit (:exit out)
                                       :scope-result result
                                       :output (str/join "\n" (:output out))})]
        (r/ok (cond-> (assoc out
                             :started-at started
                             :finished-at (now)
                             :cap (cond-> {:mode mode}
                                    mem (assoc :mem mem)
                                    cpus (assoc :cpus cpus)
                                    (oom/cap-note mode mem) (assoc :note (oom/cap-note mode mem))
                                    (= :systemd mode) (assoc :unit unit)))
                finding (assoc :oom finding)))))))

;; ---------------------------------------------------------------------------
;; container

(defn container-argv
  "The `docker run` argv of container `spec` under `cs` {:mem :cpus}. Not
   --rm: the container is inspected after it ends, then removed."
  [spec cs]
  (-> ["docker" "run" "--name" (:name spec)]
      (into (mapcat (fn [[k v]] ["--label" (str k "=" v)]) (sort (:labels spec))))
      (cond-> (:network spec) (into ["--network" (:network spec)]))
      (into (mapcat (fn [{:keys [host container]}] ["-v" (str host ":" container)]) (:mounts spec)))
      (into (mapcat (fn [k] ["-e" k]) (sort (keys (:env spec)))))
      (into (oom/container-cap-args cs))
      (conj (:image spec))
      (into (let [c (:command spec)] (cond (nil? c) [] (string? c) ["sh" "-c" c] :else c)))))

(defn- inspect [exec nm]
  (let [res (exec ["docker" "inspect" nm] {:timeout-ms 15000})]
    (when (r/ok? res)
      (try (first (json/read-str (:stdout (:ok res))))
           (catch Exception _unparseable-inspect nil)))))

(defn run-container!
  "Run the container of `spec` under its caps."
  [{:keys [exec now]} spec]
  (let [cs (caps spec)
        started (now)
        ;; The container's output is discarded here, never drained into this
        ;; heap (it is unbounded), and read back through `docker logs --tail`.
        res (exec (into ["sh" "-c" "exec \"$@\" >/dev/null 2>&1 </dev/null" "sh"]
                        (container-argv spec cs))
                  (cond-> {:timeout-ms (or (:timeout-ms spec) default-timeout-ms)}
                    (seq (:env spec)) (assoc :env (:env spec))))
        out (some-> (ran res)
                    (assoc :output
                           (let [lg (exec ["docker" "logs" "--tail" (str output-lines) (:name spec)]
                                          {:timeout-ms 15000})]
                             (if (r/ok? lg) (last-lines (:stdout (:ok lg)) (:stderr (:ok lg))) []))))]
    (if-not out
      (r/err :compose/runner-failed {:test-id (:test-id spec) :cause (select-keys res [:error :message])
                                     :message (str "suite container did not start: " (or (:message res) (:error res)))})
      (let [info (inspect exec (:name spec))
            finding (oom/container-finding (service-name spec) info)
            exit (or (get-in info ["State" "ExitCode"]) (:exit out))]
        (exec ["docker" "rm" "-f" (:name spec)] {:timeout-ms 30000})
        (r/ok (cond-> (assoc out
                             :exit (if (:timed-out? out) nil exit)
                             :started-at started
                             :finished-at (now)
                             :cap (cond-> {:mode (if (:mem cs) :docker :none)}
                                    (:mem cs) (assoc :mem (:mem cs))
                                    (:cpus cs) (assoc :cpus (:cpus cs))))
                finding (assoc :oom finding)))))))

(defrecord SuiteRunner [exec now systemd?]
  port/ISuiteRunner
  (-run! [this spec]
    (case (:runner spec)
      :container (run-container! this spec)
      :command (run-command! this spec)
      (r/err :compose/invalid-param {:param "runner"
                                     :message (str "not a runner kind: " (pr-str (:runner spec)))}))))

(defn make-runner
  "SuiteRunner. `opts`: :exec (default hive-system `shell.core/exec!`), :now
   (default `System/currentTimeMillis`), :compose/systemd-scope? (default:
   whether a systemd user manager answers; false falls back to ulimit -v)."
  [opts]
  (->SuiteRunner (or (:exec opts) shell/exec!)
                 (or (:now opts) #(System/currentTimeMillis))
                 (if (contains? opts :compose/systemd-scope?)
                   (boolean (:compose/systemd-scope? opts))
                   (host/systemd-user-scope?))))
