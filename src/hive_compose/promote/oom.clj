(ns hive-compose.promote.oom
  "Pure: resource caps on suite runners and the diagnosis of an OOM kill
   (hc-16b).

   - `mem-bytes` / `mem-str`  docker memory notation <-> bytes.
   - `capped-argv`            the argv a host command runs under for a cap:
                              `systemd-run --user --scope -p MemoryMax=<cap>
                              -p MemorySwapMax=0`, or `ulimit -v` when no
                              systemd user manager answers (`cap-mode`).
   - `container-cap-args`     the `docker run` flags of a cap.
   - `container-finding`      a finding out of one `docker inspect` object:
                              State.OOMKilled true with exit 137.
   - `host-finding`           a finding out of a capped host run: the scope's
                              Result is oom-kill, or, under the ulimit
                              fallback, an allocation failure in the output.
   - `why-line`               '<service> OOM-killed at <cap>', the line the
                              failure bundle and `compose why` print.
   - `mark-run` / `mark-rows` the finding on a run result and on its flake
                              history rows (`:case/oom? true`), so the history
                              classifies the run as :oom, never as a flake.

   A finding is
     {:oom/service s :oom/cap bytes-or-nil :oom/source :container|:scope|:ulimit
      :oom/exit n-or-nil :oom/line string}
   No IO here: docker inspect and systemctl answers arrive as values."
  (:require [clojure.string :as str]))

;; SPDX-License-Identifier: MIT

;; ---------------------------------------------------------------------------
;; Memory notation
;; ---------------------------------------------------------------------------

(def ^:private unit-bytes
  {"" 1 "b" 1 "k" 1024 "m" (* 1024 1024) "g" (* 1024 1024 1024)
   "t" (* 1024 1024 1024 1024)})

(defn mem-bytes
  "Bytes of `v`: a positive integer as is, or docker's notation (digits, an
   optional binary k|m|g|t unit with an optional trailing b, any case:
   \"512m\", \"2g\", \"64kb\"). nil for nil, zero, a negative number or text
   that is not that notation."
  [v]
  (cond
    (integer? v) (when (pos? v) (long v))
    (string? v) (when-let [[_ digits unit] (re-matches #"(?i)([0-9]+)([kmgt]?)b?" (str/trim v))]
                  (let [n (* (parse-long digits) (unit-bytes (str/lower-case unit)))]
                    (when (pos? n) n)))
    :else nil))

(defn mem-str
  "`bytes` in docker notation with the largest binary unit that divides it
   exactly: 536870912 -> \"512m\", 2147483648 -> \"2g\", 1610612736 ->
   \"1536m\", 1000 -> \"1000b\"."
  [bytes]
  (let [b (long bytes)]
    (or (some (fn [[u n]] (when (and (>= b n) (zero? (mod b n))) (str (quot b n) u)))
              [["t" (unit-bytes "t")] ["g" (unit-bytes "g")] ["m" (unit-bytes "m")] ["k" (unit-bytes "k")]])
        (str b "b"))))

;; ---------------------------------------------------------------------------
;; Capped runners
;; ---------------------------------------------------------------------------

(defn cap-mode
  "How a host command with memory cap `mem` (bytes or nil) runs: :none with
   no cap, :systemd when a systemd user manager answers (`systemd?`), else
   :ulimit, the fallback that bounds address space rather than resident
   memory and is reported as such."
  [mem systemd?]
  (cond (nil? mem) :none
        systemd? :systemd
        :else :ulimit))

(defn- cpu-quota
  "systemd CPUQuota of `cpus` cores: 1.5 -> \"150%\"."
  [cpus]
  (str (long (Math/round (* 100.0 (double cpus)))) "%"))

(defn capped-argv
  "The argv `command` (an argv vector, or a string run through `sh -c`) runs
   under for `opts` {:mode (see `cap-mode`) :mem bytes :cpus n? :unit name}.

   :systemd  systemd-run --user --scope --quiet --unit=<unit>
             -p MemoryMax=<mem> -p MemorySwapMax=0 [-p CPUQuota=<n>%] -- cmd..
             The scope is not --collect'ed: its Result (oom-kill) is read after
             the run, then the unit is reset (`scope-result-argv`,
             `scope-reset-argv`).
   :ulimit   sh -c 'ulimit -v <KiB> && exec \"$@\"' sh cmd..
   :none     the command as is."
  [command {:keys [mode mem cpus unit]}]
  (let [cmd (if (string? command) ["sh" "-c" command] (vec command))]
    (case mode
      :systemd (-> ["systemd-run" "--user" "--scope" "--quiet" (str "--unit=" unit)
                    "-p" (str "MemoryMax=" mem) "-p" "MemorySwapMax=0"]
                   (cond-> cpus (into ["-p" (str "CPUQuota=" (cpu-quota cpus))]))
                   (conj "--")
                   (into cmd))
      :ulimit (into ["sh" "-c" (str "ulimit -v " (max 1 (quot (long mem) 1024)) " && exec \"$@\"") "sh"]
                    cmd)
      :none cmd)))

(defn logged-command
  "`command` with its output appended to file `log` and stdin from /dev/null,
   as an argv: `sh -c 'log=$1; shift; exec \"$@\" >>\"$log\" 2>&1 </dev/null'
   sh <log> cmd..`. A runner's output can be unbounded; it goes to a file and
   only its tail is read, never the whole stream."
  [command log]
  (into ["sh" "-c" "log=$1; shift; exec \"$@\" >>\"$log\" 2>&1 </dev/null" "sh" (str log)]
        (if (string? command) ["sh" "-c" command] command)))

(defn scope-result-argv
  "argv reading the Result of transient scope `unit` (oom-kill, success ..)."
  [unit]
  ["systemctl" "--user" "show" (str unit ".scope") "-p" "Result" "--value"])

(defn scope-reset-argv
  "argv releasing a failed transient scope `unit` once its Result is read."
  [unit]
  ["systemctl" "--user" "reset-failed" (str unit ".scope")])

(defn scope-unit
  "The transient scope a suite command runs in: `hc-suite-<name>-<ms>`, the
   name reduced to [a-zA-Z0-9_.-] so it is a valid unit name."
  [name now-ms]
  (str "hc-suite-" (str/replace (str name) #"[^a-zA-Z0-9_.-]+" "-") "-" now-ms))

(defn cap-note
  "What the report says about how a command was capped, nil when it was not."
  [mode mem]
  (case mode
    :systemd (str "capped by a systemd user scope: MemoryMax=" (mem-str mem) ", no swap")
    :ulimit (str "systemd-run absent: capped by ulimit -v " (mem-str mem)
                 " (address space, not resident memory)")
    nil))

(defn container-cap-args
  "The `docker run` flags of `caps` {:mem bytes? :cpus n?}: memory with no
   swap beyond it (--memory-swap equal to --memory), and cpus."
  [{:keys [mem cpus]}]
  (cond-> []
    mem (into ["--memory" (str mem) "--memory-swap" (str mem)])
    cpus (into ["--cpus" (str cpus)])))

;; ---------------------------------------------------------------------------
;; Findings
;; ---------------------------------------------------------------------------

(defn why-line
  "The line a failure bundle and `compose why` print for a finding:
   '<service> OOM-killed at <cap>', '<service> OOM-killed (no cap; host OOM
   killer)' when no cap is known, and under the ulimit fallback '<service>
   hit its address-space cap at <cap> (ulimit -v)'."
  [{:oom/keys [service cap source]}]
  (cond
    (= :ulimit source) (str service " hit its address-space cap at " (mem-str cap) " (ulimit -v)")
    cap (str service " OOM-killed at " (mem-str cap))
    :else (str service " OOM-killed (no cap; host OOM killer)")))

(defn finding
  "A finding with its `:oom/line`."
  [service cap source exit]
  (let [f (cond-> {:oom/service (str service) :oom/source source}
            cap (assoc :oom/cap (long cap))
            (some? exit) (assoc :oom/exit exit))]
    (assoc f :oom/line (why-line f))))

(defn container-finding
  "The finding of `inspect`, one object of `docker inspect` output (string
   keys, as JSON reads it), for `service`; nil unless the container was
   OOM-killed: State.OOMKilled true and exit code 137. The cap is
   HostConfig.Memory when positive."
  [service inspect]
  (let [state (get inspect "State")
        exit (get state "ExitCode")
        mem (get-in inspect ["HostConfig" "Memory"])]
    (when (and (true? (get state "OOMKilled")) (= 137 exit))
      (finding service (when (and (integer? mem) (pos? mem)) mem) :container exit))))

(def ^:private allocation-failure
  "What a process says when `ulimit -v` refuses it memory."
  #"(?i)cannot allocate memory|out of memory|outofmemoryerror|could not reserve enough space|std::bad_alloc|memoryerror|allocation failed|failed to allocate|ENOMEM")

(defn host-finding
  "The finding of a capped host run of `service`, nil when it was no OOM kill.
   `run` is {:mode :systemd|:ulimit|:none :mem bytes :exit n
   :scope-result string (systemd) :output text (the run's last output)}.
   - :systemd  the scope's Result is oom-kill.
   - :ulimit   a non-zero exit with an allocation failure in the output.
   - :none     never: an uncapped run's death is no cap's doing."
  [service {:keys [mode mem exit scope-result output]}]
  (case mode
    :systemd (when (= "oom-kill" (some-> scope-result str/trim))
               (finding service mem :scope exit))
    :ulimit (when (and (number? exit) (not (zero? exit))
                       (re-find allocation-failure (str output)))
              (finding service mem :ulimit exit))
    nil))

(defn mark-run
  "Run result `run` (hc-8 cell result) with the first of `findings` under
   :oom, unchanged when there is none."
  [run findings]
  (if-let [f (first (remove nil? findings))]
    (assoc run :oom f)
    run))

(defn oom-run?
  "True when run result `run` carries an OOM finding."
  [run]
  (some? (:oom run)))

(defn mark-rows
  "The flake history rows of `run` (hc-13 CaseOutcome) tagged `:case/oom?
   true` when the run was OOM-killed, so the history reads it as :oom."
  [run rows]
  (if (oom-run? run)
    (mapv #(assoc % :case/oom? true) rows)
    (vec rows)))

(defn why-lines
  "The why lines of `findings`, sorted by service, distinct."
  [findings]
  (->> findings (remove nil?) (sort-by :oom/service) (map :oom/line) distinct vec))
