(ns hive-compose.adapter.procfs
  "IProcessTable over Linux /proc: the boundary that turns the host's process
   table into the Snapshot `hive-compose.promote.orphan` reads. Only the
   processes that can be orphaned container processes (parent `systemd
   --user`, subuid range or a container cgroup) have their mounts and
   listening sockets read; a process that is gone or unreadable mid-walk is
   left out, never an error."
  (:require [clojure.string :as str]
            [hive-compose.port :as port]
            [hive-compose.promote.orphan :as orphan]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- slurp-quiet
  "The text of `f`, nil when it cannot be read. Read line by line: `slurp`
   fails on procfs files (FileInputStream.available answers EINVAL)."
  [f]
  (try (with-open [rd (java.io.BufferedReader. (java.io.FileReader. (str f)))]
         (str/join "\n" (doall (line-seq rd))))
       (catch Exception _unreadable nil)))

(defn- pids [root]
  (keep (fn [^java.io.File f]
          (let [n (.getName f)]
            (when (re-matches #"\d+" n) (Long/parseLong n))))
        (or (.listFiles (java.io.File. (str root))) [])))

(defn- args-of [root pid]
  (some-> (slurp-quiet (str root "/" pid "/cmdline")) (str/replace "\u0000" " ") str/trim))

(defn- shallow-proc
  "{:pid :name :ppid :uid :args :cgroup} of `pid` under `root`, nil when gone."
  [root pid]
  (when-let [st (orphan/parse-status (slurp-quiet (str root "/" pid "/status")))]
    (assoc st :pid pid :args (args-of root pid) :cgroup (slurp-quiet (str root "/" pid "/cgroup")))))

(defn- deepen
  "`pr` with its mount roots and the ports it listens on."
  [root pr]
  (let [base (str root "/" (:pid pr))]
    (assoc pr
           :mounts (vec (keep orphan/mount-root
                              (str/split-lines (or (slurp-quiet (str base "/mountinfo")) ""))))
           :listening (into (sorted-set)
                            (mapcat #(orphan/listening-ports (slurp-quiet (str base "/net/" %)) (:uid pr)))
                            ["tcp" "tcp6"]))))

(defn snapshot
  "Snapshot of the process table under `root` for `user` with uid `uid`,
   its subuid range read from `subuid-file`. Every process below an orphan
   candidate is deepened too, so what a worker child holds counts."
  [{:keys [root subuid-file user uid]}]
  (let [subuid (orphan/parse-subuid (slurp-quiet subuid-file) user)
        shallow (vec (keep #(shallow-proc root %) (pids root)))
        mgr (orphan/manager-pid shallow uid)
        candidates (if (and mgr subuid)
                     (set (map :pid (filter #(and (= mgr (:ppid %)) (orphan/container-process? subuid %))
                                            shallow)))
                     #{})
        by-pid (into {} (map (juxt :pid identity)) shallow)
        under? (fn [pr] (loop [p pr n 0]
                          (cond (nil? p) false
                                (contains? candidates (:pid p)) true
                                (> n 64) false
                                :else (recur (get by-pid (:ppid p)) (inc n)))))]
    {:manager-pid mgr
     :subuid subuid
     :procs (mapv #(if (under? %) (deepen root %) %) shallow)}))

(defn- own-uid []
  (some-> (slurp-quiet "/proc/self/status") orphan/parse-status :uid))

(defrecord ProcTable [opts]
  port/IProcessTable
  (-snapshot [_]
    (let [o (merge {:root "/proc" :subuid-file "/etc/subuid"
                    :user (System/getProperty "user.name")}
                   opts)
          uid (or (:uid o) (own-uid))]
      (if (and uid (.isDirectory (java.io.File. (str (:root o)))))
        (r/try-effect* :compose/proc-unreadable (snapshot (assoc o :uid uid)))
        (r/err :compose/proc-unreadable {:root (:root o)})))))

(defn make-process-table
  "ProcTable over /proc and /etc/subuid; `opts` override :root,
   :subuid-file, :user and :uid."
  ([] (make-process-table {}))
  ([opts] (->ProcTable opts)))
