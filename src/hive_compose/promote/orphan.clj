(ns hive-compose.promote.orphan
  "Processes a dead rootless dockerd left behind, and what they still hold.
   Pure: every function reads a process Snapshot taken at the boundary
   (`hive-compose.adapter.procfs`), never /proc itself.

   When rootless dockerd dies (systemd-oomd killed docker.service on
   2026-11-05), its containers read Exited (255) but their processes keep
   running: the container's first process is reparented to the user's
   `systemd --user`, keeps its subuid and keeps its mounts. Starting or
   recreating the service then runs a second copy on the same volume in
   other PID and IPC namespaces, where e.g. postgres' own data-dir interlock
   cannot see the first (incident 20261105135748-5afc73f2).

   A live container's process is a child of its containerd-shim, never of
   `systemd --user`, so the parent alone tells an orphan from a live one; a
   process whose cgroup names a container the daemon reports running is
   still taken as owned.

   Snapshot:
     {:manager-pid n                 ; the user's `systemd --user`
      :subuid      [first count]     ; the user's /etc/subuid range
      :procs [{:pid :ppid :uid :name :args
               :cgroup     \"0::/..../docker-<id>.scope\"
               :mounts     [root ..]  ; mountinfo field 4, subuid procs only
               :listening  #{port}}]} ; LISTEN sockets of its netns, subuid procs only

   Ownership seam: `container-of` and `owner` name the container a process
   belongs to; DC-SHARED (a section refusing to recreate a service another
   live section holds) can map that container to its compose project and
   service the same way."
  (:require [clojure.string :as str]))

;; SPDX-License-Identifier: MIT

(defn parse-subuid
  "[first count] of `user` in /etc/subuid `text` (lines `name:first:count`),
   nil when the user has no range. Pure."
  [text user]
  (some (fn [line]
          (let [[nm first-id n] (str/split (str/trim line) #":")]
            (when (and (= nm user) first-id n)
              (try [(Long/parseLong first-id) (Long/parseLong n)]
                   (catch NumberFormatException _malformed nil)))))
        (str/split-lines (or text ""))))

(defn in-subuid?
  "True when `uid` falls in range [first count]."
  [[first-id n] uid]
  (boolean (and first-id n uid (<= first-id uid) (< uid (+ first-id n)))))

(defn parse-status
  "{:name :ppid :uid} of a /proc/<pid>/status `text` (uid: the real one),
   nil when it carries none of them. Pure."
  [text]
  (let [fields (into {} (keep (fn [line]
                                (let [i (str/index-of line ":")]
                                  (when i [(subs line 0 i) (str/trim (subs line (inc i)))]))))
                     (str/split-lines (or text "")))
        num (fn [s] (try (Long/parseLong s) (catch Exception _malformed nil)))
        uid (some-> (get fields "Uid") (str/split #"\s+") first num)
        ppid (some-> (get fields "PPid") num)]
    (when (and uid ppid)
      {:name (get fields "Name") :ppid ppid :uid uid})))

(defn manager-pid
  "The pid of `uid`'s `systemd --user` among `procs`, nil when none runs."
  [procs uid]
  (some (fn [{:keys [name args] :as pr}]
          (when (and (= uid (:uid pr))
                     (= "systemd" name)
                     (some #{"--user"} (str/split (str/trim (or args "")) #"\s+")))
            (:pid pr)))
        (sort-by :pid procs)))

(defn container-of
  "The container id a /proc/<pid>/cgroup `text` names (a `docker-<id>.scope`
   cgroup), nil when the process is in no container scope."
  [text]
  (some->> (re-find #"docker-([0-9a-f]{12,64})\.scope" (or text "")) second))

(defn owner
  "{:container id :live? bool} of process `pr` given `live` (the full ids of
   the containers the daemon runs), nil outside any container scope."
  [pr live]
  (when-let [id (container-of (:cgroup pr))]
    {:container id :live? (contains? (set live) id)}))

(defn listening-ports
  "#{port} of the LISTEN sockets in /proc/<pid>/net/tcp(6) `text` owned by
   `uid` (the table's uid column, as the reader's namespace sees it). A
   process on the host network sees every host socket in that table; the
   uid keeps only its own. Pure."
  [text uid]
  (into (sorted-set)
        (keep (fn [line]
                (let [cols (str/split (str/trim line) #"\s+") ;; pattern-exempt: whitespace split of a kernel table
                      [_sl local _remote st] cols
                      owner-uid (nth cols 7 nil)
                      i (some-> local (str/last-index-of ":"))]
                  (when (and i (= "0A" st) (= (str uid) owner-uid))
                    (try (Long/parseLong (subs local (inc i)) 16)
                         (catch NumberFormatException _malformed nil))))))
        (rest (str/split-lines (or text "")))))

(defn mount-root
  "Field 4 (the root of the mount within its filesystem) of a mountinfo line."
  [line]
  (nth (str/split (str/trim (or line "")) #"\s+") 3 nil)) ;; pattern-exempt: whitespace split of a kernel table

(defn volume-of
  "The docker volume a mount root lies in (`.../volumes/<name>/_data[/..]`),
   nil for any other mount."
  [root]
  (some->> (re-find #"/volumes/([^/]+)/_data(?:/.*)?$" (or root "")) second))

(defn- children-index [procs]
  (group-by :ppid procs))

(defn- subtree
  "`root` and every process below it."
  [index root]
  (loop [acc [] todo [root]]
    (if-let [[pr & more] (seq todo)]
      (recur (conj acc pr) (into (vec more) (get index (:pid pr))))
      acc)))

(defn container-process?
  "True when `pr` ran in a container: its uid is in the subuid range (a
   non-root container user), or its cgroup is a container scope (container
   root maps to the user's own uid, outside the range)."
  [subuid pr]
  (or (in-subuid? subuid (:uid pr)) (some? (container-of (:cgroup pr)))))

(defn roots
  "The orphan roots of `snapshot`: container processes (`container-process?`)
   whose parent is `systemd --user`, none of a container in `live`. Empty when
   the snapshot has no manager or no range (not rootless)."
  [{:keys [manager-pid subuid procs]} live]
  (if-not (and manager-pid subuid)
    []
    (vec (sort-by :pid
                  (filter (fn [pr]
                            (and (= manager-pid (:ppid pr))
                                 (container-process? subuid pr)
                                 (not (:live? (owner pr live)))))
                          procs)))))

(defn holds
  "What processes `prs` hold of `resources` ({:volumes #{name} :ports
   #{port}}): {:volumes :ports}, each a sorted set. A volume is held when one
   of their mounts lies in it, a port when one of them listens on it."
  [prs {:keys [volumes ports]}]
  (let [vols (set volumes) prts (set ports)]
    {:volumes (into (sorted-set) (comp (mapcat :mounts) (keep volume-of) (filter vols)) prs)
     :ports (into (sorted-set) (comp (mapcat :listening) (filter prts)) prs)}))

(defn orphans
  "[Orphan] of `snapshot` holding any of `resources`, by root pid. An Orphan:
   {:pid :name :args :container :pids [pid of it and below] :holds}. `live`
   is the set of running container ids (may be empty: the parent rule
   already leaves live containers out). Pure."
  [snapshot live resources]
  (let [index (children-index (:procs snapshot))]
    (vec (for [root (roots snapshot live)
               :let [tree (subtree index root)
                     held (holds tree resources)]
               :when (some seq (vals held))]
           (cond-> {:pid (:pid root)
                    :name (:name root)
                    :args (:args root)
                    :pids (vec (sort (map :pid tree)))
                    :holds (into {} (filter (comp seq val)) held)}
             (container-of (:cgroup root)) (assoc :container (container-of (:cgroup root))))))))

(defn describe
  "One line naming `found` orphans and what each holds, for a refusal."
  [found]
  (str/join "; "
            (for [{:keys [pid name holds]} found]
              (str name " (pid " pid ") holds "
                   (str/join ", " (for [[k vs] (sort-by key holds)]
                                    (str (clojure.core/name k) " " (str/join " " vs))))))))
