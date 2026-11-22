(ns hive-compose.boundary.files
  "The two files the addon reads and writes: the profiles file and the state
   file holding the active map."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [hive-compose.promote.sync :as sync]
            [hive-dsl.result :as r])
  (:import (java.nio.file CopyOption Files StandardCopyOption)))

;; SPDX-License-Identifier: MIT

(defn- read-edn [path category absent]
  (let [f (io/file path)]
    (if (.exists f)
      (r/try-effect* category (edn/read-string (slurp f)))
      (r/ok absent))))

(defn read-profiles
  "Result<profiles-or-nil> from the EDN profiles file at `path`."
  [path]
  (read-edn path :compose/profiles-unreadable nil))

(defn load-state
  "Result<{:active .. :current ..}> from the state file at `path`."
  [path]
  (read-edn path :compose/state-unreadable {}))

(defn save-state!
  "Write `st` to `path` through a sibling temp file and an atomic move.
   Result<path>."
  [path st]
  (r/try-effect* :compose/state-unwritable
    (let [f (io/file path)
          tmp (io/file (str path ".tmp"))]
      (io/make-parents f)
      (spit tmp (binding [*print-namespace-maps* false
                          *print-length* nil
                          *print-level* nil]
                  (pr-str st)))
      (Files/move (.toPath tmp) (.toPath f)
                  (into-array CopyOption [StandardCopyOption/REPLACE_EXISTING
                                          StandardCopyOption/ATOMIC_MOVE]))
      path)))

(defn read-lock
  "Result<Lock-or-nil> from the stack lockfile at `path`
   (hive-compose.promote.stack/lock-path); nil when there is none."
  [path]
  (read-edn path :compose/lock-unreadable nil))

(defn write-lock!
  "Write lockfile value `lock` (hive-compose.promote.stack/lock) to `path`
   atomically, creating its directory. Result<path>."
  [path lock]
  (let [res (save-state! path lock)]
    (if (r/err? res) (assoc res :error :compose/lock-unwritable) res)))

(defn- persisted [st]
  {:active (or (:active st) {}) :current (:current st)})

(defn sync-state!
  "Merge the live state with the file at `path` and write the merge.

   `state` is the live atom, `base` an atom holding what this instance last
   read or wrote. What the file gained since `base` lands in `state` too, so a
   second instance sharing the file (a hot remount) keeps its writes and so do
   we. An unreadable file counts as unchanged since `base`, so it is
   overwritten as before. Locked per path within the JVM. Result<merged>."
  [path base state]
  (locking (.intern (str path))
    (let [loaded (load-state path)
          theirs (if (r/ok? loaded) (persisted (:ok loaded)) @base)
          after (swap! state #(merge % (sync/merge-state @base (persisted %) theirs)))
          merged (persisted after)]
      (r/let-ok [_ (if (and (r/ok? loaded) (= merged theirs)) (r/ok path) (save-state! path merged))]
        (reset! base merged)
        (r/ok merged)))))
