(ns hive-compose.boundary.files
  "The two files the addon reads and writes: the profiles file and the state
   file holding the active map."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
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
