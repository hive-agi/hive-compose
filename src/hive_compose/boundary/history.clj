(ns hive-compose.boundary.history
  "The flake history file: an append-only EDN-lines file in the state dir,
   one CaseOutcome row per line (see `hive-compose.schema`).

   An append writes every row of the call as one write under an exclusive
   file lock (a second JVM sharing the file waits) and forces it to disk, so
   a run is either all in the file or, when the process dies mid-write, ends
   in one torn last line. A reader skips a line it cannot read and counts it
   under `:skipped` in the meta of the rows; nothing is ever rewritten."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [hive-compose.port :as port]
            [hive-compose.promote.flaky :as flaky]
            [hive-dsl.result :as r])
  (:import (java.io RandomAccessFile)
           (java.nio.charset StandardCharsets)))

;; SPDX-License-Identifier: MIT

(defn- line [row]
  (binding [*print-namespace-maps* false
            *print-length* nil
            *print-level* nil]
    (pr-str row)))

(defn append-rows!
  "Append `rows` to the EDN-lines file at `path`, creating it and its
   directory when absent. When the file does not end in a newline (a torn
   last line), one is written first, so the torn line stays alone and the
   new rows read. Result<{:appended n}>."
  [path rows]
  (if (empty? rows)
    (r/ok {:appended 0})
    (r/try-effect* :compose/history-unwritable
      (let [f (.getCanonicalFile (io/file path))
            body (str (str/join "\n" (map line rows)) "\n")]
        (io/make-parents f)
        (locking (.intern (str f))
          (with-open [raf (RandomAccessFile. f "rw")]
            (let [ch (.getChannel raf)
                  lock (.lock ch)]
              (try
                (let [size (.size ch)
                      torn? (and (pos? size)
                                 (do (.seek raf (dec size)) (not= 10 (.read raf))))
                      bytes (.getBytes (str (when torn? "\n") body) StandardCharsets/UTF_8)]
                  (.seek raf size)
                  (.write raf bytes)
                  (.force ch true))
                (finally (.release lock))))))
        {:appended (count rows)}))))

(defn- parse-line [s]
  (try
    (let [v (edn/read-string s)]
      (when (map? v) v))
    (catch Exception _ nil)))

(defn read-rows
  "Result<[row]>: the rows of the EDN-lines file at `path` in append order, []
   when it is absent. A blank line is ignored; a line that is no EDN map (a
   torn last line) is skipped and counted under `:skipped` in the meta."
  [path]
  (let [f (io/file path)]
    (if-not (.exists f)
      (r/ok [])
      (r/try-effect* :compose/history-unreadable
        (with-open [rdr (io/reader f :encoding "UTF-8")]
          (let [lines (remove str/blank? (line-seq rdr))
                parsed (mapv parse-line lines)
                rows (filterv some? parsed)]
            (with-meta rows {:skipped (- (count parsed) (count rows))})))))))

(defrecord FileHistory [path]
  port/IFlakeHistory
  (-append! [_ rows] (append-rows! path rows))
  (-history [_ query]
    (r/let-ok [rows (read-rows path)]
      (r/ok (with-meta (vec (flaky/matching query rows)) (meta rows))))))

(defn file-history
  "An IFlakeHistory kept in the EDN-lines file at `path`."
  [path]
  (->FileHistory path))
