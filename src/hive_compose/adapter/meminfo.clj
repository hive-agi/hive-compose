(ns hive-compose.adapter.meminfo
  "IHostMemory over Linux /proc/meminfo (hc-16a). The parse is pure
   (`hive-compose.promote.resources/parse-meminfo`); this is only the read."
  (:require [clojure.string :as str]
            [hive-compose.port :as port]
            [hive-compose.promote.resources :as res]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- read-lines
  "The text of `f`, read line by line (procfs answers EINVAL to `available`)."
  [f]
  (with-open [rd (java.io.BufferedReader. (java.io.FileReader. (str f)))]
    (str/join "\n" (doall (line-seq rd)))))

(defrecord ProcMeminfo [path]
  port/IHostMemory
  (-mem-available [_]
    (r/let-ok [text (r/try-effect* :compose/meminfo-unreadable (read-lines path))]
      (if-let [b (res/parse-meminfo text)]
        (r/ok b)
        (r/err :compose/meminfo-unreadable {:path path :hint "no MemAvailable line"})))))

(defn make-host-memory
  "IHostMemory reading `path` (default /proc/meminfo)."
  ([] (make-host-memory "/proc/meminfo"))
  ([path] (->ProcMeminfo path)))
