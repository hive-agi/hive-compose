(ns hive-compose.adapter.procnet
  "IActivity over Linux /proc/net/tcp and /proc/net/tcp6: the remote
   endpoints of the connections accepted on a local port. Parsing is pure;
   reading the two files is the only effect, and a file that cannot be read
   contributes nothing (another OS answers no peers, never an error)."
  (:require [clojure.string :as str]
            [hive-compose.port :as port]))

;; SPDX-License-Identifier: MIT

(def ^:private listen-state "0A")

(defn- hex-port
  "The port of a /proc/net/tcp `ADDR:PORT` (hex) column, nil when malformed."
  [col]
  (let [i (str/last-index-of (or col "") ":")]
    (when (and i (< (inc i) (count col)))
      (try (Long/parseLong (subs col (inc i)) 16)
           (catch NumberFormatException _malformed nil)))))

(defn peers
  "{port #{peer}} of /proc/net/tcp(6) `text` for local `ports`: the remote
   column of every socket whose local port is one of them, listening sockets
   left out. Pure."
  [text ports]
  (let [ports (set ports)]
    (reduce (fn [acc line]
              (let [[_sl local remote st] (str/split (str/trim line) #"\s+") ;; pattern-exempt: whitespace split of a kernel table
                    p (hex-port local)]
                (if (and p (contains? ports p) remote (not= listen-state st))
                  (update acc p (fnil conj #{}) remote)
                  acc)))
            {}
            (rest (str/split-lines (or text ""))))))

(defn- slurp-quiet [f]
  (try (slurp f) (catch Exception _unreadable "")))

(defrecord ProcNet [files]
  port/IActivity
  (-peers [_ ports]
    (if (empty? ports)
      {}
      (apply merge-with into (map #(peers (slurp-quiet %) ports) files)))))

(defn make-activity
  "ProcNet over `files` (default /proc/net/tcp and /proc/net/tcp6)."
  ([] (make-activity ["/proc/net/tcp" "/proc/net/tcp6"]))
  ([files] (->ProcNet files)))
