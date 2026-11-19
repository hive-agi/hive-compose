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

(defn- hex-bytes
  "The bytes of hex string `s` (an even number of digits), nil when malformed."
  [s]
  (when (and (string? s) (pos? (count s)) (even? (count s)))
    (try (byte-array (map #(unchecked-byte (Integer/parseInt (subs s % (+ % 2)) 16))
                          (range 0 (count s) 2)))
         (catch NumberFormatException _malformed nil))))

(defn- network-order
  "Kernel address bytes `bs` in network order: /proc/net/tcp(6) prints an
   address as 32-bit words, each in host (little-endian) order."
  [^bytes bs]
  (byte-array (mapcat reverse (partition 4 bs))))

(defn endpoint
  "\"ip:port\" of a /proc/net/tcp(6) `ADDR:PORT` column (\"[ip6]:port\" for
   IPv6; an IPv4-mapped IPv6 address reads as its IPv4 form), nil when
   malformed. Pure: the address is a literal, nothing is looked up."
  [col]
  (let [i (str/last-index-of (or col "") ":")
        p (hex-port col)
        bs (when i (hex-bytes (subs col 0 i)))]
    (when (and p bs (contains? #{4 16} (alength ^bytes bs)))
      (let [addr (java.net.InetAddress/getByAddress ^bytes (network-order bs))]
        (if (instance? java.net.Inet6Address addr)
          (str "[" (.getHostAddress addr) "]:" p)
          (str (.getHostAddress addr) ":" p))))))

(defn peers
  "{port #{peer}} of /proc/net/tcp(6) `text` for local `ports`: the remote
   endpoint (as `endpoint`, \"ip:port\") of every socket whose local port is
   one of them, listening sockets left out. Pure."
  [text ports]
  (let [ports (set ports)]
    (reduce (fn [acc line]
              (let [[_sl local remote st] (str/split (str/trim line) #"\s+") ;; pattern-exempt: whitespace split of a kernel table
                    p (hex-port local)
                    peer (endpoint remote)]
                (if (and p (contains? ports p) peer (not= listen-state st))
                  (update acc p (fnil conj #{}) peer)
                  acc)))
            {}
            (rest (str/split-lines (or text ""))))))

(defn- slurp-quiet
  "The text of `f`, \"\" when it cannot be read. Read line by line: `slurp`
   fails on procfs files (FileInputStream.available answers EINVAL)."
  [f]
  (try (with-open [r (java.io.BufferedReader. (java.io.FileReader. (str f)))]
         (str/join "\n" (doall (line-seq r))))
       (catch Exception _unreadable "")))

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
