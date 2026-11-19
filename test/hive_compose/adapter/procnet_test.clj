(ns hive-compose.adapter.procnet-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]
            [hive-compose.adapter.procnet :as procnet]
            [hive-compose.port :as port]))

;; SPDX-License-Identifier: MIT

;; 7888 = 0x1ED0, 5432 = 0x1538, 41000 = 0xA028
(def tcp
  (str "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
       "   0: 0100007F:1ED0 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 1 1\n"
       "   1: 0100007F:1ED0 0100007F:A028 01 00000000:00000000 00:00000000 00000000  1000        0 2 1\n"
       "   2: 0100007F:A028 0100007F:1ED0 01 00000000:00000000 00:00000000 00000000  1000        0 3 1\n"
       "   3: 00000000:1538 0200007F:B000 06 00000000:00000000 00:00000000 00000000  1000        0 4 1\n"
       "garbage line\n"))

(deftest peers-are-the-accepted-connections-of-a-local-port
  (is (= {7888 #{"0100007F:A028"}} (procnet/peers tcp [7888]))
      "the listening socket and the client side (local port 41000) are left out")
  (is (= {5432 #{"0200007F:B000"}} (procnet/peers tcp [5432])) "a closing connection still counts")
  (is (= {} (procnet/peers tcp [9999])))
  (is (= {} (procnet/peers nil [7888]))))

(deftest the-default-probe-reads-the-kernel-table
  ;; Linux only: a live connection to a port the OS chose shows as one peer.
  (when (.exists (io/file "/proc/net/tcp"))
    (with-open [ss (java.net.ServerSocket. 0)]
      (let [p (.getLocalPort ss)
            accepted (future (.accept ss))]
        (with-open [_client (java.net.Socket. "127.0.0.1" (int p))
                    _server ^java.net.Socket @accepted]
          (is (= 1 (count (get (port/-peers (procnet/make-activity) #{p}) p)))))))))

(deftest the-probe-merges-both-tables-and-survives-a-missing-one
  (let [f (doto (java.io.File/createTempFile "tcp" ".txt") (.deleteOnExit))]
    (spit f tcp)
    (let [probe (procnet/make-activity [(str f) "/nonexistent/tcp6"])]
      (is (= {7888 #{"0100007F:A028"}} (port/-peers probe #{7888})))
      (is (= {} (port/-peers probe #{}))))
    (io/delete-file f true)))
