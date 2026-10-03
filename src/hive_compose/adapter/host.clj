(ns hive-compose.adapter.host
  "IProgramRunner over the host: processes through hive-system (`process.core`
   spawns, `process.liveness` answers for a pid, `shell.core/exec!` signals),
   build files through EDN reads.

   A program starts as `sh -c 'exec setsid <command> >> <log> 2>&1'`: the shell
   replaces itself with the program in a session of its own, so the spawned pid
   is the program's and its process group's. The group is what `-stop!`
   signals, which reaches the JVMs and node processes a watch forks. The pid is
   all that is kept, so a program outlives the addon and is stopped by pid after
   a restart."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [hive-compose.port :as port]
            [hive-compose.promote.program :as program]
            [hive-dsl.result :as r]
            [hive-system.process.core :as process]
            [hive-system.process.liveness :as liveness]
            [hive-system.shell.core :as shell])
  (:import (java.net InetSocketAddress Socket)))

;; SPDX-License-Identifier: MIT

(def ^:private edn-files ["shadow-cljs.edn" "deps.edn" "bb.edn"])

(defn- read-build-file
  "The EDN of `f`, reader tags kept as tagged literals; an empty map when it
   does not parse, so the directory still counts as that kind."
  [f]
  (try (let [v (edn/read-string {:default tagged-literal} (slurp f))]
         (if (map? v) v {}))
       (catch Exception _unparseable-still-that-kind {})))

(defn- facts [dir]
  (cond-> (into {}
                (keep (fn [nm]
                        (let [f (io/file dir nm)]
                          (when (.isFile f) [nm (read-build-file f)]))))
                edn-files)
    (.isFile (io/file dir "project.clj")) (assoc "project.clj" true)))

(defn- close-pipes!
  "Close the pipes of a spawned handle: the program reads /dev/null and writes
   its log, so nothing flows through them."
  [{:keys [stdin stdout stderr]}]
  (doseq [^java.io.Closeable s [stdin stdout stderr]]
    (try (.close s) (catch Exception _best-effort-close nil))))

(defn- signal-group!
  "Signal the process group `pid` leads; the bare pid when it leads none."
  [exec pid sig]
  (let [group (exec ["kill" (str "-" sig) "--" (str "-" pid)] {:timeout-ms 5000})]
    (if (and (r/ok? group) (zero? (:exit (:ok group))))
      group
      (exec ["kill" (str "-" sig) (str pid)] {:timeout-ms 5000}))))

(defn- await-death
  "True when `pid` dies within `grace-ms`."
  [alive? pid grace-ms]
  (loop [left grace-ms]
    (cond
      (not (alive? pid)) true
      (<= left 0) false
      :else (do (Thread/sleep 100) (recur (- left 100))))))

(defrecord HostRunner [spawn exec alive? grace-ms]
  port/IProgramRunner
  (-facts [_ dir]
    (r/try-effect* :compose/program-unreadable (facts dir)))
  (-start! [_ p log]
    (r/bind (r/try-effect* :compose/log-unwritable (io/make-parents (io/file log)) log)
            (fn [_]
              (r/map-ok (spawn (program/detached-script (:program/command p) log)
                               (cond-> {:dir (:program/dir p)}
                                 (seq (:program/env p)) (assoc :env (:program/env p))))
                        (fn [handle]
                          (close-pipes! handle)
                          {:pid (:pid handle)})))))
  (-alive? [_ pid] (boolean (alive? pid)))
  (-listening? [_ port]
    ;; A refused or timed-out connect is the answer, not a failure.
    (try (with-open [s (Socket.)]
           (.connect s (InetSocketAddress. "127.0.0.1" (int port)) 250)
           true)
         (catch java.io.IOException _ false)))
  (-nrepl-port [_ dir]
    (let [f (io/file dir ".nrepl-port")]
      (when (.isFile f)
        (parse-long (str/trim (slurp f))))))
  (-halt! [_ pid]
    (if-not (alive? pid)
      (r/ok {:pid pid :stopped? true :already-dead? true})
      (r/try-effect* :compose/program-stop-failed
        (signal-group! exec pid "TERM")
        (when-not (await-death alive? pid grace-ms)
          (signal-group! exec pid "KILL")
          (await-death alive? pid 2000))
        {:pid pid :stopped? (not (alive? pid))}))))

(defn make-runner
  "HostRunner. `opts`: :spawn (default hive-system `process.core/spawn!`), :exec
   (default `shell.core/exec!`), :alive? (default `process.liveness/alive?`),
   :compose/stop-grace-ms before a group is killed (default 5000)."
  [opts]
  (->HostRunner (or (:spawn opts) process/spawn!)
                (or (:exec opts) shell/exec!)
                (or (:alive? opts) liveness/alive?)
                (or (:compose/stop-grace-ms opts) 5000)))
