(ns hive-compose.swarm-e2e-test
  "End to end through the `compose` tool of a real addon: docker containers,
   a babashka nREPL started as the section's host program, /proc/net/tcp as
   the activity probe. Only the swarm is a stub (no hive host in a test JVM)
   and the clock is the test's."
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [hive-addon.protocol :as addon]
            [hive-compose.addon :as compose]
            [hive-compose.adapter.cli :as cli]
            [hive-compose.port :as port]
            [hive-compose.stub-swarm :as stub-swarm])
  (:import (java.net ServerSocket Socket)))

;; SPDX-License-Identifier: MIT

(def minute 60000)

(def compose-yml
  "services:
  db:
    image: busybox:1.36
    command: [\"sleep\", \"600\"]
  api:
    image: busybox:1.36
    command: [\"sleep\", \"600\"]
    depends_on: [db]
")

(defn- tmp-dir [prefix]
  (str (java.nio.file.Files/createTempDirectory prefix (make-array java.nio.file.attribute.FileAttribute 0))))

(defn- free-port
  "A port the OS hands out now, so the test never binds a fixed one."
  []
  (with-open [s (ServerSocket. 0)] (.getLocalPort s)))

(defn- listening? [p]
  (try (with-open [s (Socket.)] (.connect s (java.net.InetSocketAddress. "127.0.0.1" (int p)) 250) true)
       (catch java.io.IOException _refused-means-not-listening false)))

(defn- await-until [pred ms]
  (loop [left ms]
    (cond (pred) true
          (<= left 0) false
          :else (do (Thread/sleep 250) (recur (- left 250))))))

(defn- nrepl-eval
  "Evaluate `code` over a bencode nREPL connection `s`; the raw reply text."
  [^Socket s code]
  (let [out (.getOutputStream s)
        in (.getInputStream s)
        msg (str "d2:op4:eval4:code" (count (.getBytes ^String code "UTF-8")) ":" code "2:id1:1e")]
    (.write out (.getBytes msg "UTF-8"))
    (.flush out)
    (.setSoTimeout s 15000)
    (let [buf (byte-array 8192) sb (StringBuilder.)]
      (loop []
        (let [n (.read in buf)]
          (when (pos? n) (.append sb (String. buf 0 n "UTF-8")))
          (if (or (neg? n) (str/includes? (str sb) "4:done"))
            (str sb)
            (recur)))))))

(defn- world []
  (let [root (tmp-dir "hive-compose-e2e")
        repl-dir (str root "/repl")
        project (str "hivecomposee2e" (System/currentTimeMillis))
        port (free-port)
        clock (atom 1000000)
        swarm (stub-swarm/swarm [])]
    (spit (io/file root "compose.yml") compose-yml)
    (io/make-parents (io/file repl-dir "src" "app" "core.clj"))
    (spit (io/file repl-dir "src" "app" "core.clj") "(ns app.core)")
    {:root root :repl-dir repl-dir :project project :port port :clock clock :swarm swarm
     :addon (compose/make-addon
             {:compose/config-source (constantly {})
              :compose/projects {"e2e" {:project/dir root
                                        :project/name project
                                        :project/programs [{:program/id "repl"
                                                            :program/dir repl-dir
                                                            :program/command ["bb" "nrepl-server" (str port)]
                                                            :program/nrepl-port port
                                                            :program/with ["api"]}]}}
              :compose/profiles-file (str root "/no-profiles.edn")
              :compose/state-file (str root "/state.edn")
              :compose/log-dir (str root "/logs")
              :compose/tick-seconds 3600
              :compose/initial-delay-ms 3600000
              :compose/adopt? false
              :compose/systemd-scope? false
              :compose/swarm swarm
              :compose/now #(deref clock)})}))

(defn- call
  "The parsed answer of the compose tool for `params`."
  [w params]
  (let [res ((:handler (first (addon/tools (:addon w)))) params)]
    (assoc (json/read-str (get-in res [:content 0 :text]) :key-fn keyword)
           ::error? (boolean (:isError res)))))

(defn- running [w]
  (:ok (port/-running (cli/make-engine {}) {:profile/id "x" :profile/dir (:root w) :profile/project (:project w)})))

(defn- cleanup! [w]
  (try (addon/shutdown! (:addon w)) (catch Throwable _cleanup-is-best-effort nil))
  (port/-down! (cli/make-engine {}) {:profile/id "x" :profile/dir (:root w) :profile/project (:project w)}
               ["api" "db"])
  (when (listening? (:port w))
    ;; the section's program outlived a failed assertion: stop it by its port's pid
    (try (.waitFor (.start (ProcessBuilder. ["fuser" "-k" (str (:port w) "/tcp")]))) (catch Throwable _fuser-absent-cleanup-is-best-effort nil))))

(deftest ^:integration a-claimed-app-is-shared-kept-and-freed-when-nobody-uses-it
  (let [w (world)
        a (:addon w)]
    (try
      (is (:success? (addon/initialize! a {})))
      (testing "claim brings the app and its JVM up and names the shared nREPL"
        (let [res (call w {"command" "claim" "target" "e2e/api" "agent" "ling-1" "note" "fix login"})]
          (is (not (::error? res)) (pr-str res))
          (is (= ["ling-1"] (:claims res)))
          (is (= #{"api" "db"} (running w)))
          (is (await-until #(listening? (:port w)) 60000) "the bb nREPL came up")
          (is (= (:port w) (-> res :nrepl first :nrepl-port)))))
      (testing "nrepl path= routes a module file to that port"
        (let [res (call w {"command" "nrepl" "path" (str (:repl-dir w) "/src/app/core.clj")})]
          (is (= (:port w) (:nrepl-port res)))
          (is (true? (:nrepl-ready? res)))
          (is (str/includes? (:advice res) "do not start another JVM"))))
      (testing "an agent drives the shared nREPL; the reaper sees the connection as use"
        (with-open [s (Socket. "127.0.0.1" (int (:port w)))]
          (is (str/includes? (nrepl-eval s "(+ 1 2)") "5:value1:3"))
          (compose/tick! a)
          (is (some #(= (:port w) (:port %))
                    (-> (call w {"command" "who"}) :sections first :used-by))
              "the open connection to the nREPL counts")))
      (testing "an agent that starts working in the module is told which port to use, once"
        (stub-swarm/agents! (:swarm w) [{:agent/id "ling-2" :agent/cwd (str (:repl-dir w) "/src")}])
        (compose/tick! a)
        (compose/tick! a)
        (let [routes (filter #(and (= :nrepl/route (first %)) (= "ling-2" (:to (second %))))
                             (stub-swarm/notices (:swarm w)))]
          (is (= 1 (count routes)))
          (is (= "ling-2" (:agent (second (first routes)))))
          (is (= (:port w) (-> routes first second :nrepl first :nrepl-port)))))
      (testing "95 minutes on, past the 60 minute TTL: renewed claim and working agent keep it up"
        (dotimes [_ 2]
          (swap! (:clock w) + (* 25 minute))
          (call w {"command" "claim" "target" "e2e/api" "agent" "ling-1"})
          (swap! (:clock w) + (* 22 minute))
          (is (empty? (:reaped (compose/tick! a)))))
        (is (= #{"api" "db"} (running w))))
      (testing "work ends: release, the agent leaves, the orphan TTL frees containers and JVM"
        (let [res (call w {"command" "release" "target" "e2e/api" "agent" "ling-1"})]
          (is (true? (:orphaned? res))))
        (stub-swarm/agents! (:swarm w) [])
        (swap! (:clock w) + (* 11 minute))
        (let [pass (compose/tick! a)]
          (is (= ["e2e/api"] (mapv :profile/id (:reaped pass))) (pr-str pass)))
        (is (= #{} (running w)) "containers stopped")
        (is (await-until #(not (listening? (:port w))) 20000) "the shared JVM was halted")
        (is (some #(= :section/reaped (first %)) (stub-swarm/notices (:swarm w))) "the swarm was told"))
      (finally (cleanup! w)))))

(deftest ^:integration release-stop-frees-the-app-at-once
  (let [w (world)
        a (:addon w)]
    (try
      (addon/initialize! a {})
      (call w {"command" "claim" "target" "e2e/api" "agent" "ling-1"})
      (is (await-until #(listening? (:port w)) 60000))
      (let [res (call w {"command" "release" "target" "e2e/api" "agent" "ling-1" "stop" true})]
        (is (not (::error? res)) (pr-str res))
        (is (= #{} (running w)))
        (is (await-until #(not (listening? (:port w))) 20000)))
      (finally (cleanup! w)))))
