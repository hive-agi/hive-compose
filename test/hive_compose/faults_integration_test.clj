(ns hive-compose.faults-integration-test
  "Against the real docker daemon: one toxiproxy interposed between two
   consumers' view networks and their upstreams, the way
   hive-compose.promote.faults plans it, driven through the HTTP adapter."
  (:require [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [hive-compose.adapter.toxiproxy :as http]
            [hive-compose.pipeline.faults :as pipeline]
            [hive-compose.port :as port]
            [hive-compose.promote.faults :as faults]
            [hive-compose.schema :as schema]))

;; SPDX-License-Identifier: MIT

(defn- docker [& args]
  (let [{:keys [exit out err]} (apply sh/sh "docker" args)]
    {:exit exit :out (str/trim out) :err err}))

(defn- fetch
  "What a throwaway container on `network` reads from `url` (nil on failure)
   and how long it took, ms."
  [network url]
  (let [t0 (System/currentTimeMillis)
        {:keys [exit out]} (docker "run" "--rm" "--network" network "busybox:1.36"
                                   "wget" "-q" "-T" "10" "-O" "-" url)]
    {:body (when (zero? exit) out) :ms (- (System/currentTimeMillis) t0)}))

(def settings
  {:compose/projects
   {"db" {:project/id "db" :project/dir "/w/db" :project/provides {"web" {:service "web" :port 8080}}}
    "a" {:project/id "a" :project/dir "/w/a" :project/needs {"web" {:via :toxiproxy}}}
    "b" {:project/id "b" :project/dir "/w/b" :project/needs {"web" {:share? false :via :toxiproxy}}}}})

(def resolution
  {:resolution/instances
   {"db/web" {:instance/id "db/web" :instance/capability "web" :instance/provider "db"
              :instance/service "web" :instance/share? true :instance/consumers ["a"]
              :instance/aliases ["web"]}
    "db/web@b" {:instance/id "db/web@b" :instance/capability "web" :instance/provider "db"
                :instance/service "web" :instance/share? false :instance/consumers ["b"]
                :instance/aliases ["web"] :instance/compose-project "db-b-it"}}
   :resolution/bindings {"a" {"web" "db/web"} "b" {"web" "db/web@b"}}})

(deftest ^:integration toxiproxy-interposes-on-view-networks
  (let [run (str "it" (System/currentTimeMillis))
        p (:ok (faults/plan settings resolution run))
        nets (into [(:faults/network p)] (map #(schema/view-network run %) ["a" "b"]))
        tp (str "hive-tp-" run)
        ups {"db/web" (str "hive-up1-" run) "db/web@b" (str "hive-up2-" run)}]
    (try
      (doseq [n nets] (docker "network" "create" "--label" (str "hive.run=" run) n))
      (doseq [[id nm] ups]
        (docker "run" "-d" "--name" nm "--network" (:faults/network p)
                "--network-alias" (faults/upstream-alias id) "busybox:1.36" "sh" "-c"
                (str "mkdir -p /w && echo " (if (= id "db/web") "shared" "private")
                     " > /w/index.html && httpd -f -p 8080 -h /w")))
      (docker "run" "-d" "--name" tp "--network" (:faults/network p) "-p" "127.0.0.1::8474" faults/image)
      (doseq [{:keys [network instance aliases]} (faults/joins p)
              :when (= instance faults/instance-id)]
        (apply docker "network" "connect" (concat (mapcat (fn [a] ["--alias" a]) aliases) [network tp])))
      (let [url (str "http://" (:out (docker "port" tp "8474")))
            ctx {:faults (atom {})}]
        (Thread/sleep 1000)
        (is (= ["a.web" "b.web"] (sort (map #(get % "name") (:proxies (:ok (pipeline/install! ctx p url)))))))
        (testing "each consumer reaches its own instance under the original alias"
          (is (= "shared" (:body (fetch (schema/view-network run "a") "http://web:8080/"))))
          (is (= "private" (:body (fetch (schema/view-network run "b") "http://web:8080/")))))
        (testing "the real instance is not reachable from the consumer's view network"
          (is (nil? (:body (fetch (schema/view-network run "a") (str "http://" (faults/upstream-alias "db/web") ":8080/"))))))
        (testing "a latency toxic slows only its proxy"
          (let [base (:ms (fetch (schema/view-network run "a") "http://web:8080/"))]
            (is (contains? (pipeline/faults! ctx {:run run :op "add" :proxy "a.web"
                                                  :toxic {:type "latency" :attributes {"latency" 1500}}})
                           :ok))
            (is (<= (+ base 1000) (:ms (fetch (schema/view-network run "a") "http://web:8080/"))))
            (is (= 1 (count (get-in (:ok (port/-list (http/make-client url))) ["a.web" "toxics"]))))
            (is (= {:reset true} (:ok (pipeline/faults! ctx {:run run :op "reset"}))))))
        (testing "reset_peer cuts the connection"
          (pipeline/faults! ctx {:run run :op "add" :proxy "b.web"
                                 :toxic {:type "reset_peer" :attributes {"timeout" 0}}})
          (is (nil? (:body (fetch (schema/view-network run "b") "http://web:8080/"))))))
      (finally
        (apply docker "rm" "-f" tp (vals ups))
        (doseq [n nets] (docker "network" "rm" n))))))
