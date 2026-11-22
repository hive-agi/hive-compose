(ns hive-compose.adapter.cli-stack-test
  "IStackEngine over the docker CLI: pure argv builders, the override file,
   parsers of labelled containers and networks, and the engine over a
   recording exec."
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [hive-compose.adapter.cli :as cli]
            [hive-compose.port :as port]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def labels {"hive.stack" "shop" "hive.run" "shop-3f9a1c"})

(deftest network-argv
  (is (= ["docker" "network" "create" "--driver" "bridge"
          "--label" "hive.run=shop-3f9a1c" "--label" "hive.stack=shop" "hive-shop-3f9a1c-web"]
         (cli/network-create-argv "hive-shop-3f9a1c-web" labels))
      "labels sorted by key")
  (is (= ["docker" "network" "ls" "--filter" "name=^n$" "--format" "{{.Name}}"] (cli/network-ls-argv "n")))
  (is (= ["docker" "network" "connect" "--alias" "kafka" "--alias" "broker" "net" "kafka-kafka-1"]
         (cli/network-connect-argv "net" "kafka-kafka-1" ["kafka" "broker"])))
  (is (= ["docker" "network" "disconnect" "-f" "net" "c"] (cli/network-disconnect-argv "net" "c")))
  (is (= ["docker" "network" "rm" "net"] (cli/network-rm-argv "net"))))

(deftest labelled-argv
  (is (= ["docker" "ps" "-a" "--no-trunc" "--filter" "label=hive.run=shop-3f9a1c"
          "--filter" "label=hive.stack=shop" "--format" "{{json .}}"]
         (cli/labelled-containers-argv labels)))
  (is (= ["docker" "network" "ls" "--filter" "label=hive.run=r1x" "--format" "{{json .}}"]
         (cli/labelled-networks-argv {"hive.run" "r1x"}))))

(def spec {:project "kafka-web-shop-3f9a1c" :dir "/w/kafka" :files ["compose.yml"]
           :services ["kafka"] :labels labels :networks ["hive-shop-3f9a1c-web"]
           :container-env ["HIVE_RUN_ID"] :wait? true :wait-timeout-s 120})

(deftest project-argv
  (is (= ["docker" "compose" "--project-directory" "/w/kafka" "-p" "kafka-web-shop-3f9a1c"
          "-f" "compose.yml" "-f" "/s/o.json" "up" "-d" "--wait" "--wait-timeout" "120" "kafka"]
         (cli/up-project-argv spec "/s/o.json")))
  (is (= ["docker" "compose" "-p" "p" "up" "-d" "a"]
         (cli/up-project-argv {:project "p" :services ["a"]} nil))
      "no dir, no files, no override: the project's own files")
  (is (= ["docker" "compose" "--project-directory" "/w/kafka" "-p" "kafka-web-shop-3f9a1c"
          "-f" "compose.yml" "down" "kafka"]
         (cli/down-project-argv spec nil)))
  (is (= ["docker" "compose" "-p" "p" "down" "--remove-orphans"]
         (cli/down-project-argv {:project "p" :all? true} nil)))
  (is (not-any? #{"-v" "--volumes"} (cli/down-project-argv {:project "p" :all? true} nil))
      "never volumes"))

(deftest the-override
  (is (cli/override? spec))
  (is (not (cli/override? {:project "p" :services ["a"]})))
  (let [doc (cli/override-doc spec)]
    (is (= {"services" {"kafka" {"labels" {"hive.run" "shop-3f9a1c" "hive.stack" "shop"}
                                 "networks" {"hive-shop-3f9a1c-web" {}}
                                 "environment" {"HIVE_RUN_ID" nil}}}
            "networks" {"hive-shop-3f9a1c-web" {"external" true "name" "hive-shop-3f9a1c-web"}}}
           doc))
    (is (= doc (json/read-str (cli/override-json spec))) "JSON round-trips; null keeps values out of the file")))

(deftest parsing
  (is (= {"a" "1" "b" "x=y"} (cli/parse-labels "a=1,b=x=y")))
  (is (= {} (cli/parse-labels "")))
  (is (= :healthy (cli/health-of "Up 2 minutes (healthy)")))
  (is (= :unhealthy (cli/health-of "Up 2 minutes (unhealthy)")))
  (is (= :starting (cli/health-of "Up 3 seconds (health: starting)")))
  (is (= :none (cli/health-of "Up 2 minutes")))
  (let [rows (str (json/write-str {"Names" "k-kafka-1" "State" "running" "Status" "Up 1 minute (healthy)"
                                   "Labels" "com.docker.compose.project=k,com.docker.compose.service=kafka,hive.run=r1x"})
                  "\n"
                  (json/write-str {"Names" "a-app-1" "State" "exited" "Status" "Exited (0)"
                                   "Labels" "com.docker.compose.project=a,com.docker.compose.service=app"}))]
    (is (= [{:name "a-app-1" :project "a" :service "app" :state "exited" :health :none
             :labels {"com.docker.compose.project" "a" "com.docker.compose.service" "app"}}
            {:name "k-kafka-1" :project "k" :service "kafka" :state "running" :health :healthy
             :labels {"com.docker.compose.project" "k" "com.docker.compose.service" "kafka" "hive.run" "r1x"}}]
           (cli/labelled-containers rows))))
  (is (= [{:name "n" :labels {"hive.run" "r1x"}}]
         (cli/labelled-networks (json/write-str {"Name" "n" "Labels" "hive.run=r1x"})))))

;; ---------------------------------------------------------------------------
;; the engine over a recording exec

(defn- docker-exec
  "Exec answering like docker: `nets` an atom #{name}; `attached` {network
   [container]}; every argv (with its opts) recorded in `calls`."
  [nets attached calls]
  (fn [argv opts]
    (swap! calls conj [argv opts])
    (let [ok (fn [s] (r/ok {:exit 0 :stdout s :stderr ""}))
          [a b c] (drop 1 argv)]
      (cond
        (and (= a "network") (= b "ls")) (ok (if-let [n (some #(when (str/starts-with? % "name=^") %) argv)]
                                               (let [nm (subs n 6 (dec (count n)))] (if (@nets nm) nm ""))
                                               ""))
        (and (= a "network") (= b "create")) (do (swap! nets conj (last argv)) (ok "id"))
        (and (= a "network") (= b "inspect")) (ok (str/join "\n" (get attached c)))
        (and (= a "network") (= b "disconnect")) (ok "")
        (and (= a "network") (= b "rm")) (do (swap! nets disj c) (ok c))
        (and (= a "network") (= b "connect"))
        (if (= "dup-1" (last argv))
          (r/ok {:exit 1 :stdout "" :stderr "endpoint with name dup-1 already exists in network n"})
          (ok ""))
        (= a "ps") (ok (if (some #{"label=com.docker.compose.service=gone"} argv) "" "k-kafka-1\n"))
        (= a "compose") (ok "")
        :else (r/ok {:exit 1 :stdout "" :stderr (str "unexpected " argv)})))))

(defn- engine [nets attached calls dir]
  (cli/make-engine {:exec (docker-exec nets attached calls)
                    :compose/resolver {:pass (reify hive-system.secrets.protocols/ISecretBackend
                                               (fetch [_ path _]
                                                 (r/ok (hive-system.secrets.core/make-secret (str "secret-of-" path) path))))}
                    :compose/stack-dir dir}))

(defn- tmp-dir []
  (str (doto (java.nio.file.Files/createTempDirectory "hc-stack" (make-array java.nio.file.attribute.FileAttribute 0)))))

(deftest network-create-is-idempotent
  (let [nets (atom #{}) calls (atom []) e (engine nets {} calls (tmp-dir))]
    (is (= {:network "n" :created? true} (:ok (port/-network-create! e "n" labels))))
    (is (= {:network "n" :created? false} (:ok (port/-network-create! e "n" labels))))
    (is (= 1 (count (filter #(= "create" (get-in % [0 2])) @calls))))))

(deftest network-rm-disconnects-first
  (let [nets (atom #{"n"}) calls (atom []) e (engine nets {"n" ["a-1" "b-1"]} calls (tmp-dir))
        res (port/-network-rm! e "n")]
    (is (= {:network "n" :removed? true :disconnected ["a-1" "b-1"]} (:ok res)))
    (is (= ["disconnect" "disconnect" "rm"]
           (keep #(#{"disconnect" "rm"} (get-in % [0 2])) @calls)))
    (is (= {:network "n" :removed? false :disconnected []} (:ok (port/-network-rm! e "n"))))))

(deftest network-connect
  (let [nets (atom #{"n"}) calls (atom []) e (engine nets {} calls (tmp-dir))]
    (is (= {:network "n" :containers ["k-kafka-1"]}
           (:ok (port/-network-connect! e "n" {:project "k" :service "kafka"} ["kafka"]))))
    (is (= :compose/no-container
           (:error (port/-network-connect! e "n" {:project "k" :service "gone"} ["gone"]))))))

(deftest connect-twice-is-ok
  (let [calls (atom [])
        exec (fn [argv opts]
               (swap! calls conj argv)
               (if (= "ps" (second argv))
                 (r/ok {:exit 0 :stdout "dup-1\n" :stderr ""})
                 ((docker-exec (atom #{}) {} (atom [])) argv opts)))
        e (cli/make-engine {:exec exec :compose/stack-dir (tmp-dir)})]
    (is (r/ok? (port/-network-connect! e "n" {:project "d" :service "dup"} ["dup"])))))

(deftest up-project-writes-the-override-and-resolves-secrets
  (let [dir (tmp-dir) nets (atom #{}) calls (atom []) e (engine nets {} calls dir)
        res (port/-up-project! e (assoc spec :env {"HIVE_RUN_ID" "shop-3f9a1c" "TOKEN" {:pass "shop/token"}}
                                        :container-env ["HIVE_RUN_ID" "TOKEN"]))
        [argv opts] (last @calls)
        path (cli/override-path dir "kafka-web-shop-3f9a1c")]
    (is (r/ok? res))
    (is (= path (:override (:ok res))))
    (is (= ["-f" "compose.yml" "-f" path "up"] (take 5 (drop-while #(not= "-f" %) argv)))
        "the override is the last -f")
    (is (= "secret-of-shop/token" (get-in opts [:env "TOKEN"])) "read at spawn, in the process env")
    (let [file (slurp (io/file path))]
      (is (not (str/includes? file "secret-of"))) "no value in the file"
      (is (= nil (get-in (json/read-str file) ["services" "kafka" "environment" "TOKEN"]))))))

(deftest up-project-without-stack-keys-writes-nothing
  (let [dir (tmp-dir) calls (atom []) e (engine (atom #{}) {} calls dir)
        res (port/-up-project! e {:project "kafka" :dir "/w/kafka" :services ["kafka"] :wait? true})]
    (is (r/ok? res))
    (is (nil? (:override (:ok res))))
    (is (not (.exists (io/file (cli/override-path dir "kafka")))))
    (is (= ["docker" "compose" "--project-directory" "/w/kafka" "-p" "kafka" "up" "-d" "--wait" "kafka"]
           (first (last @calls))))))

(deftest down-project-reuses-the-override
  (let [dir (tmp-dir) calls (atom []) e (engine (atom #{}) {} calls dir)]
    (port/-up-project! e spec)
    (port/-down-project! e (assoc spec :all? true))
    (let [argv (first (last @calls))]
      (is (some #{(cli/override-path dir "kafka-web-shop-3f9a1c")} argv)
          "compose must see the same files to find the external networks")
      (is (= ["down" "--remove-orphans"] (take-last 2 argv))))))

(deftest the-default-compose-file-precedes-the-override
  (testing "pure: compose's own lookup order"
    (is (= "compose.yaml" (cli/default-file (constantly true))))
    (is (= "docker-compose.yml" (cli/default-file #{"docker-compose.yml" "x"})))
    (is (= "compose.yml" (cli/default-file #{"compose.yml" "docker-compose.yaml"})) "compose.* before docker-compose.*")
    (is (= "docker-compose.yaml" (cli/default-file #{"docker-compose.yml" "docker-compose.yaml"})))
    (is (nil? (cli/default-file (constantly false)))))
  (testing "argv: the base file, then the override, then up"
    (is (= ["docker" "compose" "--project-directory" "/w/a" "-p" "a-r1x"
            "-f" "compose.yml" "-f" "/s/a-r1x.override.json" "up" "-d" "app"]
           (cli/up-project-argv {:project "a-r1x" :dir "/w/a" :files ["compose.yml"] :services ["app"]}
                                "/s/a-r1x.override.json"))))
  (testing "engine: a project dir with no compose file is a Result error, nothing runs"
    (let [dir (tmp-dir) calls (atom [])
          e (engine (atom #{}) {} calls dir)
          res (port/-up-project! e {:project "a-r1x" :dir dir :services ["app"] :labels {"hive.run" "r1x"}})]
      (is (= :compose/no-compose-file (:error res)))
      (is (empty? @calls))))
  (testing "engine: the found default file is named before the override"
    (let [proj (tmp-dir) _ (spit (io/file proj "docker-compose.yml") "services: {}\n")
          dir (tmp-dir) calls (atom [])
          e (engine (atom #{}) {} calls dir)
          _ (port/-up-project! e {:project "a-r1x" :dir proj :services ["app"] :labels {"hive.run" "r1x"}})
          argv (first (last @calls))]
      (is (= ["-f" "docker-compose.yml" "-f" (cli/override-path dir "a-r1x") "up"]
             (take 5 (drop-while #(not= "-f" %) argv)))))))

(deftest stack-dir-beside-the-state-file
  (is (= "/home/u/.local/state/hive-compose/stacks"
         (cli/stack-dir {:compose/state-file "/home/u/.local/state/hive-compose/state.edn"}))))
