(ns hive-compose.promote.program-test
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.promote.program :as program]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def shadow-facts
  {"shadow-cljs.edn" {:nrepl {:port 7902}
                      :builds {:app {} :test {}}}
   "deps.edn" {:aliases {}}})

(deftest a-shadow-directory-watches-its-builds-and-names-its-nrepl
  (is (= {:program/kind :shadow
          :program/command ["npx" "shadow-cljs" "watch" "app" "test"]
          :program/nrepl-port 7902}
         (program/detect shadow-facts))
      "shadow-cljs.edn wins over the deps.edn beside it"))

(deftest a-tagged-port-yields-its-default
  (let [tagged (tagged-literal 'shadow/env ["SHADOW_NREPL" :default 7903])]
    (is (= 7903 (program/port-value tagged)))
    (is (= 7903 (:program/nrepl-port
                 (program/detect {"shadow-cljs.edn" {:nrepl {:port tagged} :builds {:app {}}}}))))
    (is (nil? (program/port-value (tagged-literal 'shadow/env "SHADOW_NREPL"))))
    (is (nil? (program/port-value nil)))))

(deftest a-deps-directory-starts-with-an-nrepl
  (testing "its own :nrepl alias, with the port the alias declares"
    (is (= {:program/kind :deps
            :program/command ["clojure" "-M:nrepl"]
            :program/nrepl-port 7999}
           (program/detect {"deps.edn" {:aliases {:nrepl {:main-opts ["-m" "nrepl.cmdline" "--port" "7999"]}}}}))))
  (testing "no alias: a plain nrepl.cmdline on the port asked for"
    (let [d (program/detect {"deps.edn" {}} nil 7950)]
      (is (= :deps (:program/kind d)))
      (is (= 7950 (:program/nrepl-port d)))
      (is (= ["--port" "7950"] (take-last 2 (:program/command d))))
      (is (some #{"nrepl.cmdline"} (:program/command d)))))
  (testing "no alias, no port: nREPL picks one and writes .nrepl-port"
    (let [d (program/detect {"deps.edn" {}})]
      (is (nil? (:program/nrepl-port d)))
      (is (not-any? #{"--port"} (:program/command d)))))
  (testing "an alias on another port than the one asked for is not used"
    (let [d (program/detect {"deps.edn" {:aliases {:nrepl {:main-opts ["--port" "7999"]}}}} nil 7000)]
      (is (= 7000 (:program/nrepl-port d)))
      (is (not= ["clojure" "-M:nrepl"] (:program/command d))))))

(deftest lein-and-babashka-run-their-repl-servers
  (is (= ["lein" "repl" ":headless" ":port" "7001"]
         (:program/command (program/detect {"project.clj" true} nil 7001))))
  (is (= ["lein" "repl" ":headless"] (:program/command (program/detect {"project.clj" true}))))
  (is (= {:program/kind :bb :program/command ["bb" "nrepl-server" "1667"] :program/nrepl-port 1667}
         (program/detect {"bb.edn" {}}))))

(deftest kind-can-be-forced-in-a-directory-holding-several
  (let [d (program/detect (assoc-in shadow-facts ["deps.edn" :aliases :nrepl :main-opts] ["-p" "7999"])
                          :deps nil)]
    (is (= :deps (:program/kind d)))
    (is (= 7999 (:program/nrepl-port d)))))

(deftest resolved-completes-a-program
  (testing "a declared command wins; the nREPL port is still detected"
    (let [res (program/resolved {:program/id "web" :program/dir "/w/web"
                                 :program/command "clojure -M:modules-watch"}
                                shadow-facts)]
      (is (r/ok? res))
      (is (= "clojure -M:modules-watch" (:program/command (:ok res))))
      (is (= 7902 (:program/nrepl-port (:ok res))))
      (is (= :shadow (:program/kind (:ok res))))))
  (testing "a directory with no Clojure build file needs a command"
    (is (r/err? (program/resolved {:program/id "x" :program/dir "/w/x"} {})))
    (let [res (program/resolved {:program/id "x" :program/dir "/w/x" :program/command ["make" "run"]} {})]
      (is (= :plain (:program/kind (:ok res))))
      (is (not (contains? (:ok res) :program/nrepl-port))))))

(deftest wanted-pairs-programs-with-services
  (let [programs [{:program/id "web" :program/with ["envoy"]}
                  {:program/id "inv" :program/with ["inventory-dev"]}
                  {:program/id "always"}]]
    (is (= ["web" "always"] (mapv :program/id (program/wanted programs ["envoy" "postgres"]))))
    (is (= ["always"] (mapv :program/id (program/wanted programs ["postgres"]))))))

(deftest release-programs-spares-what-others-hold
  (let [run {:pid 1}
        active {"a" {:project "shop" :programs {"web" run "inv" run}}
                "b" {:project "shop" :programs {"web" run}}
                "c" {:project "blog" :programs {"inv" run}}}]
    (is (= #{"inv"} (set (keys (program/release-programs active "a" #{}))))
        "web is held by b; c holds inv in another project")
    (is (= #{} (set (keys (program/release-programs active "a" #{"inv"})))))
    (is (= {} (program/release-programs active "missing" #{})))))

(deftest the-detached-script-quotes-every-word
  (is (= "exec setsid 'npx' 'shadow-cljs' 'watch' 'app' >> '/l/x.log' 2>&1 < /dev/null"
         (program/detached-script ["npx" "shadow-cljs" "watch" "app"] "/l/x.log")))
  (is (= "exec setsid 'sh' '-c' 'echo '\\''hi'\\''' >> '/l/x.log' 2>&1 < /dev/null"
         (program/detached-script "echo 'hi'" "/l/x.log"))))
