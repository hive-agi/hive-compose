(ns hive-compose.adapter.runner-test
  "The docker / host ISuiteRunner over a recording exec: argv shape (names
   only in argv, values in the process env), secrets read at spawn and
   scrubbed from the tail, the container removed whatever happens."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.adapter.runner :as runner]
            [hive-compose.port :as port]
            [hive-compose.promote.env]
            [hive-dsl.result :as r]
            [hive-system.secrets.core :as sec]
            [hive-system.secrets.protocols :as proto]))

;; SPDX-License-Identifier: MIT

(defn- tmp [] (str (System/getProperty "java.io.tmpdir") "/hc8-runner-" (System/nanoTime)))

(def secret "s3cr3t-value")

(def resolver
  {:pass (reify proto/ISecretBackend
           (backend-id [_] :pass)
           (fetch [_ path _]
             (if (= path "ci/token")
               (r/ok (sec/make-secret secret :pass path))
               (r/err :pass/not-found {})))
           (available? [_] (r/ok true)))})

(defn- recording [answers]
  (let [calls (atom [])]
    [calls (fn [argv opts]
             (swap! calls conj [argv opts])
             (let [k (second argv)]
               (or (get answers [k (nth argv 2 nil)]) (get answers k)
                   (r/ok {:exit 0 :stdout "" :stderr ""}))))]))

(defn- spec []
  {:runner :container :image "img:1" :name "hive-r01-smoke-shop" :network "hive-r01-suite-smoke"
   :mounts [{:host (tmp) :container "/reports"}] :labels {"hive.run" "r01" "hive.consumer" "suite:smoke"}
   :env {"TOKEN" {:pass "ci/token"} "HIVE_RUN_ID" "r01"} :command "pytest -q"
   :reports-dir (tmp) :timeout-ms 5000})

(deftest create-argv-carries-names-never-values
  (let [argv (runner/create-argv (spec))]
    (is (= ["docker" "create" "--name" "hive-r01-smoke-shop" "--network" "hive-r01-suite-smoke"
            "--label" "hive.consumer=suite:smoke" "--label" "hive.run=r01"]
           (take 10 argv)))
    (is (= ["-e" "HIVE_RUN_ID" "-e" "TOKEN" "img:1" "sh" "-c" "pytest -q"] (take-last 8 argv)))
    (is (some #(str/starts-with? % "type=bind,source=") argv) "--mount, so a : in a path is safe")
    (is (not-any? #(str/includes? % "ci/token") argv))))

(defspec env-args-are-names-only 50
  (prop/for-all [env (gen/map (gen/fmap #(str "V" %) gen/string-alphanumeric) gen/string-alphanumeric)]
    (= (vec (mapcat (fn [k] ["-e" k]) (sort (keys env)))) (runner/env-args env))))

(deftest a-container-run-resolves-at-spawn-scrubs-and-removes
  (let [[calls exec] (recording {["start" "-a"] (r/ok {:exit 3 :stdout (str "token " secret "\nFAIL x") :stderr ""})})
        rn (runner/make-runner {:exec exec :compose/resolver resolver :now (constantly 7)})
        res (port/-run-container! rn (spec))
        argvs (mapv first @calls)]
    (is (r/ok? res) (pr-str res))
    (is (= 3 (get-in res [:ok :exit])) "a failing suite is an answer, not an error")
    (is (= ["token pass:ci/token" "FAIL x"] (get-in res [:ok :log-tail])) "the secret is scrubbed")
    (is (= [["docker" "image"] ["docker" "rm"] ["docker" "create"] ["docker" "start"] ["docker" "rm"]]
           (mapv #(vec (take 2 %)) argvs)) "image present: no pull")
    (is (= secret (get-in (nth @calls 2) [1 :env "TOKEN"])) "the value travels in the docker process env")
    (is (= 5000 (get-in (nth @calls 3) [1 :timeout-ms])))))

(deftest a-missing-image-is-pulled-under-its-own-timeout
  (let [[calls exec] (recording {["image" "inspect"] (r/ok {:exit 1 :stdout "" :stderr "No such image"})})
        res (port/-run-container! (runner/make-runner {:exec exec :compose/resolver resolver}) (spec))
        pull (first (filter #(= "pull" (second (first %))) @calls))]
    (is (r/ok? res) (pr-str res))
    (is (= ["docker" "pull" "img:1"] (first pull)))
    (is (= runner/default-pull-timeout-ms (get-in pull [1 :timeout-ms])) "not the 5 s run timeout"))
  (let [[calls exec] (recording {["image" "inspect"] (r/ok {:exit 1 :stdout "" :stderr ""})
                                 "pull" (r/err :shell/timeout {:timeout-ms 600000})})
        res (port/-run-container! (runner/make-runner {:exec exec :compose/resolver resolver}) (spec))]
    (is (= :shell/timeout (:error res)))
    (is (fn? (:scrub res)) "errors carry :scrub too")
    (is (not-any? #(= "create" (second (first %))) @calls))
    (is (= ["docker" "rm" "-f" "hive-r01-smoke-shop"] (first (last @calls))))))

(deftest an-unreadable-secret-starts-nothing
  (let [[calls exec] (recording {})
        rn (runner/make-runner {:exec exec :compose/resolver resolver})
        res (port/-run-container! rn (assoc-in (spec) [:env "TOKEN"] {:pass "nope"}))]
    (is (= :compose/secret-unresolved (:error res)))
    (is (empty? @calls))))

(deftest a-timeout-still-removes-the-container
  (let [[calls exec] (recording {["start" "-a"] (r/err :shell/timeout {:timeout-ms 5000})})
        res (port/-run-container! (runner/make-runner {:exec exec :compose/resolver resolver}) (spec))]
    (is (= :shell/timeout (:error res)))
    (is (nil? (:exit res)))
    (is (= ["docker" "rm" "-f" "hive-r01-smoke-shop"] (first (last @calls))))))

(deftest a-failed-or-throwing-create-still-removes-the-container
  (doseq [answer [(r/err :shell/timeout {:timeout-ms 60000})
                  (r/ok {:exit 125 :stdout "" :stderr (str "bad " secret)})]]
    (let [[calls exec] (recording {["create" "--name"] answer})
          res (port/-run-container! (runner/make-runner {:exec exec :compose/resolver resolver}) (spec))]
      (is (r/err? res))
      (is (not (str/includes? (pr-str res) secret)) "stderr scrubbed")
      (is (= ["docker" "rm" "-f" "hive-r01-smoke-shop"] (first (last @calls))))))
  (let [calls (atom [])
        exec (fn [argv opts]
               (swap! calls conj argv)
               (if (= "create" (second argv)) (throw (ex-info (str "boom " secret) {})) (r/ok {:exit 0 :stdout "" :stderr ""})))
        res (port/-run-container! (runner/make-runner {:exec exec :compose/resolver resolver}) (spec))]
    (is (= :compose/runner-threw (:error res)) "never throws")
    (is (not (str/includes? (pr-str res) secret)))
    (is (= ["docker" "rm" "-f" "hive-r01-smoke-shop"] (last @calls)))))

(deftest the-runner-scrub-hides-resolved-values
  (let [[_ exec] (recording {})
        res (port/-run-container! (runner/make-runner {:exec exec :compose/resolver resolver}) (spec))]
    (is (= "x pass:ci/token" ((get-in res [:ok :scrub]) (str "x " secret))))))

(deftest extra-networks-are-connected-before-start
  (let [[calls exec] (recording {})
        _ (port/-run-container! (runner/make-runner {:exec exec :compose/resolver resolver})
                                (assoc (spec) :extra-networks ["hive-r01-suite-smoke2"]))]
    (is (= ["docker" "network" "connect" "hive-r01-suite-smoke2" "hive-r01-smoke-shop"] (first (nth @calls 3))))
    (is (= "start" (second (first (nth @calls 4)))) "connected before start")))

(deftest a-failing-connect-still-removes-the-container
  (let [[calls exec] (recording {["network" "connect"] (r/ok {:exit 1 :stdout "" :stderr "no such network"})})
        res (port/-run-container! (runner/make-runner {:exec exec :compose/resolver resolver})
                                  (assoc (spec) :extra-networks ["gone"]))]
    (is (r/err? res))
    (is (not-any? #(= "start" (second (first %))) @calls))
    (is (= ["docker" "rm" "-f" "hive-r01-smoke-shop"] (first (last @calls))))))

(deftest a-host-command-that-throws-is-an-error-with-scrub
  (let [exec (fn [_ _] (throw (ex-info (str "boom " secret) {})))
        res (port/-run-command! (runner/make-runner {:exec exec :compose/resolver resolver})
                                {:runner :command :command "x" :env {"TOKEN" {:pass "ci/token"}} :reports-dir (tmp)})]
    (is (= :compose/runner-threw (:error res)))
    (is (not (str/includes? (pr-str (dissoc res :scrub)) secret)))
    (is (fn? (:scrub res)))))

(deftest scrub-replaces-the-longest-secret-first
  (is (= "[pass:a/long]" (hive-compose.promote.env/scrub {"A" {:pass "a/short"} "B" {:pass "a/long"}}
                                                         {"A" "tok" "B" "tok-extended"}
                                                         "[tok-extended]"))))

(deftest a-host-command-runs-in-its-dir-with-its-env
  (let [[calls exec] (recording {"run" (r/ok {:exit 0 :stdout "ok" :stderr ""})})
        rn (runner/make-runner {:exec exec :compose/resolver resolver})
        res (port/-run-command! rn {:runner :command :command ["k6" "run" "x.js"] :dir "/w/load"
                                    :env {"TOKEN" {:pass "ci/token"}} :reports-dir (tmp)})
        [argv opts] (first @calls)]
    (is (= 0 (get-in res [:ok :exit])))
    (is (= ["k6" "run" "x.js"] argv))
    (is (= "/w/load" (:dir opts)))
    (is (= secret (get-in opts [:env "TOKEN"])))
    (is (= runner/default-timeout-ms (:timeout-ms opts)))
    (is (= ["sh" "-c" "make e2e"] (runner/command-argv "make e2e")))))
