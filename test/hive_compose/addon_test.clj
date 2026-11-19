(ns hive-compose.addon-test
  (:require [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [hive-addon.protocol :as addon]
            [hive-compose.addon :as sut]
            [hive-compose.stub :as stub]))

;; SPDX-License-Identifier: MIT

(defn- temp-dir []
  (str (java.nio.file.Files/createTempDirectory "hive-compose-test"
                                                (make-array java.nio.file.attribute.FileAttribute 0))))

(defn- config [dir engine]
  {:compose/profiles [{:profile/id "api" :profile/dir "/w/shop"}
                      {:profile/id "blog" :profile/dir "/w/blog"}]
   :compose/profiles-file (str dir "/absent.edn")
   :compose/state-file (str dir "/state.edn")
   :compose/engine engine
   :compose/initial-delay-ms 3600000})

(defn- call [a params]
  (let [{:keys [content isError]} ((:handler (first (addon/tools a))) params)]
    {:error? (boolean isError)
     :body (json/read-str (:text (first content)) :key-fn keyword)}))

(deftest construction-is-pure
  (let [a (sut/addon-ctor {})]
    (is (= "hive.compose" (addon/addon-id a)))
    (is (= :new (:lifecycle @(:state a))))
    (is (nil? (:scheduler @(:state a))))))

(deftest lifecycle-is-idempotent-and-stops-the-reaper
  (let [dir (temp-dir)
        a (sut/make-addon (config dir (stub/engine {})))]
    (is (:success? (addon/initialize! a {})))
    (is (:already-initialized? (addon/initialize! a {})))
    (let [sched (:scheduler @(:state a))]
      (is (= :ok (:status (addon/health a))))
      (addon/shutdown! a)
      (addon/shutdown! a)
      (is (.isShutdown ^java.util.concurrent.ExecutorService sched))
      (is (= :down (:status (addon/health a)))))))

(deftest tool-drives-profiles-and-persists-state
  (let [dir (temp-dir)
        engine (stub/engine {"api" ["api" "db"] "blog" ["ghost"]})
        a (sut/make-addon (config dir engine))]
    (addon/initialize! a {})
    (try
      (is (= ["api" "db"] (:services (:body (call a {"command" "up" "profile" "api"})))))
      (is (= "blog" (:profile (:body (call a {:command "switch" :profile "blog"})))))
      (is (= #{} (stub/running engine "shop")))
      (is (= "blog" (:current (edn/read-string (slurp (io/file dir "state.edn"))))))
      (testing "errors are flagged"
        (is (:error? (call a {"command" "up" "profile" "nope"})))
        (is (:error? (call a {"command" "up"})))
        (is (:error? (call a {"command" "bogus"}))))
      (testing "a restart picks the active map back up"
        (addon/shutdown! a)
        (let [b (sut/make-addon (config dir engine))]
          (addon/initialize! b {})
          (is (= ["blog"] (keys (:active @(:state (:ctx @(:state b)))))))
          (addon/shutdown! b)))
      (finally (addon/shutdown! a)))))

(deftest invalid-config-fails-initialize
  (let [a (sut/make-addon {:compose/profiles [{:profile/id "x"}]
                           :compose/profiles-file "/nonexistent/x.edn"})]
    (is (false? (:success? (addon/initialize! a {}))))
    (is (:error? (call a {"command" "status"})))))

(deftest first-tick-adopts-then-reaps
  (let [dir (temp-dir)
        engine (stub/engine {"blog" ["ghost"]} {"blog" #{"ghost"}})
        a (sut/make-addon (config dir engine))]
    (addon/initialize! a {})
    (try
      (is (= ["blog"] (:adopted (sut/tick! a))))
      (is (nil? (:adopted (sut/tick! a))) "adoption runs once")
      (finally (addon/shutdown! a)))))

(deftest reload-reads-the-config-declared-now
  (testing "an addon block edited after init (config A -> B) is what reload! runs on"
    (let [dir (temp-dir)
          declared (io/file dir "addon-config.edn")
          block-a {:compose/profiles [{:profile/id "api" :profile/dir "/w/shop"}]}
          block-b {:compose/profiles [{:profile/id "escala-dev" :profile/dir "/w/escala"}]}
          _ (spit declared (pr-str block-a))
          a (sut/make-addon (assoc (config dir (stub/engine {}))
                                   :compose/profiles nil
                                   :compose/config-source #(edn/read-string (slurp declared))))]
      ;; the host hands initialize! the block declared at that time merged in
      (is (:success? (addon/initialize! a block-a)))
      (try
        (let [same (:body (call a {"command" "reload"}))]
          (is (= ["api"] (:profiles same)))
          (is (= {} (:changed same)))
          (is (string? (:note same)) "an unchanged reload says nothing changed"))
        (spit declared (pr-str block-b))
        (let [res (sut/reload! a)]
          (is (= ["escala-dev"] (:profiles (:ok res))))
          (is (= :config-source (:source (:ok res))))
          (is (= {:profiles {:added ["escala-dev"] :removed ["api"]}} (:changed (:ok res))))
          (is (seq (:not-reloaded (:ok res))) "what a reload cannot apply is stated")
          (is (= ["escala-dev"] (:profiles (:body (call a {"command" "reload"})))))
          (is (= #{"escala-dev"} (set (keys (get-in (:ctx @(:state a)) [:settings :compose/profiles]))))
              "B's profile is in the running settings and A's is gone"))
        (finally (addon/shutdown! a))))))

(deftest reload-applies-project-env-and-files-to-active-sections
  (testing "an edited :project/env and :project/files reach new targets and the sections already up"
    (let [dir (temp-dir)
          declared (io/file dir "addon-config.edn")
          proj (fn [env files] {:compose/projects [{:project/id "dc" :project/dir "/w/dc"
                                                   :project/files files :project/env env}]})
          block-a (proj {"SRC" "/a"} ["base.yml"])
          block-b (proj {"SRC" "/b" "TOKEN" {:pass "x/token"}} ["base.yml" "extra.yml"])
          engine (stub/engine {} {} {"dc" {"web" {} "db" {}}})
          _ (spit declared (pr-str block-a))
          a (sut/make-addon (assoc (config dir engine)
                                   :compose/profiles nil
                                   :compose/config-source #(edn/read-string (slurp declared))))]
      (is (:success? (addon/initialize! a block-a)))
      (try
        (is (not (:error? (call a {"command" "up" "target" "dc/web" "env" {"CALL" "1"}}))))
        (spit declared (pr-str block-b))
        (let [rep (:body (call a {"command" "reload"}))
              snap (get-in @(:state (:ctx @(:state a))) [:active "dc/web" :profile])]
          (is (= {:changed ["dc"]} (get-in rep [:changed :projects])))
          (is (= ["dc/web"] (get-in rep [:sections :rebased])))
          (is (= ["base.yml" "extra.yml"] (:profile/files snap)))
          (is (= {"SRC" "/b" "TOKEN" {:pass "x/token"} "CALL" "1"} (:profile/env snap))
              "project env follows the config; the call's own env stays")
          (is (not (re-find #"x/token" (pr-str (:changed rep))))
              "the report names projects, never env values"))
        (let [p (:ok (hive-compose.pipeline.ops/resolve-target (:ctx @(:state a)) "dc/db"))]
          (is (= {"SRC" "/b" "TOKEN" {:pass "x/token"}} (:profile/env p)))
          (is (= ["base.yml" "extra.yml"] (:profile/files p))))
        (finally (addon/shutdown! a))))))
