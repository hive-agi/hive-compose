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
