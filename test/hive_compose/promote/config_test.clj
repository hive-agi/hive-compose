(ns hive-compose.promote.config-test
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.promote.config :as config]))

;; SPDX-License-Identifier: MIT

(def home "/fake-home")

(deftest defaults-and-home-expansion
  (let [s (:ok (config/settings {:compose/profiles [{:profile/id "api" :profile/dir "~/w/shop"}]} nil home))]
    (is (= 60 (:compose/default-ttl-minutes s)))
    (is (= :stop (:compose/default-idle-action s)))
    (is (= (str home "/w/shop") (get-in s [:compose/profiles "api" :profile/dir])))
    (is (= (str home "/.local/state/hive-compose/state.edn") (:compose/state-file s)))))

(deftest map-shaped-profiles-take-their-key-as-id
  (let [s (:ok (config/settings {} {:api {:profile/dir "/w"}} home))]
    (is (= "api" (get-in s [:compose/profiles "api" :profile/id])))))

(deftest inline-overrides-file
  (let [s (:ok (config/settings {:compose/profiles [{:profile/id "api" :profile/dir "/inline"}]}
                                [{:profile/id "api" :profile/dir "/file"} {:profile/id "web" :profile/dir "/w"}]
                                home))]
    (is (= "/inline" (get-in s [:compose/profiles "api" :profile/dir])))
    (is (= #{"api" "web"} (set (keys (:compose/profiles s)))))))

(deftest invalid-config-is-an-err
  (testing "missing dir"
    (is (= :compose/invalid-config (:error (config/settings {:compose/profiles [{:profile/id "api"}]} nil home)))))
  (testing "bad idle action"
    (is (= :compose/invalid-config
           (:error (config/settings {:compose/profiles [{:profile/id "a" :profile/dir "/w" :profile/idle-action :nuke}]}
                                    nil home))))))
