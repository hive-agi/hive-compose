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

(deftest a-preset-carries-an-overlay-file-and-pass-env-of-its-own
  (testing "an overlay -f file and {:pass} env on a preset apply to that preset only, not to the project"
    (let [files ["docker-compose.yml" "docker-compose.override.yml" "docker-compose.crm-whatsapp.yml"]
          s (:ok (config/settings
                  {:compose/projects [{:project/id "dc" :project/dir "~/dc"
                                       :project/files ["docker-compose.yml" "docker-compose.override.yml"]}]}
                  [{:profile/id "crm-whatsapp" :profile/dir "~/dc" :profile/project "dc"
                    :profile/files files
                    :profile/services ["sisf-crm"]
                    :profile/env {"CRM_WA_ACCESS_TOKEN" {:pass "Funeraria/Whatsapp/token"}}}]
                  home))
          p (get-in s [:compose/profiles "crm-whatsapp"])]
      (is (= files (:profile/files p)))
      (is (= {"CRM_WA_ACCESS_TOKEN" {:pass "Funeraria/Whatsapp/token"}} (:profile/env p)))
      (is (= "dc" (:profile/project p)) "same compose project, so it shares and replaces dc's containers")
      (is (= ["docker-compose.yml" "docker-compose.override.yml"]
             (get-in s [:compose/projects "dc" :project/files])))
      (is (nil? (get-in s [:compose/projects "dc" :project/env]))))))
