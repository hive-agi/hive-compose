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

(deftest flake-history-settings-are-additive
  (let [s (:ok (config/settings {} nil home))]
    (is (= (str home "/.local/state/hive-compose/flake-history.ednl") (:compose/history-file s)))
    (is (not (contains? s :compose/flaky)) "no thresholds unless configured"))
  (let [s (:ok (config/settings {:compose/state-file "/st/state.edn"
                                 :compose/flaky {:window 10}} nil home))]
    (is (= "/st/flake-history.ednl" (:compose/history-file s)))
    (is (= {:window 10} (:compose/flaky s))))
  (is (= "/h/x.ednl" (:compose/history-file (:ok (config/settings {:compose/history-file "/h/x.ednl"} nil home)))))
  (is (= :compose/invalid-config (:error (config/settings {:compose/flaky {:window 0}} nil home)))))

(deftest map-shaped-profiles-take-their-key-as-id
  (let [s (:ok (config/settings {} {:api {:profile/dir "/w"}} home))]
    (is (= "api" (get-in s [:compose/profiles "api" :profile/id])))))

(deftest inline-overrides-file
  (let [s (:ok (config/settings {:compose/profiles [{:profile/id "api" :profile/dir "/inline"}]}
                                [{:profile/id "api" :profile/dir "/file"} {:profile/id "web" :profile/dir "/w"}]
                                home))]
    (is (= "/inline" (get-in s [:compose/profiles "api" :profile/dir])))
    (is (= #{"api" "web"} (set (keys (:compose/profiles s)))))))

(deftest activity-is-global-with-a-per-project-override
  (let [s (:ok (config/settings {:compose/activity ["nrepl"]
                                 :compose/projects {:shop {:project/dir "/w/shop" :project/activity ["nrepl" "published"]}
                                                    :blog {:project/dir "/w/blog"}}
                                 :compose/profiles [{:profile/id "p" :profile/dir "/w/p" :profile/activity ["published"]}]}
                                nil home))]
    (is (= #{:nrepl} (:compose/activity s)))
    (is (= #{:nrepl :published} (get-in s [:compose/projects "shop" :project/activity])) "names from JSON become keywords")
    (is (nil? (get-in s [:compose/projects "blog" :project/activity])))
    (is (= #{:published} (get-in s [:compose/profiles "p" :profile/activity]))))
  (is (= #{:nrepl} (:compose/activity (:ok (config/settings {} nil home)))) "the default")
  (is (= :compose/invalid-config
         (:error (config/settings {:compose/projects {:shop {:project/dir "/w" :project/activity [:browser]}}} nil home)))
      "an unknown kind is refused"))

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

(def ^:private preflight-cfg
  {:compose/projects {"gone" {:project/id "gone" :project/dir "/w/gone"}
                      "nofile" {:project/id "nofile" :project/dir "/w/nofile"
                                :project/files ["docker-compose.yml" "/abs/override.yml"]}
                      "nodefault" {:project/id "nodefault" :project/dir "/w/nodefault"}
                      "shop" {:project/id "shop" :project/dir "/w/shop"}}
   :compose/stacks {"s" {:stack/id "s" :stack/consumers ["gone" "shop"]}
                    "ok" {:stack/id "ok" :stack/consumers ["shop"]}}})

(def ^:private fake-fs
  "The fake filesystem: what exists. /w/gone is not there at all, /w/nofile's
   override is missing, /w/nodefault holds no compose file."
  #{"/w/nofile" "/w/nofile/docker-compose.yml" "/w/nodefault" "/w/shop" "/w/shop/compose.yaml"})

(deftest preflight-names-what-is-missing-as-data
  (let [projects (:compose/projects (:ok (config/settings preflight-cfg nil home)))
        pf #(config/project-preflight fake-fs (get projects %))]
    (testing "a missing directory"
      (is (= {:reason :compose/project-missing :project "gone" :missing :dir :paths ["/w/gone"]}
             (pf "gone"))))
    (testing "a missing configured compose file names just that file"
      (is (= {:reason :compose/project-missing :project "nofile" :missing :compose-file
              :paths ["/abs/override.yml"]}
             (pf "nofile"))))
    (testing "no compose file under compose's default names"
      (is (= {:reason :compose/project-missing :project "nodefault" :missing :compose-file
              :paths ["/w/nodefault"] :looked-for config/compose-default-files}
             (pf "nodefault"))))
    (testing "a present project: nil"
      (is (nil? (pf "shop"))))
    (testing "a project that expects no compose file needs only its dir"
      (is (false? (config/compose-file-expected? {:project/compose? false})))
      (is (nil? (config/project-preflight fake-fs {:project/id "x" :project/dir "/w/nodefault"
                                                   :project/compose? false}))))
    (testing "total: no dir at all is a missing dir, never a throw"
      (is (= :dir (:missing (config/project-preflight fake-fs {})))))
    (is (= ["gone" "nodefault" "nofile"] (keys (config/projects-preflight fake-fs projects))))))

(deftest projects-rows-are-marked-and-present-ones-unchanged
  (let [projects (:compose/projects (:ok (config/settings preflight-cfg nil home)))
        shop {:name "shop" :status "running(1)" :managed? true :label-files []
              :configured [{:id "shop"}]}
        nofile {:name "nofile" :status "running(1)" :managed? false :label-files []
                :configured [{:id "nofile" :files ["/w/nofile/docker-compose.yml" "/abs/override.yml"]}]}
        rows (config/mark-projects [shop nofile] projects fake-fs)
        by-name (into {} (map (juxt :name identity)) rows)]
    (is (= shop (by-name "shop")) "a present project's row is unchanged")
    (is (= :compose-file (get-in by-name ["nofile" :configured 0 :problem :missing])))
    (testing "a missing project docker does not list gets a row of its own"
      (is (= "missing" (:status (by-name "gone"))))
      (is (= :compose/project-missing (get-in by-name ["gone" :configured 0 :problem :reason])))
      (is (= :dir (get-in by-name ["gone" :configured 0 :problem :missing]))))
    (is (= [shop] (config/mark-projects [shop] {"shop" (get projects "shop")} fake-fs))
        "nothing missing: the rows as given")))

(deftest plan-carries-the-problems-of-the-projects-its-stack-brings-up
  (let [s (:ok (config/settings preflight-cfg nil home))
        res (config/mark-plan {:ok {:text "plan"}} s "s" fake-fs)]
    (is (= ["gone" "shop"] (config/stack-projects s "s")))
    (is (= [{:reason :compose/project-missing :project "gone" :missing :dir :paths ["/w/gone"]}]
           (get-in res [:ok :problems])))
    (is (re-find #"MISSING: project gone dir /w/gone" (get-in res [:ok :text])))
    (is (= {:ok {:text "plan"}} (config/mark-plan {:ok {:text "plan"}} s "ok" fake-fs))
        "a stack of present projects: the plan unchanged")
    (is (= [:dir] (map :missing (:problems (config/mark-plan {:error :x} s "s" fake-fs))))
        "an error plan carries them too")
    (is (= [] (config/stack-projects s "unknown")))))
