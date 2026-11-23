(ns hive-compose.pipeline.programs-only-test
  "A project with :project/compose? false runs only host programs: no docker
   call on up, down, ps, status or the reconcile pass (card 20261123200751-0f950466)."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.pipeline.ops :as ops]
            [hive-compose.port :as port]
            [hive-compose.promote.config :as config]
            [hive-compose.promote.program :as program]
            [hive-compose.stub :as stub]
            [hive-compose.stub-runner :as runner]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- counting-engine
  "An IComposeEngine that counts every call into `calls` and refuses it: any
   docker call a programs-only section makes shows up here."
  [calls]
  (let [hit (fn [m] (swap! calls conj m) (r/err :test/docker-called {:method m}))]
    (reify port/IComposeEngine
      (-version [_] (hit :version))
      (-closure [_ _] (hit :closure))
      (-listed [_ _] (hit :listed))
      (-profiles [_ _] (hit :profiles))
      (-running [_ _] (hit :running))
      (-projects [_] (hit :projects))
      (-up! [_ _ _] (hit :up))
      (-stop! [_ _ _] (hit :stop))
      (-down! [_ _ _] (hit :down))
      (-restart! [_ _ _ _] (hit :restart))
      (-logs [_ _ _ _] (hit :logs)))))

(def fleet
  {:project/id "fleet" :project/dir "/w/fleet" :project/compose? false
   :project/programs [{:program/id "inventory" :program/dir "/w/fleet"
                       :program/command ["bb" "inventory"]}]})

(def base
  {:compose/default-ttl-minutes 60
   :compose/default-idle-action :stop
   :compose/log-dir "/logs"
   :compose/programs? true})

(defn- ctx [engine projects profiles]
  {:engine engine
   :runner (runner/runner {})
   :settings (assoc base :compose/projects projects :compose/profiles profiles)
   :state (atom {:active {} :current nil})
   :now (constantly 0)
   :save! (fn [_] (r/ok nil))})

(deftest config-accepts-programs-only-and-refuses-an-empty-project
  (testing "programs and no compose project: valid"
    (is (r/ok? (config/settings {:compose/projects [fleet]} nil "/h"))))
  (testing "no compose project and no program: refused with a keyword reason"
    (let [res (config/settings {:compose/projects [(dissoc fleet :project/programs)]} nil "/h")]
      (is (= :compose/invalid-config (:error res)))))
  (testing "a program paired with a service of a project that has none: refused"
    (let [res (config/settings {:compose/projects
                                [(assoc-in fleet [:project/programs 0 :program/with] ["web"])]}
                               nil "/h")]
      (is (= :compose/invalid-config (:error res)))))
  (testing "a compose project without :project/compose? is unchanged"
    (is (r/ok? (config/settings {:compose/projects [(dissoc fleet :project/compose? :project/programs)]}
                                nil "/h")))))

(deftest up-mode-is-total
  (is (= (r/ok :compose) (program/up-mode {:profile/id "a" :profile/dir "/a"})))
  (is (= (r/ok :programs-only) (program/up-mode {:profile/id "f" :profile/dir "/f" :profile/compose? false
                                                 :profile/programs [{:program/id "x" :program/dir "/f"}]})))
  (is (= :compose/nothing-to-run
         (:error (program/up-mode {:profile/id "f" :profile/dir "/f" :profile/compose? false})))))

(deftest a-programs-only-section-lives-with-no-docker-call
  (let [calls (atom [])
        c (ctx (counting-engine calls) {"fleet" fleet} {})
        p (:ok (ops/subject c {:target "fleet"}))]
    (is (= "fleet/fleet" (:profile/id p)))
    (testing "up starts the program, docker is never called"
      (let [res (ops/up-profile! c p)]
        (is (r/ok? res))
        (is (= [] (:services (:ok res))))
        (is (= 1 (count (runner/starts (:runner c)))))
        (is (= [[:start "inventory" ["bb" "inventory"] "/logs/fleet-inventory.log"]]
               (runner/starts (:runner c))))
        (is (contains? (get-in @(:state c) [:active "fleet/fleet" :programs]) "inventory"))))
    (testing "status and ps read it"
      (let [row (some #(when (= "fleet/fleet" (:id %)) %) (:profiles (ops/status c)))]
        (is (:active? row))
        (is (= ["inventory"] (mapv :program/id (:programs row)))))
      (is (= {:needs [] :running []} (select-keys (:ok (ops/ps c "fleet/fleet")) [:needs :running]))))
    (testing "the reconcile pass keeps it: it never had a service to lose"
      (is (= [] (ops/reconcile! c)))
      (is (empty? (runner/stops (:runner c)))))
    (testing "down stops the program"
      (let [res (ops/down! c "fleet/fleet" :down)]
        (is (r/ok? res))
        (is (= 1 (count (runner/stops (:runner c)))))
        (is (empty? (runner/alive (:runner c))))
        (is (empty? (:active @(:state c))))))
    (is (= [] @calls) "no docker call at all")))

(deftest a-project-with-services-and-programs-is-unchanged
  (let [web {:program/id "web" :program/dir "/w/shop" :program/command ["npx" "watch"] :program/with ["web"]}
        c (ctx (stub/engine {"web" ["postgres" "web"]}) {}
               {"web" {:profile/id "web" :profile/dir "/w/shop" :profile/services ["web"]
                       :profile/programs [web]}})
        res (ops/up! c "web")]
    (is (r/ok? res))
    (is (= 1 (count (stub/ups (:engine c)))) "compose up ran")
    (is (= 1 (count (runner/starts (:runner c)))))
    (is (r/ok? (ops/down! c "web" :down)))
    (is (= 1 (count (runner/stops (:runner c)))))))
