(ns hive-compose.promote.plan-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.promote.plan :as plan]
            [hive-compose.promote.profile :as profile]))

;; SPDX-License-Identifier: MIT

(def settings {:compose/default-ttl-minutes 60
               :compose/default-idle-action :stop
               :compose/profiles {"api" {:profile/id "api" :profile/dir "/w/shop"}
                                  "web" {:profile/id "web" :profile/dir "/w/shop" :profile/ttl-minutes 10}
                                  "pinned" {:profile/id "pinned" :profile/dir "/w/other" :profile/idle-action :none}}})

(defn- active-entry [id project services touch]
  {:profile/id id :project project :services services :started-at 0 :last-touch touch})

(deftest default-project-follows-compose
  (is (= "shop" (profile/default-project "/w/shop")))
  (is (= "myapp_2" (profile/default-project "/w/My.App_2/")))
  (is (= "custom" (profile/project {:profile/dir "/w/shop" :profile/project "custom"}))))

(deftest release-keeps-what-others-need
  (let [active {"api" (active-entry "api" "shop" ["api" "postgres"] 0)
                "web" (active-entry "web" "shop" ["postgres" "web"] 0)
                "x" (active-entry "x" "elsewhere" ["api"] 0)}]
    (is (= ["api"] (plan/release-services active "api")))
    (is (= ["web"] (plan/release-services active "web")))
    (is (= ["api"] (plan/release-services active "x")) "other projects never share")))

(deftest switch-releases-only-what-next-does-not-share
  (let [active {"api" (active-entry "api" "shop" ["api" "postgres" "redis"] 0)}]
    (testing "same project: shared services stay"
      (is (= [{:profile/id "api" :project "shop" :action :stop :services ["api" "redis"]}]
             (plan/switch-releases active "api" "web" "shop" ["postgres" "web"]))))
    (testing "other project: everything of the previous goes"
      (is (= ["api" "postgres" "redis"]
             (:services (first (plan/switch-releases active "api" "web" "other" ["postgres"]))))))
    (testing "nothing to release"
      (is (= [] (plan/switch-releases active nil "web" "shop" [])))
      (is (= [] (plan/switch-releases active "api" "api" "shop" [])))
      (is (= [] (plan/switch-releases {} "gone" "web" "shop" []))))))

(deftest down-release-of-inactive-profile-spares-active-ones
  (let [active {"api" (active-entry "api" "shop" ["api" "postgres"] 0)}]
    (is (= ["web"] (:services (plan/down-release active "web" "shop" ["postgres" "web"] :down))))
    (is (= ["api" "postgres"] (:services (plan/down-release active "api" "shop" [] :stop))))))

(deftest reap-releases-idle-profiles-and-the-services-they-alone-held
  (let [min 60000
        active {"api" (active-entry "api" "shop" ["api" "postgres"] 0)
                "web" (active-entry "web" "shop" ["postgres" "web"] 0)
                "pinned" (active-entry "pinned" "other" ["db"] 0)}]
    (testing "only web is past its 10 minute ttl"
      (is (= [{:profile/id "web" :project "shop" :action :stop :services ["web"]}]
             (plan/reap-releases settings (* 11 min) active))))
    (testing "both idle: the shared postgres goes with the last of them"
      (is (= [{:profile/id "api" :project "shop" :action :stop :services ["api"]}
              {:profile/id "web" :project "shop" :action :stop :services ["postgres" "web"]}]
             (plan/reap-releases settings (* 61 min) active))))
    (testing ":none is never reaped"
      (is (not-any? #(= "pinned" (:profile/id %)) (plan/reap-releases settings (* 10000 min) active))))
    (testing "a touched profile is not idle"
      (is (= [] (plan/reap-releases settings (* 11 min) (assoc-in active ["web" :last-touch] (* 5 min))))))))

(def gen-active
  (gen/fmap (fn [entries]
              (into {} (map-indexed (fn [i [project services]]
                                      (let [id (str "p" i)]
                                        [id (active-entry id project (vec (distinct services)) 0)])))
                    entries))
            (gen/vector (gen/tuple (gen/elements ["a" "b"])
                                   (gen/vector (gen/elements ["db" "api" "web" "cache"]) 1 4))
                        0 5)))

(defspec reaping-everything-releases-every-service-exactly-once 200
  (prop/for-all [active gen-active]
    (let [steps (plan/reap-releases settings Long/MAX_VALUE active)
          released (frequencies (mapcat (fn [s] (map #(vector (:project s) %) (:services s))) steps))
          held (set (mapcat (fn [[_ a]] (map #(vector (:project a) %) (:services a))) active))]
      (and (= (count steps) (count active))
           (= held (set (keys released)))
           (every? #(= 1 %) (vals released))))))

(defspec release-never-takes-what-another-active-profile-needs 200
  (prop/for-all [active gen-active]
    (every? (fn [[id a]]
              (let [others (plan/needed-by-others active (:project a) id)]
                (not-any? others (plan/release-services active id))))
            active)))
