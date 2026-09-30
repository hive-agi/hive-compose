(ns hive-compose.pipeline.ops-test
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.pipeline.ops :as ops]
            [hive-compose.stub :as stub]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def minute 60000)

(def settings
  {:compose/default-ttl-minutes 60
   :compose/default-idle-action :stop
   :compose/profiles {"api" {:profile/id "api" :profile/dir "/w/shop" :profile/services ["api"]}
                      "web" {:profile/id "web" :profile/dir "/w/shop" :profile/services ["web"]
                             :profile/ttl-minutes 10 :profile/idle-action :down}
                      "blog" {:profile/id "blog" :profile/dir "/w/blog"}}})

(def closures {"api" ["api" "postgres"]
               "web" ["postgres" "web"]
               "blog" ["ghost" "mysql"]})

(defn- ctx
  ([] (ctx {}))
  ([running]
   (let [clock (atom 0)
         saved (atom nil)]
     {:engine (stub/engine closures running)
      :settings settings
      :state (atom {:active {} :current nil})
      :now #(deref clock)
      :clock clock
      :saved saved
      :save! #(r/ok (reset! saved %))})))

(defn- advance! [c ms] (swap! (:clock c) + ms))

(deftest up-starts-the-closure-and-tracks-it
  (let [c (ctx)
        res (ops/up! c "api")]
    (is (r/ok? res))
    (is (= #{"api" "postgres"} (stub/running (:engine c) "shop")))
    (is (= "api" (:current @(:state c))))
    (is (= ["api" "postgres"] (get-in @(:saved c) [:active "api" :services])) "persisted")
    (testing "up stacks without changing current"
      (ops/up! c "blog")
      (is (= "api" (:current @(:state c))))
      (is (= #{"api" "blog"} (set (keys (:active @(:state c)))))))))

(deftest switch-keeps-shared-services
  (let [c (ctx)]
    (ops/up! c "api")
    (let [res (ops/switch! c "web")]
      (is (r/ok? res))
      (is (= [[:stop "shop" ["api"]]] (filterv #(= :stop (first %)) (stub/calls (:engine c))))
          "only api is released; postgres is shared")
      (is (= #{"postgres" "web"} (stub/running (:engine c) "shop")))
      (is (= "web" (:current @(:state c))))
      (is (= ["web"] (vec (keys (:active @(:state c)))))))))

(deftest switch-across-projects-releases-everything-previous
  (let [c (ctx)]
    (ops/up! c "api")
    (ops/switch! c "blog")
    (is (= #{} (stub/running (:engine c) "shop")))
    (is (= #{"ghost" "mysql"} (stub/running (:engine c) "blog")))))

(deftest a-failed-release-keeps-the-profile-tracked
  (let [c (ctx)]
    (ops/up! c "api")
    (stub/fail! (:engine c) :stop)
    (let [res (ops/switch! c "blog")]
      (is (r/ok? res) "the next profile still comes up")
      (is (false? (:ok? (first (:released (:ok res))))))
      (is (contains? (:active @(:state c)) "api") "still answered for, so the reaper retries"))))

(deftest down-spares-services-other-active-profiles-need
  (let [c (ctx)]
    (ops/up! c "api")
    (ops/up! c "web")
    (let [res (ops/down! c "api" :down)]
      (is (= ["api"] (:services (:ok res))))
      (is (= #{"postgres" "web"} (stub/running (:engine c) "shop")))
      (is (not (contains? (:active @(:state c)) "api"))))))

(deftest unknown-profile-is-an-err
  (is (= :compose/unknown-profile (:error (ops/up! (ctx) "nope")))))

(deftest reaper-releases-idle-profiles
  (let [c (ctx)]
    (ops/up! c "api")
    (ops/up! c "web")
    (advance! c (* 11 minute))
    (let [{:keys [reaped]} (ops/reap! c)]
      (is (= [{:profile/id "web" :project "shop" :action :down :services ["web"] :ok? true}] reaped))
      (is (= #{"api" "postgres"} (stub/running (:engine c) "shop"))))
    (testing "touch resets the clock"
      (advance! c (* 50 minute))
      (ops/touch! c "api")
      (advance! c (* 30 minute))
      (is (empty? (:reaped (ops/reap! c)))))
    (testing "then api goes too, postgres with it"
      (advance! c (* 31 minute))
      (ops/reap! c)
      (is (= #{} (stub/running (:engine c) "shop")))
      (is (empty? (:active @(:state c)))))))

(deftest reconcile-forgets-stacks-taken-down-by-hand
  (let [c (ctx)]
    (ops/up! c "blog")
    (swap! (:world (:engine c)) assoc-in [:running "blog"] #{})
    (is (= ["blog"] (:reconciled (ops/reap! c))))
    (is (nil? (:current @(:state c))))))

(deftest adopt-takes-charge-of-running-configured-stacks
  (let [c (ctx {"blog" #{"ghost"}})]
    (is (= ["blog"] (ops/adopt! c)))
    (is (= ["ghost" "mysql"] (get-in @(:state c) [:active "blog" :services])))
    (is (= [] (ops/adopt! c)) "idempotent")
    (advance! c (* 61 minute))
    (ops/reap! c)
    (is (= #{} (stub/running (:engine c) "blog")) "adopted stacks are reaped like any other")))

(deftest status-reports-the-countdown
  (let [c (ctx)]
    (ops/up! c "web")
    (advance! c (* 4 minute))
    (let [row (first (filter #(= "web" (:id %)) (:profiles (ops/status c))))]
      (is (:active? row))
      (is (= 240 (:idle-seconds row)))
      (is (= 360 (:reap-in-seconds row))))))

(deftest ps-and-logs-touch-the-profile
  (let [c (ctx)]
    (ops/up! c "api")
    (advance! c (* 5 minute))
    (is (= ["api" "postgres"] (:running (:ok (ops/ps c "api")))))
    (is (= (* 5 minute) (get-in @(:state c) [:active "api" :last-touch])))
    (advance! c minute)
    (is (= {:out "log"} (:ok (ops/logs c "api" 20))))
    (is (= (* 6 minute) (get-in @(:state c) [:active "api" :last-touch])))))
