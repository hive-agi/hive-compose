(ns hive-compose.pipeline.restart-test
  "Friction 20261026111354-45e6b8d6: compose had no restart verb, and a
   down/stop of a service another active section needs answered ok with
   services [] and said nothing."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.adapter.cli :as cli]
            [hive-compose.pipeline.ops :as ops]
            [hive-compose.pipeline.restart :as restart]
            [hive-compose.port :as port]
            [hive-compose.promote.plan :as plan]
            [hive-compose.stub :as stub]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def settings
  {:compose/default-ttl-minutes 60
   :compose/default-idle-action :stop
   :compose/profiles {"api" {:profile/id "api" :profile/dir "/w/shop" :profile/services ["api"]}
                      "web" {:profile/id "web" :profile/dir "/w/shop" :profile/services ["web"]}
                      "db" {:profile/id "db" :profile/dir "/w/shop" :profile/services ["postgres"]}}})

(def closures {"api" ["api" "postgres"]
               "web" ["postgres" "web"]
               "db" ["postgres"]})

(defn- ctx []
  {:engine (stub/engine closures {})
   :settings settings
   :state (atom {:active {} :current nil})
   :now (constantly 0)
   :save! (constantly (r/ok nil))})

(deftest restart-recycles-a-service-other-sections-need
  (let [c (ctx)]
    (ops/up! c "api")
    (ops/up! c "web")
    (let [res (r/bind (ops/lookup c "db") #(restart/restart-profile! c % false))]
      (is (r/ok? res))
      (is (= ["postgres"] (:services (:ok res))))
      (is (= :restart (:action (:ok res))))
      (is (= [:restart "shop" ["postgres"]] (last (stub/calls (:engine c)))))
      (is (= #{"api" "postgres" "web"} (stub/running (:engine c) "shop")) "nothing taken away")
      (is (= #{"api" "web"} (set (keys (:active @(:state c))))) "the active map is unchanged"))
    (testing "recreate=true recreates"
      (let [res (r/bind (ops/lookup c "api") #(restart/restart-profile! c % true))]
        (is (= :recreate (:action (:ok res))))
        (is (= [:recreate "shop" ["api"]] (last (stub/calls (:engine c)))))))))

(deftest restart-failure-is-an-err
  (let [c (ctx)]
    (stub/fail! (:engine c) :restart)
    (is (= :stub/fault (:error (r/bind (ops/lookup c "api") #(restart/restart-profile! c % false)))))))

(deftest down-that-releases-nothing-says-why
  (let [c (ctx)]
    (ops/up! c "api")
    (ops/up! c "web")
    (let [res (r/bind (ops/lookup c "db") #(ops/down-profile! c % :stop))
          out (:ok res)]
      (is (r/ok? res))
      (is (= [] (:services out)))
      (is (= {"postgres" ["api" "web"]} (:kept out)))
      (is (re-find #"nothing was stopped" (:note out)))
      (is (re-find #"restart" (:note out)))
      (is (= #{"api" "postgres" "web"} (stub/running (:engine c) "shop"))))
    (testing "a partial release names what it kept"
      (let [out (:ok (ops/down! c "api" :down))]
        (is (= ["api"] (:services out)))
        (is (= {"postgres" ["web"]} (:kept out)))
        (is (nil? (:note out)))))))

(deftest kept-by-is-pure
  (let [active {"api" {:project "shop" :services ["api" "postgres"]}
                "web" {:project "shop" :services ["web"] :needs ["postgres" "web"]}
                "blog" {:project "blog" :services ["postgres"]}}]
    (is (= {"postgres" ["web"]} (plan/kept-by active "api" "shop" ["api" "postgres"] ["api"])))
    (is (= {} (plan/kept-by active "api" "shop" ["api" "postgres"] ["api" "postgres"])))))

(deftest restart-argv
  (let [p {:profile/id "x" :profile/dir "/w/shop"}]
    (is (= ["restart" "api"] (take-last 2 (cli/restart-argv p ["api"] false))))
    (is (= ["up" "-d" "--force-recreate" "--no-deps" "api"] (take-last 5 (cli/restart-argv p ["api"] true))))
    (is (not-any? #{"-v" "--volumes"} (cli/restart-argv p ["api"] true)))
    (let [calls (atom [])
          e (cli/make-engine {:exec (fn [argv _] (swap! calls conj argv) (r/ok {:exit 0 :stdout ""}))})]
      (is (= :no-services (:skipped (:ok (port/-restart! e p [] false)))))
      (is (empty? @calls) "a bare `compose restart` would restart the whole project"))))
