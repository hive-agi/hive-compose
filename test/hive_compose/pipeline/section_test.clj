(ns hive-compose.pipeline.section-test
  "Sections named by target against a compose project shaped like a real one:
   always-on infrastructure behind a gateway, apps gated by native profiles."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.pipeline.ops :as ops]
            [hive-compose.stub :as stub]
            [hive-dsl.result :as r]
            [hive-compose.promote.profile :as profile]))

;; SPDX-License-Identifier: MIT

(def minute 60000)

(def shop
  {"postgres" {}
   "redis" {}
   "keycloak" {}
   "ledger" {}
   "auth" {:deps ["postgres" "redis"]}
   "envoy" {:deps ["auth" "keycloak"]}
   "web" {:deps ["envoy"] :profiles #{"frontend"}}
   "crm" {:deps ["envoy" "postgres"] :profiles #{"crm"}}
   "minio" {:profiles #{"site"}}
   "site" {:deps ["minio"] :profiles #{"site"}}})

(def settings
  {:compose/default-ttl-minutes 60
   :compose/default-idle-action :stop
   :compose/profiles {}
   :compose/projects {"shop" {:project/id "shop" :project/dir "/w/shop"}
                      "blog" {:project/id "blog" :project/dir "/w/blog" :project/ttl-minutes 5}}})

(defn- ctx
  ([] (ctx {}))
  ([running]
   (let [clock (atom 0)]
     {:engine (stub/engine {} running {"shop" shop "blog" {"ghost" {:deps ["mysql"]} "mysql" {}}})
      :settings settings
      :state (atom {:active {} :current nil})
      :now #(deref clock)
      :clock clock})))

(defn- up-target! [c t] (r/bind (ops/subject c {:target t}) #(ops/up-profile! c %)))
(defn- down-target! [c t] (r/bind (ops/subject c {:target t}) #(ops/down-profile! c % :down)))

(deftest a-service-target-runs-exactly-its-closure
  (let [c (ctx)
        res (up-target! c "web")]
    (is (= "shop/web" (:profile (:ok res))))
    (is (= ["auth" "envoy" "keycloak" "postgres" "redis" "web"] (:services (:ok res))))
    (is (not (contains? (stub/running (:engine c) "shop") "ledger"))
        "always-on services outside the closure stay down")))

(deftest a-native-profile-target-means-its-own-members
  (let [res (up-target! (ctx) "frontend")]
    (is (= "shop/frontend" (:profile (:ok res))))
    (is (= ["auth" "envoy" "keycloak" "postgres" "redis" "web"] (:services (:ok res)))
        "not the always-on base a bare --profile would add")))

(deftest several-targets-form-one-section
  (let [res (up-target! (ctx) "shop/web,site")]
    (is (= "shop/site+web" (:profile (:ok res))))
    (is (some #{"minio"} (:services (:ok res))))))

(deftest down-releases-only-that-section
  (let [c (ctx)]
    (up-target! c "web")
    (up-target! c "crm")
    (let [res (down-target! c "web")]
      (is (= ["web"] (:services (:ok res))) "envoy and its deps are still needed by crm")
      (is (= #{"auth" "crm" "envoy" "keycloak" "postgres" "redis"} (stub/running (:engine c) "shop")))
      (is (= ["shop/crm"] (keys (:active @(:state c))))))
    (testing "the last section takes the shared infrastructure with it"
      (down-target! c "shop/crm")
      (is (= #{} (stub/running (:engine c) "shop"))))))

(deftest switch-between-sections-keeps-shared-infrastructure
  (let [c (ctx)]
    (up-target! c "web")
    (let [res (r/bind (ops/subject c {:target "crm"}) #(ops/switch-profile! c %))]
      (is (= ["web"] (-> res :ok :released first :services)))
      (is (= "shop/crm" (:current @(:state c)))))))

(deftest targets-resolve-across-projects
  (let [c (ctx)]
    (is (= "blog/ghost" (:profile (:ok (up-target! c "ghost")))))
    (is (= 5 (get-in @(:state c) [:active "blog/ghost" :profile :profile/ttl-minutes]))
        "sections inherit their project's ttl")
    (is (= :compose/unknown-target (:error (ops/subject c {:target "nope"}))))
    (is (= :compose/unknown-project (:error (ops/subject c {:target "zzz/web"}))))
    (is (= :compose/unknown-target (:error (ops/subject c {:target "blog/web"}))))
    (is (= :compose/missing-param (:error (ops/subject c {}))))))

(deftest idle-sections-are-reaped
  (let [c (ctx)]
    (up-target! c "web")
    (up-target! c "ghost")
    (swap! (:clock c) + (* 6 minute))
    (is (= ["blog/ghost"] (mapv :profile/id (:reaped (ops/reap! c)))) "blog's 5 minute ttl")
    (swap! (:clock c) + (* 60 minute))
    (ops/reap! c)
    (is (= #{} (stub/running (:engine c) "shop")))))

(deftest stray-containers-of-a-project-are-adopted-and-reaped
  (let [c (ctx {"shop" #{"ledger" "postgres"}})]
    (up-target! c "site")
    (is (= ["shop/adopted"] (ops/adopt! c)))
    (is (= ["ledger" "postgres"] (get-in @(:state c) [:active "shop/adopted" :services])))
    (is (= [] (ops/adopt! c)) "nothing left unowned")
    (swap! (:clock c) + (* 61 minute))
    (ops/reap! c)
    (is (= #{} (stub/running (:engine c) "shop")))))

(deftest reaping-a-section-spares-what-an-adopted-one-depends-on
  (let [c (ctx {"shop" #{"auth" "redis"}})]
    (up-target! c "postgres")
    (is (= ["shop/adopted"] (ops/adopt! c)))
    (is (= ["auth" "postgres" "redis"] (get-in @(:state c) [:active "shop/adopted" :needs]))
        "auth depends on postgres, which shop/postgres started")
    (swap! (:clock c) + (* 30 minute))
    (ops/touch! c "shop/adopted")
    (swap! (:clock c) + (* 31 minute))
    (is (= ["shop/postgres"] (mapv :profile/id (:reaped (ops/reap! c)))))
    (is (contains? (stub/running (:engine c) "shop") "postgres")
        "the idle postgres section leaves postgres to the section still using it")
    (swap! (:clock c) + (* 61 minute))
    (ops/reap! c)
    (is (= #{} (stub/running (:engine c) "shop")) "the last user takes it down")))

(deftest no-deps-starts-only-the-named-services-but-still-needs-the-rest
  (let [c (ctx)
        res (r/bind (ops/subject c {:target "web"})
                    #(ops/up-profile! c (profile/with-call-options % {:no-deps? true})))]
    (is (= ["web"] (:services (:ok res))))
    (is (= [:up "shop" ["web"]] (last (stub/calls (:engine c)))))
    (is (= ["auth" "envoy" "keycloak" "postgres" "redis" "web"]
           (get-in @(:state c) [:active "shop/web" :needs])))))

(deftest projects-sharing-a-compose-project-adopt-each-stray-once
  (let [c (assoc (ctx {"shop" #{"ledger"}})
                 :settings (assoc-in settings [:compose/projects "shop-live"]
                                     {:project/id "shop-live" :project/dir "/w/shop"}))]
    (is (= ["shop/adopted"] (ops/adopt! c)))
    (is (= ["shop/adopted"] (keys (:active @(:state c)))))))

(deftest touch-without-a-target-touches-every-active-section
  (let [c (ctx)]
    (up-target! c "web")
    (up-target! c "ghost")
    (swap! (:clock c) + (* 10 minute))
    (is (= ["blog/ghost"] (:touched (:ok (ops/touch-all! c "blog")))))
    (is (= ["blog/ghost" "shop/web"] (:touched (:ok (ops/touch-all! c nil)))))
    (is (every? #(= (* 10 minute) (:last-touch %)) (vals (:active @(:state c)))))))

(deftest targets-lists-what-each-project-offers
  (let [rows (:ok (ops/targets (ctx)))]
    (is (= ["blog" "shop"] (mapv :project rows)))
    (is (= ["crm" "frontend" "site"] (:profiles (second rows))))
    (is (some #{"ledger"} (:always-on (second rows))))))
