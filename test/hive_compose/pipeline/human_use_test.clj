(ns hive-compose.pipeline.human-use-test
  "Human use of a section: new connections to the host ports its containers
   publish, opt-in per project with :project/activity, those its own
   containers hold left out."
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [hive-compose.adapter.procnet :as procnet]
            [hive-compose.pipeline.ops :as ops]
            [hive-compose.pipeline.swarm :as swarm]
            [hive-compose.port :as port]
            [hive-compose.stub :as stub]
            [hive-compose.stub-runner :as stub-runner]
            [hive-compose.stub-swarm :as stub-swarm]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def minute 60000)

(def web-port 18080)

(def settings
  {:compose/default-ttl-minutes 60
   :compose/default-idle-action :stop
   :compose/claim-ttl-minutes 30
   :compose/orphan-ttl-minutes 10
   :compose/swarm? true
   :compose/notify? true
   :compose/activity #{:nrepl}
   :compose/profiles {}
   :compose/projects {"shop" {:project/id "shop" :project/dir "/w/shop"
                              :project/activity #{:nrepl :published}}
                      "blog" {:project/id "blog" :project/dir "/w/blog"}}})

(def model
  {"shop" {"web" {:deps ["db"]} "db" {}}
   "blog" {"ghost" {}}})

(defn- ctx
  ([] (ctx (stub-swarm/activity)))
  ([activity]
   (let [clock (atom 0)]
     {:engine (stub/engine {} {} model)
      :runner (stub-runner/runner)
      :swarm (stub-swarm/swarm [])
      :activity activity
      :settings settings
      :state (atom {:active {} :current nil})
      :now #(deref clock)
      :clock clock
      :save! (fn [_] (r/ok nil))})))

(defn- advance! [c ms] (swap! (:clock c) + ms))
(defn- section [c target] (:ok (ops/subject c {:target target})))

(defn- pass! [c]
  (let [observed (swarm/observe! c)]
    (assoc (ops/reap! c) :observed observed)))

(defn- reaped [pass] (mapv :profile/id (:reaped pass)))

(deftest a-browser-on-a-published-port-keeps-an-opted-in-section
  (let [c (ctx)]
    (ops/up-profile! c (section c "shop/web"))
    (stub/published! (:engine c) "shop" {"web" #{web-port}})
    (stub/addresses! (:engine c) "shop" #{"172.18.0.2" "172.18.0.3"})
    (advance! c (* 61 minute))
    (stub-swarm/peers! (:activity c) {web-port #{"127.0.0.1:50001" "127.0.0.1:50002"}})
    (let [pass (pass! c)]
      (is (empty? (reaped pass)) "a browser's new connections are use")
      (is (= ["shop/web"] (get-in pass [:observed :in-use]))))
    (let [[s] (:sections (:ok (swarm/who c)))]
      (is (= [{:port web-port :peers 2 :source :published}] (:used-by s)))
      (is (= [:published] (:in-use-via s)) "who names the source that counted")
      (is (= [:nrepl :published] (:activity s))))
    (let [row (first (filter #(= "shop/web" (:id %)) (:profiles (ops/status c))))]
      (is (= [:published] (:in-use-via row)) "status names it too")
      (is (= [:nrepl :published] (:activity row))))
    (testing "the same keep-alive connections an hour later are not new use"
      (advance! c (* 61 minute))
      (is (= ["shop/web"] (reaped (pass! c)))))))

(deftest connections-from-the-projects-own-containers-are-not-use
  (let [c (ctx)]
    (ops/up-profile! c (section c "shop/web"))
    (stub/published! (:engine c) "shop" {"web" #{web-port}})
    (stub/addresses! (:engine c) "shop" #{"172.18.0.3"})
    (advance! c (* 61 minute))
    (stub-swarm/peers! (:activity c) {web-port #{"172.18.0.3:40000" "172.18.0.3:40001"}})
    (let [pass (pass! c)]
      (is (= ["shop/web"] (reaped pass)) "a pool from its own containers keeps nothing alive")
      (is (empty? (get-in pass [:observed :in-use]))))))

(deftest a-host-side-client-counts-beside-a-container-pool
  (let [c (ctx)]
    (ops/up-profile! c (section c "shop/web"))
    (stub/published! (:engine c) "shop" {"web" #{web-port}})
    (stub/addresses! (:engine c) "shop" #{"172.18.0.3"})
    (advance! c (* 61 minute))
    (stub-swarm/peers! (:activity c) {web-port #{"172.18.0.3:40000" "[::1]:50003"}})
    (is (empty? (reaped (pass! c))))
    (is (= [{:port web-port :peers 1 :source :published}]
           (get-in @(:state c) [:active "shop/web" :usage :by]))
        "only the host-side peer is counted")))

(deftest published-ports-do-not-count-for-a-project-that-did-not-opt-in
  (let [c (ctx)]
    (ops/up-profile! c (section c "blog/ghost"))
    (stub/published! (:engine c) "blog" {"ghost" #{2368}})
    (advance! c (* 61 minute))
    (stub-swarm/peers! (:activity c) {2368 #{"127.0.0.1:50004"}})
    (is (= ["blog/ghost"] (reaped (pass! c))) "the global default #{:nrepl} holds for blog")
    (testing "the global setting opts every project in"
      (let [c (assoc (ctx) :settings (assoc settings :compose/activity #{:published}))]
        (ops/up-profile! c (section c "blog/ghost"))
        (stub/published! (:engine c) "blog" {"ghost" #{2368}})
        (advance! c (* 61 minute))
        (stub-swarm/peers! (:activity c) {2368 #{"127.0.0.1:50004"}})
        (is (empty? (reaped (pass! c))))))))

(deftest an-engine-that-cannot-name-its-containers-still-counts-host-peers
  (let [c (ctx)]
    (ops/up-profile! c (section c "shop/web"))
    (stub/published! (:engine c) "shop" {"web" #{web-port}})
    (stub/fail! (:engine c) :addresses)
    (advance! c (* 61 minute))
    (stub-swarm/peers! (:activity c) {web-port #{"127.0.0.1:50005"}})
    (is (empty? (reaped (pass! c))))))

(deftest a-real-client-on-a-published-port-is-seen-and-counts
  ;; Real boundary: a localhost listener stands for a published port, a real
  ;; client connects, and the /proc/net/tcp probe the addon wires sees it.
  (when (.exists (io/file "/proc/net/tcp"))
    (with-open [ss (java.net.ServerSocket. 0 50 (java.net.InetAddress/getByName "127.0.0.1"))]
      (let [p (.getLocalPort ss)
            probe (procnet/make-activity)
            c (ctx probe)]
        (ops/up-profile! c (section c "shop/web"))
        (stub/published! (:engine c) "shop" {"web" #{p}})
        (swarm/observe! c)
        (is (empty? (get (:peers @(:state c)) p)) "nobody connected before the client")
        (advance! c (* 61 minute))
        (let [accepted (future (.accept ss))]
          (with-open [client (java.net.Socket. "127.0.0.1" (int p))
                      _server ^java.net.Socket (deref accepted 5000 nil)]
            (let [peer (str "127.0.0.1:" (.getLocalPort client))]
              (is (contains? (get (port/-peers probe #{p}) p #{}) peer)
                  "the probe names the client's own address and port")
              (let [pass (pass! c)]
                (is (empty? (reaped pass)) "the new connection is use")
                (is (= [:published] (get-in @(:state c) [:active "shop/web" :usage :sources])))))))))))
