(ns hive-compose.adapter.swarm-test
  "HostSwarm with its host fns handed in: a directed notice goes through the
   directed channel with its target passed through, falls back to the shout
   when that channel does not resolve, and says which channel it used."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.adapter.swarm :as swarm]
            [hive-compose.port :as port]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- recording
  "A host fn recording each call's args into `calls` under `k`, answering `v`."
  [calls k v]
  (fn [& args] (swap! calls update k (fnil conj []) (vec args)) v))

(defn- absent [& _] (r/err :host/absent {:host/sym "hive-mcp.hivemind.messaging/shout-with-verdict!"}))

(deftest a-directed-notice-reaches-its-agent-through-the-directed-channel
  (let [calls (atom {})
        s (swarm/make-swarm {:list-agents (constantly [])
                             :shout! (recording calls :shout true)
                             :send! (recording calls :send {:delivered true :routing :direct
                                                            :shout-id "s1" :to "ling-2" :context-id "ctx-1"})})
        res (port/-notify! s :nrepl/route {:message "use port 7888" :to "ling-2"})]
    (is (= {:channel :direct :to "ling-2" :routing :direct :context-id "ctx-1"} (:ok res)))
    (is (nil? (:shout @calls)) "no broadcast")
    (let [[[from event data]] (:send @calls)]
      (is (= swarm/sender from))
      (is (= :progress event))
      (is (= "ling-2" (:to data)) "the target agent id is passed through to the host")
      (is (= "route" (:event data))))))

(deftest a-directed-notice-falls-back-to-the-shout-without-a-directed-channel
  (let [calls (atom {})
        s (swarm/make-swarm {:list-agents (constantly [])
                             :shout! (recording calls :shout true)
                             :send! absent})
        res (port/-notify! s :nrepl/route {:message "use port 7888" :to "ling-2"})]
    (is (= {:channel :broadcast :fallback? true} (:ok res)))
    (is (= 1 (count (:shout @calls))))
    (is (not (contains? (nth (first (:shout @calls)) 2) :to)) "the fallback shout carries no :to")))

(deftest an-undirected-notice-is-a-broadcast
  (let [calls (atom {})
        s (swarm/make-swarm {:list-agents (constantly [])
                             :shout! (recording calls :shout true)
                             :send! (recording calls :send {:delivered true})})]
    (is (= {:channel :broadcast} (:ok (port/-notify! s :section/up {:message "up"}))))
    (is (nil? (:send @calls)) "section notices never take the directed channel")))

(deftest failures-are-errors-not-throws
  (testing "a directed notice the host drops"
    (let [s (swarm/make-swarm {:list-agents (constantly [])
                               :shout! (constantly true)
                               :send! (constantly {:delivered false :reason :empty-payload})})]
      (is (= :compose/notice-dropped (:error (port/-notify! s :nrepl/route {:message "x" :to "ling-2"}))))))
  (testing "a directed channel that throws"
    (let [s (swarm/make-swarm {:list-agents (constantly [])
                               :shout! (constantly true)
                               :send! (fn [& _] (throw (ex-info "boom" {})))})]
      (is (= :compose/swarm-failed (:error (port/-notify! s :nrepl/route {:message "x" :to "ling-2"}))))))
  (testing "no host at all"
    (let [s (swarm/make-swarm {:list-agents absent :shout! absent :send! absent})]
      (is (= :host/absent (:error (port/-notify! s :nrepl/route {:message "x" :to "ling-2"}))))
      (is (= :host/absent (:error (port/-agents s))))))
  (testing "a broadcast the host drops"
    (let [s (swarm/make-swarm {:list-agents (constantly []) :shout! (constantly false) :send! absent})]
      (is (= :compose/notice-dropped (:error (port/-notify! s :section/up {:message "x"})))))))
