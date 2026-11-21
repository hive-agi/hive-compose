(ns hive-compose.pipeline.swarm-test
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.pipeline.ops :as ops]
            [hive-compose.pipeline.swarm :as swarm]
            [hive-compose.stub :as stub]
            [hive-compose.stub-runner :as stub-runner]
            [hive-compose.stub-swarm :as stub-swarm]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def minute 60000)

;; A fake nREPL port number: the stub runner answers for it, nothing listens.
(def repl-port 17888)

(def settings
  {:compose/default-ttl-minutes 60
   :compose/default-idle-action :stop
   :compose/claim-ttl-minutes 30
   :compose/orphan-ttl-minutes 10
   :compose/swarm? true
   :compose/notify? true
   :compose/activity #{:nrepl}
   :compose/profiles {}
   :compose/projects {"shop" {:project/id "shop"
                              :project/dir "/w/shop"
                              :project/programs [{:program/id "api-repl"
                                                  :program/dir "/w/shop/api"
                                                  :program/command ["clojure" "-M:nrepl"]
                                                  :program/nrepl-port repl-port
                                                  :program/with ["api"]}]}
                      "blog" {:project/id "blog" :project/dir "/w/blog"}}})

(def model
  {"shop" {"api" {:deps ["postgres"]} "postgres" {}}
   "blog" {"ghost" {}}})

(defn- ctx
  ([] (ctx []))
  ([agents]
   (let [clock (atom 0)]
     {:engine (stub/engine {} {} model)
      :runner (stub-runner/runner)
      :swarm (stub-swarm/swarm agents)
      :activity (stub-swarm/activity)
      :settings settings
      :state (atom {:active {} :current nil})
      :now #(deref clock)
      :clock clock
      :save! (fn [_] (r/ok nil))})))

(defn- advance! [c ms] (swap! (:clock c) + ms))

(defn- section [c target] (:ok (ops/subject c {:target target})))

(defn- pass!
  "One reaper tick as the addon runs it: observe, then reap."
  [c]
  (let [observed (swarm/observe! c)
        reaped (ops/reap! c)]
    (swarm/announce-reaped! c (:reaped reaped))
    (assoc reaped :observed observed)))

(defn- active? [c sid] (contains? (:active @(:state c)) sid))

(deftest claim-brings-the-section-up-and-names-the-shared-nrepl
  (let [c (ctx)
        _ (stub-runner/listen! (:runner c) repl-port)
        res (swarm/claim! c (section c "shop/api") "ling-1" {:note "fix login"})]
    (is (r/ok? res))
    (is (= #{"api" "postgres"} (stub/running (:engine c) "shop")) "claim started what it needs")
    (is (= ["ling-1"] (:claims (:ok res))))
    (is (some? (:started (:ok res))))
    (is (not (contains? (:started (:ok res)) :out)) "docker's own output stays out of the claim answer")
    (let [[route] (:nrepl (:ok res))]
      (is (= repl-port (:nrepl-port route)))
      (is (= "/w/shop/api" (:dir route)))
      (is (:nrepl-ready? route))
      (is (re-find #"do not start another JVM" (:advice route)))) ;; pattern-exempt: test assertion on a literal
    (is (= "fix login" (get-in @(:state c) [:active "shop/api" :claims "ling-1" :note])))
    (testing "a claim without an agent is refused"
      (is (r/err? (swarm/claim! c (section c "shop/api") "" {}))))))

(deftest a-renewed-claim-keeps-the-section-past-its-ttl
  (let [c (ctx)
        p (section c "shop/api")]
    (swarm/claim! c p "ling-1" {})
    (dotimes [_ 4]
      (advance! c (* 20 minute))
      (swarm/claim! c p "ling-1" {})
      (pass! c))
    (is (active? c "shop/api") "80 minutes on, renewed every 20: never reaped")))

(deftest a-stale-claim-orphans-the-section-and-the-orphan-ttl-reaps-it
  (let [c (ctx)
        p (section c "shop/api")]
    (swarm/claim! c p "ling-1" {})
    (advance! c (* 20 minute))
    (pass! c)
    (is (active? c "shop/api") "the claim still holds at 20 minutes")
    (advance! c (* 15 minute))
    (let [pass (pass! c)]
      (is (= [{:section "shop/api" :agent "ling-1" :reason :stale}] (get-in pass [:observed :expired])))
      (is (= ["shop/api"] (get-in pass [:observed :orphaned])))
      (is (= ["shop/api"] (mapv :profile/id (:reaped pass)))
          "idle 15 minutes since the last use, past the 10 minute orphan TTL, well short of the 60 minute one")
      (is (not (active? c "shop/api")))
      (is (= #{} (stub/running (:engine c) "shop")))
      (is (some #(= :section/reaped (first %)) (stub-swarm/notices (:swarm c))) "the swarm is told"))))

(deftest a-claim-ends-with-its-agent
  (let [c (ctx [{:agent/id "ling-1" :agent/cwd "/elsewhere"}])
        p (section c "shop/api")]
    (swarm/claim! c p "ling-1" {})
    (is (true? (get-in @(:state c) [:active "shop/api" :claims "ling-1" :swarm?])))
    (stub-swarm/agents! (:swarm c) [])
    (let [pass (pass! c)]
      (is (= [{:section "shop/api" :agent "ling-1" :reason :gone}] (get-in pass [:observed :expired])))
      (is (some? (get-in @(:state c) [:active "shop/api" :orphaned-at]))))))

(deftest an-agent-working-in-the-project-keeps-the-section-and-is-told-once
  (let [c (ctx [{:agent/id "ling-2" :agent/cwd "/w/shop/api/src"}])
        _ (stub-runner/listen! (:runner c) repl-port)
        _ (ops/up-profile! c (section c "shop/api"))]
    (advance! c (* 61 minute))
    (let [pass (pass! c)]
      (is (empty? (:reaped pass)) "past its TTL, but an agent works in it")
      (is (= [{:agent "ling-2" :section "shop/api" :channel :direct}] (get-in pass [:observed :notified])))
      (is (= [[:nrepl/route "ling-2"]] (stub-swarm/targets (:swarm c))) "addressed to the agent, not broadcast")
      (is (= [{:agent "ling-2"}] (get-in @(:state c) [:active "shop/api" :usage :by]))))
    (let [[event data] (last (stub-swarm/notices (:swarm c)))]
      (is (= :nrepl/route event))
      (is (= [{:dir "/w/shop/api" :program/id "api-repl" :nrepl-port repl-port}] (:nrepl data))))
    (is (empty? (get-in (pass! c) [:observed :notified])) "told once")
    (testing "once the agent leaves, the section idles out"
      (stub-swarm/agents! (:swarm c) [])
      (advance! c (* 61 minute))
      (is (= ["shop/api"] (mapv :profile/id (:reaped (pass! c))))))))

(deftest a-claiming-agent-is-told-directly-wherever-it-works
  (let [c (ctx [{:agent/id "ling-1" :agent/cwd "/elsewhere"}
                {:agent/id "ling-3" :agent/cwd "/w/blog"}])
        _ (stub-runner/listen! (:runner c) repl-port)]
    (swarm/claim! c (section c "shop/api") "ling-1" {})
    (let [pass (pass! c)]
      (is (= [{:agent "ling-1" :section "shop/api" :channel :direct}] (get-in pass [:observed :notified]))
          "the claimant is told though its cwd lies outside the project; the blog agent is not")
      (is (= [[:nrepl/route "ling-1"]] (stub-swarm/targets (:swarm c)))))
    (is (empty? (get-in (pass! c) [:observed :notified])) "told once")
    (testing "a claimant the swarm does not list is still told while its claim holds"
      (let [c (ctx [])
            _ (stub-runner/listen! (:runner c) repl-port)]
        (swarm/claim! c (section c "shop/api") "ling-9" {})
        (is (= [{:agent "ling-9" :section "shop/api" :channel :direct}]
               (get-in (pass! c) [:observed :notified])))
        (is (empty? (get-in (pass! c) [:observed :notified])) "and only once")))))

(deftest section-notices-stay-broadcasts
  (let [c (ctx [{:agent/id "ling-2" :agent/cwd "/w/shop/api"}])]
    (stub-runner/listen! (:runner c) repl-port)
    (ops/up-profile! c (section c "shop/api"))
    (is (= {:channel :broadcast} (:ok (swarm/announce-up! c "shop/api"))))
    (is (nil? (:to (second (last (stub-swarm/notices (:swarm c)))))))
    (is (empty? (stub-swarm/targets (:swarm c))) "section/up names no single agent")))

(deftest new-nrepl-connections-count-as-use-a-lingering-one-does-not
  (let [c (ctx)
        _ (stub-runner/listen! (:runner c) repl-port)
        _ (ops/up-profile! c (section c "shop/api"))]
    (stub-swarm/peers! (:activity c) {repl-port #{"127.0.0.1:50000"}})
    (advance! c (* 61 minute))
    (is (empty? (:reaped (pass! c))) "a new connection to the shared nREPL is use")
    (advance! c (* 61 minute))
    (is (= ["shop/api"] (mapv :profile/id (:reaped (pass! c))))
        "the same connection an hour later is an editor left attached, not use")))

(deftest release-orphans-the-section-and-stop-frees-it-now
  (let [c (ctx)
        p (section c "shop/api")]
    (swarm/claim! c p "ling-1" {})
    (swarm/claim! c p "ling-2" {})
    (let [res (swarm/release-claim! c p "ling-1" {:stop? true})]
      (is (= ["ling-2"] (:claims (:ok res))))
      (is (= "not stopped: other agents still claim it" (:note (:ok res))))
      (is (active? c "shop/api")))
    (let [res (swarm/release-claim! c p "ling-2" {:stop? true})]
      (is (r/ok? res))
      (is (not (active? c "shop/api")))
      (is (= #{} (stub/running (:engine c) "shop")) "stopped at once")
      (is (= 1 (count (stub-runner/stops (:runner c)))) "its program is halted too"))
    (testing "releasing what is not claimed says so"
      (is (= :compose/not-active (:error (swarm/release-claim! c p "ling-2" {})))))))

(deftest release-without-stop-leaves-the-orphan-ttl-running
  (let [c (ctx)
        p (section c "shop/api")]
    (swarm/claim! c p "ling-1" {})
    (let [res (:ok (swarm/release-claim! c p "ling-1" {}))]
      (is (:orphaned? res))
      (is (= (* 10 60) (:reap-in-seconds res))))
    (advance! c (* 11 minute))
    (is (= ["shop/api"] (mapv :profile/id (:reaped (pass! c)))))))

(deftest up-again-keeps-the-claims
  (let [c (ctx)
        p (section c "shop/api")]
    (swarm/claim! c p "ling-1" {})
    (ops/up-profile! c p)
    (is (contains? (get-in @(:state c) [:active "shop/api" :claims]) "ling-1"))))

(deftest nrepl-routes-a-path-to-the-program-that-holds-it
  (let [c (ctx)]
    (stub-runner/listen! (:runner c) repl-port)
    (let [hit (:ok (swarm/nrepl c {:path "/w/shop/api/src/shop/core.clj"}))]
      (is (= repl-port (:nrepl-port hit)))
      (is (:external? hit) "it answers though no section holds it: somebody else runs it, share it"))
    (is (nil? (:route (:ok (swarm/nrepl c {:path "/w/blog/x.clj"})))))
    (is (= 1 (count (:routes (:ok (swarm/nrepl c {:project "shop"}))))))
    (is (= [] (:routes (:ok (swarm/nrepl c {:project "blog"})))))))

(deftest who-shows-claims-usage-and-where-agents-work
  (let [c (ctx [{:agent/id "ling-2" :agent/cwd "/w/shop"} {:agent/id "ling-3" :agent/cwd "/w/blog"}])
        p (section c "shop/api")]
    (swarm/claim! c p "ling-1" {:note "fix login"})
    (pass! c)
    (let [{:keys [sections agents]} (:ok (swarm/who c))
          [s] sections]
      (is (= "shop/api" (:id s)))
      (is (= {"ling-1" {:note "fix login" :since 0 :seen 0}} (:claims s)))
      (is (:in-use? s))
      (is (= [{:claim "ling-1"} {:agent "ling-2"}] (:used-by s)))
      (is (= {"ling-2" ["shop/api"] "ling-3" []}
             (into {} (map (juxt :agent/id :sections)) agents))))))

(deftest without-a-swarm-the-addon-runs-on-claims-alone
  (let [c (ctx)]
    (stub-swarm/absent! (:swarm c))
    (let [p (section c "shop/api")]
      (swarm/claim! c p "ling-1" {})
      (is (nil? (get-in @(:state c) [:active "shop/api" :claims "ling-1" :swarm?])))
      (let [pass (pass! c)]
        (is (= :absent (get-in pass [:observed :swarm])))
        (is (= ["shop/api"] (get-in pass [:observed :in-use])))))))

(deftest up-announces-the-shared-nrepls
  (let [c (ctx)]
    (stub-runner/listen! (:runner c) repl-port)
    (ops/up-profile! c (section c "shop/api"))
    (is (r/ok? (swarm/announce-up! c "shop/api")))
    (let [[event data] (last (stub-swarm/notices (:swarm c)))]
      (is (= :section/up event))
      (is (= "shop/api" (:section data))))
    (testing "nothing to tell for a section with no nREPL"
      (ops/up-profile! c (section c "blog/ghost"))
      (is (nil? (swarm/announce-up! c "blog/ghost"))))))
