(ns hive-compose.pipeline.swarm-test
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.pipeline.ops :as ops]
            [hive-compose.pipeline.swarm :as swarm]
            [hive-compose.port :as port]
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

(defn- ups [c] (filterv #(#{:up :restart :recreate} (first %)) (stub/calls (:engine c))))

(deftest a-claim-on-an-active-section-never-starts-anything
  ;; staging already skipped up for a section in THIS instance's map: the
  ;; up-count assertions here are regression guards; :renewed and the
  ;; target= lookups are red on staging. The D1 red tests proper are
  ;; a-claim-on-a-running-untracked-section-takes-it-without-an-up and
  ;; a-claim-sees-a-section-another-instance-brought-up.
  (let [c (ctx)
        p (section c "shop/api")]
    (is (contains? (:ok (swarm/claim! c p "ling-1" {})) :started) "the first claim brings it up")
    (let [before (ups c)
          res (:ok (swarm/claim! c p "ling-2" {:note "renew"}))]
      (is (= before (ups c)) "no up, restart or recreate on a renewal")
      (is (not (contains? res :started)))
      (is (= {:profile "shop/api" :services ["api" "postgres"]} (:renewed res))))
    (testing "the same by preset id: profile= and target= both find the active section"
      (let [c (ctx)
            c (assoc-in c [:settings :compose/profiles "flow"]
                        {:profile/id "flow" :profile/dir "/w/shop" :profile/project "shop" :profile/services ["api"]})
            p (:ok (ops/section-subject c {:id "flow"}))]
        (swarm/claim! c p "ling-1" {})
        (let [before (ups c)]
          (doseq [call [{:id "flow"} {:target "flow"} {:target "shop/flow"}]]
            (let [res (r/bind (ops/section-subject c call) #(swarm/claim! c % "ling-2" {}))]
              (is (r/ok? res) (str call))
              (is (:renewed (:ok res)) (str call))))
          (is (= before (ups c))))))))

(deftest a-claim-on-a-running-untracked-section-takes-it-without-an-up
  (testing "its services run (another section or instance started them): claim tracks it, starts nothing"
    (let [c (ctx)
          _ (ops/up-profile! c (section c "shop/api"))
          _ (swap! (:state c) update :active dissoc "shop/api")
          before (ups c)
          res (:ok (swarm/claim! c (section c "shop/api") "ling-1" {}))]
      (is (= before (ups c)))
      (is (not (contains? res :started)))
      (is (= ["api" "postgres"] (get-in res [:adopted :services])))
      (is (= ["ling-1"] (:claims res)))))
  (testing "when something it needs is down, it is brought up as before"
    (let [c (ctx)
          res (:ok (swarm/claim! c (section c "shop/api") "ling-1" {}))]
      (is (contains? res :started))
      (is (= [[:up "shop" ["api" "postgres"]]] (ups c))))))

(deftest an-adopted-section-starts-its-host-programs
  (let [c (ctx)
        _ (ops/up-profile! c (section c "shop/api"))
        _ (swap! (:state c) update :active dissoc "shop/api")
        starts (count (stub-runner/starts (:runner c)))
        before (ups c)
        res (:ok (swarm/claim! c (section c "shop/api") "ling-1" {}))]
    (is (= before (ups c)) "still no engine start")
    (is (= (inc starts) (count (stub-runner/starts (:runner c)))) "the paired nREPL program is started")
    (is (= ["api-repl"] (keys (get-in @(:state c) [:active "shop/api" :programs]))) "and recorded, so release stops it")
    (is (= [true] (mapv :started? (get-in res [:adopted :programs])))))
  (testing "a program that cannot start: the claim still holds and says why"
    (let [c (ctx)
          _ (ops/up-profile! c (section c "shop/api"))
          _ (swap! (:state c) update :active dissoc "shop/api")
          _ (stub-runner/fail! (:runner c) :start)
          res (swarm/claim! c (section c "shop/api") "ling-1" {})]
      (is (r/ok? res))
      (is (= ["ling-1"] (:claims (:ok res))))
      (is (= "api-repl" (get-in res [:ok :adopted :programs-not-started 0 :program/id])))
      (is (some? (get-in res [:ok :adopted :programs-not-started 0 :error]))))))

(defn- gated-runner
  "A program runner over stub runner `rn` whose -start! counts starts and, on
   the first, waits until both claimers have entered claim! (`entered`) and
   then up to 2 s for a second start (`second`): a claim that is not
   serialized starts the program twice inside that window, a serialized one
   cannot (the second claimer waits on the section lock)."
  [rn entered starts second]
  (reify port/IProgramRunner
    (-facts [_ dir] (port/-facts rn dir))
    (-start! [_ p log]
      (swap! starts inc)
      (.countDown ^java.util.concurrent.CountDownLatch second)
      (.await ^java.util.concurrent.CountDownLatch entered 5 java.util.concurrent.TimeUnit/SECONDS)
      (.await ^java.util.concurrent.CountDownLatch second 2 java.util.concurrent.TimeUnit/SECONDS)
      (port/-start! rn p log))
    (-alive? [_ pid] (port/-alive? rn pid))
    (-listening? [_ port] (port/-listening? rn port))
    (-nrepl-port [_ dir] (port/-nrepl-port rn dir))
    (-halt! [_ pid] (port/-halt! rn pid))))

(deftest concurrent-claims-on-an-untracked-running-section-start-its-programs-once
  (let [entered (java.util.concurrent.CountDownLatch. 2)
        second (java.util.concurrent.CountDownLatch. 2)
        starts (atom 0)
        c (assoc (ctx)
                 :engine (stub/engine {} {"shop" #{"api" "postgres"}} model)
                 :runner (gated-runner (stub-runner/runner) entered starts second))
        p (section c "shop/api")
        claim #(future (.countDown entered) (swarm/claim! c p % {}))
        a (claim "ling-1")
        b (claim "ling-2")
        res [(deref a 10000 ::timeout) (deref b 10000 ::timeout)]]
    (is (every? r/ok? res) (pr-str res))
    (is (= 1 @starts) "the paired nREPL program is started once, not once per claimer")
    (is (= #{:adopted :renewed} (set (map #(some #{:adopted :renewed} (keys (:ok %))) res)))
        "one claimer adopts, the other finds it tracked and renews")
    (is (= ["api-repl"] (keys (get-in @(:state c) [:active "shop/api" :programs]))) "recorded once")
    (is (= ["ling-1" "ling-2"] (sort (keys (get-in @(:state c) [:active "shop/api" :claims])))))
    (is (= [] (ups c)) "no engine start")))

(deftest a-wave-claims-end-counts-from-after-a-slow-start
  (let [c (ctx)
        p (section c "shop/api")
        res (:ok (swarm/claim! c p "coordinator:f07f8867"
                               {:note "gate" :ttl-minutes 1
                                :start! #(do (advance! c (* 2 minute)) (ops/up-profile! c p))}))]
    (is (= (* 3 minute) (get-in res [:claim :until])) "now after the 2-minute up, plus 1 minute")
    (is (= 60 (get-in res [:claim :expires-in-seconds])))))

(deftest a-plain-renewal-after-the-owners-wave-ended-keeps-the-section
  (let [c (ctx)
        p (section c "shop/api")]
    (swarm/claim! c p "coordinator:f07f8867" {:note "gate" :ttl-minutes 60})
    (advance! c (* 61 minute))
    (let [res (:ok (swarm/claim! c p "coordinator:f07f8867" {}))]
      (is (string? (:wave-ended res)) "the answer says the wave claim had ended")
      (is (false? (get-in res [:claim :wave?]))))
    (let [pass (pass! c)]
      (is (empty? (get-in pass [:observed :expired])) "the renewal holds: not dropped as :expired")
      (is (empty? (:reaped pass))))
    (is (active? c "shop/api"))))

(deftest a-claim-never-resurrects-a-section-forgotten-meanwhile
  ;; the section is not tracked and its start answers ok, but a reaper pass
  ;; forgot it meanwhile (here: the start leaves no entry): the claim fails
  ;; instead of writing an entry with only :claims and :last-touch
  (let [c (ctx)
        p (section c "shop/api")
        res (swarm/claim! c p "ling-3" {:start! #(r/ok {:profile "shop/api"})})]
    (is (= :compose/not-active (:error res)))
    (is (nil? (get-in @(:state c) [:active "shop/api"])))))

(deftest a-claim-sees-a-section-another-instance-brought-up
  ;; two addon instances share one state file; :sync! merges it in. The second
  ;; instance's claim must see the first's section and renew, not start it.
  (let [c1 (ctx)
        _ (ops/up-profile! c1 (section c1 "shop/api"))
        c2 (ctx)
        c2 (assoc c2 :sync! (fn [] (swap! (:state c2) assoc :active (:active @(:state c1)))))
        res (:ok (swarm/claim! c2 (section c2 "shop/api") "ling-2" {}))]
    (is (:renewed res) "a renewal, not a start")
    (is (= [] (ups c2)) "no up on the second instance's engine")))

(deftest claim-and-release-accept-a-section-or-preset-id-as-target
  (let [c (assoc-in (ctx) [:settings :compose/profiles "flow"]
                    {:profile/id "flow" :profile/dir "/w/shop" :profile/project "shop" :profile/services ["api"]})]
    (is (= "flow" (:profile/id (:ok (ops/section-subject c {:target "flow"})))) "a preset id")
    (is (= "flow" (:profile/id (:ok (ops/section-subject c {:target "shop/flow"})))) "behind its compose project")
    (is (= "shop/api" (:profile/id (:ok (ops/section-subject c {:target "shop/api"})))) "a target still resolves")
    (ops/up-profile! c (section c "blog/ghost"))
    (is (= "blog/ghost" (:profile/id (:ok (ops/section-subject c {:target "blog/ghost"})))) "an active section id")
    (is (= :compose/unknown-target (:error (ops/section-subject c {:target "nope"}))))))

(deftest a-wave-claim-outlives-its-coordinator-and-lanes-claim-on-top
  (let [c (ctx [{:agent/id "coordinator:f07f8867"} {:agent/id "ling-1" :agent/cwd "/elsewhere"}])
        p (section c "shop/api")]
    (testing "bounded, and it needs a note"
      (is (= :compose/claim-ttl-over-max (:error (swarm/claim! c p "coordinator:f07f8867" {:note "gate" :ttl-minutes 481}))))
      (is (= :compose/missing-param (:error (swarm/claim! c p "coordinator:f07f8867" {:ttl-minutes 240}))))
      (is (not (active? c "shop/api")) "a refused wave claim starts nothing"))
    (let [res (:ok (swarm/claim! c p "coordinator:f07f8867" {:note "pv01 gate" :ttl-minutes 240}))]
      (is (= {:note "pv01 gate" :since 0 :seen 0 :wave? true :until (* 240 minute) :expires-in-seconds (* 240 60)}
             (:claim res))))
    (swarm/claim! c p "ling-1" {:note "scored run"})
    (swarm/release-claim! c p "ling-1" {})
    (is (contains? (get-in @(:state c) [:active "shop/api" :claims]) "coordinator:f07f8867")
        "releasing an ordinary claim leaves the wave claim")
    (stub-swarm/agents! (:swarm c) [])
    (dotimes [_ 7]
      (advance! c (* 30 minute))
      (let [pass (pass! c)]
        (is (empty? (get-in pass [:observed :expired])) "parked or gone, unrenewed: the wave claim holds")
        (is (empty? (:reaped pass)))))
    (is (active? c "shop/api") "3.5 h on, past the section TTL and the claim TTL: kept")
    (let [[s] (:sections (:ok (swarm/who c)))
          w (get-in s [:claims "coordinator:f07f8867"])]
      (is (:wave? w))
      (is (= (* 30 60) (:expires-in-seconds w)))
      (is (= "pv01 gate" (:note w))))
    (testing "renewing moves its end"
      (swarm/claim! c p "coordinator:f07f8867" {:note "pv01 gate" :ttl-minutes 60})
      (is (= (* 270 minute) (get-in @(:state c) [:active "shop/api" :claims "coordinator:f07f8867" :until]))))
    (advance! c (* 61 minute))
    (let [pass (pass! c)]
      (is (= [{:section "shop/api" :agent "coordinator:f07f8867" :reason :expired}] (get-in pass [:observed :expired])))
      (is (= ["shop/api"] (get-in pass [:observed :orphaned])) "an ended wave claim orphans like any other"))
    (advance! c (* 11 minute))
    (pass! c)
    (is (not (active? c "shop/api")) "then the reaper takes it like any orphan")))

(deftest release-ends-a-wave-claim-at-once
  (let [c (ctx)
        p (section c "shop/api")]
    (swarm/claim! c p "coordinator:f07f8867" {:note "gate" :ttl-minutes 480})
    (let [res (:ok (swarm/release-claim! c p "coordinator:f07f8867" {}))]
      (is (= [] (:claims res)))
      (is (:orphaned? res)))))

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
      (is (= {"ling-1" {:note "fix login" :since 0 :seen 0 :wave? false :expires-in-seconds (* 30 60)}} (:claims s)))
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
