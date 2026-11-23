(ns hive-compose.promote.swarm-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.promote.swarm :as sw]
            [hive-compose.schema]))

;; SPDX-License-Identifier: MIT

(deftest under-is-a-directory-prefix-not-a-string-prefix
  (is (sw/under? "/w/shop" "/w/shop"))
  (is (sw/under? "/w/shop/" "/w/shop/src/a.clj"))
  (is (not (sw/under? "/w/shop" "/w/shopping/src")) "a sibling sharing the prefix is not inside")
  (is (not (sw/under? "/w/shop" nil)))
  (is (sw/under? "/" "/anything"))
  (testing "a configured dir with .. segments matches the real tree it names"
    (is (sw/under? "/home/u/PP/funeraria/dc/../../funeraria-wt/app" "/home/u/PP/funeraria-wt/app/src/core.cljs"))
    (is (= "/home/u/PP/funeraria-wt/app" (sw/clean-dir "/home/u/PP/funeraria/dc/../../funeraria-wt/app/")))
    (is (not (sw/under? "/w/a/../b" "/w/a/src")))))

(deftest best-route-resolves-dotdot-dirs
  (let [routes [{:dir "/home/u/PP/funeraria/dc/../../funeraria-wt/escala" :nrepl-port 7909}]]
    (is (= 7909 (:nrepl-port (sw/best-route routes "/home/u/PP/funeraria-wt/escala/src/app/core.cljs"))))))

(deftest agents-in-picks-agents-whose-cwd-lies-in-the-dirs
  (let [agents [{:agent/id "b" :agent/cwd "/w/shop/api"}
                {:agent/id "a" :agent/cwd "/w/shop"}
                {:agent/id "c" :agent/cwd "/w"}
                {:agent/id "d"}]]
    (is (= ["a" "b"] (map :agent/id (sw/agents-in agents ["/w/shop"])))
        "a coordinator above the project does not count as working in it")))

(deftest concerned-joins-claimants-and-working-agents
  (let [working [{:agent/id "b" :agent/cwd "/w/shop/api"}]]
    (is (= [{:agent/id "a"} {:agent/id "b" :agent/cwd "/w/shop/api"}]
           (sw/concerned working ["b" "a" nil]))
        "a claimant also working keeps its row; one the swarm does not list stands by id")
    (is (= [] (sw/concerned [] [])))
    (is (= working (sw/concerned working nil)))))

(deftest a-claim-renews-and-keeps-its-start
  (let [c1 (sw/claim {} "ling-1" 100 {:note "fix login" :swarm? true})
        c2 (sw/claim c1 "ling-1" 500 {})]
    (is (= {:since 100 :seen 100 :note "fix login" :swarm? true} (get c1 "ling-1")))
    (is (= {:since 100 :seen 500 :note "fix login" :swarm? true} (get c2 "ling-1")))))

(deftest claims-expire-when-stale-or-when-their-agent-left-the-swarm
  (let [claims {"ling-1" {:since 0 :seen 0 :swarm? true}
                "ling-2" {:since 0 :seen 900 :swarm? true}
                "human" {:since 0 :seen 900}}]
    (testing "no swarm answers: only staleness ends a claim"
      (is (= {"ling-1" :stale} (sw/expired-claims claims nil 1000 1000))))
    (testing "the swarm no longer lists ling-2; a claim made outside the swarm is not judged by it"
      (is (= {"ling-1" :gone "ling-2" :gone} (sw/expired-claims claims #{"other"} 1000 5000))))))

;; ---------------------------------------------------------------------------
;; the liveness law (claim-end): wave claims and plain claims

(defn- legacy-end
  "The law before wave claims, kept here as the oracle for plain claims."
  [agent {:keys [seen swarm?]} live now ttl-ms]
  (cond
    (and swarm? live (not (contains? live agent))) :gone
    (>= (- now (or seen 0)) ttl-ms) :stale
    :else nil))

(def gen-agent (gen/elements ["ling-1" "ling-2" "coordinator:f07f8867"]))
(def gen-live (gen/one-of [(gen/return nil) (gen/set gen-agent)]))
(def gen-plain (gen/hash-map :since gen/nat :seen gen/nat :swarm? gen/boolean))
(def gen-wave (gen/fmap (fn [[c until]] (assoc c :until until)) (gen/tuple gen-plain gen/nat)))
(def gen-claim (gen/one-of [gen-plain gen-wave]))

(defspec claim-end-is-monotone-in-now 300
  (prop/for-all [agent gen-agent c gen-claim live gen-live
                 now gen/nat dt gen/nat ttl gen/s-pos-int]
    (or (nil? (sw/claim-end agent c live now ttl))
        (some? (sw/claim-end agent c live (+ now dt) ttl)))))

(defspec a-wave-claim-is-live-iff-now-is-before-until 300
  (prop/for-all [agent gen-agent c gen-wave live gen-live now gen/nat ttl gen/s-pos-int]
    (= (< now (:until c)) (sw/claim-live? agent c live now ttl))))

(defspec a-wave-claim-never-depends-on-the-swarm-roster 300
  (prop/for-all [agent gen-agent c gen-wave a gen-live b gen-live now gen/nat ttl gen/s-pos-int]
    (= (sw/claim-end agent c a now ttl) (sw/claim-end agent c b now ttl))))

(defspec plain-claims-keep-the-old-law 300
  (prop/for-all [agent gen-agent c gen-plain live gen-live now gen/nat ttl gen/s-pos-int]
    (= (legacy-end agent c live now ttl) (sw/claim-end agent c live now ttl))))

(defspec a-renewal-is-live-when-it-is-made 300
  ;; over (stored until or none, now, ttl given or not): whatever was stored,
  ;; the claim right after a renewal at `now` holds at `now`
  (prop/for-all [stored (gen/one-of [(gen/return nil) gen/nat])
                 now gen/nat
                 ttl (gen/one-of [(gen/return nil) gen/s-pos-int])
                 claim-ttl gen/s-pos-int]
    (let [old (cond-> {:since 0 :seen 0} stored (assoc :until stored))
          until (when ttl (+ now ttl))
          c (get (sw/claim {"a" old} "a" now {:until until}) "a")]
      (and (sw/claim-live? "a" c nil now claim-ttl)
           ;; a live wave claim renewed without ttl keeps its end; a lapsed one is plain
           (= (:until c) (cond until until
                               (and stored (> stored now)) stored
                               :else nil))))))

(deftest a-plain-renewal-after-the-wave-ended-is-a-plain-claim
  (let [c (sw/claim {"co" {:since 0 :seen 0 :until 1000 :note "gate"}} "co" 1500 {})]
    (is (= {:since 0 :seen 1500 :note "gate"} (get c "co")))
    (is (sw/lapsed-wave {:until 1000} 1500 nil))
    (is (not (sw/lapsed-wave {:until 1000} 999 nil)) "a live wave claim keeps its end")
    (is (not (sw/lapsed-wave {:until 1000} 1500 9000)) "a new ttl moves it instead")))

(deftest a-wave-claim-outlives-its-agent-and-ends-at-until
  (let [claims (sw/claim {} "coordinator:f07f8867" 0 {:note "pv01 gate" :swarm? true :until 1000})]
    (is (= {} (sw/expired-claims claims #{} 999 10)) "gone from the swarm and long unrenewed: still held")
    (is (= {"coordinator:f07f8867" :expired} (sw/expired-claims claims #{} 1000 10)))
    (testing "a renewal moves :until; one without leaves it"
      (is (= 5000 (get-in (sw/claim claims "coordinator:f07f8867" 500 {:until 5000}) ["coordinator:f07f8867" :until])))
      (is (= 1000 (get-in (sw/claim claims "coordinator:f07f8867" 500 {}) ["coordinator:f07f8867" :until]))))
    (testing "the view names wave? and expires-in-seconds"
      (is (= {:note "pv01 gate" :since 0 :seen 0 :wave? true :until 1000 :expires-in-seconds 0}
             (sw/claim-view (get claims "coordinator:f07f8867") 400 60000)))
      (is (= {:since 0 :seen 0 :wave? false :expires-in-seconds 60}
             (sw/claim-view {:since 0 :seen 0} 0 60000))))))

(deftest wave-until-is-bounded-and-needs-a-note
  (is (= {:ok nil} (sw/wave-until 0 nil 480 nil)) "no ttl_minutes: a plain claim")
  (is (= {:ok (* 240 60000)} (sw/wave-until 0 240 480 "pv01")))
  (let [over (sw/wave-until 0 481 480 "pv01")]
    (is (= :compose/claim-ttl-over-max (:error over)))
    (is (= 480 (:max over)) "the refusal names the bound"))
  (is (= :compose/invalid-param (:error (sw/wave-until 0 0 480 "pv01"))))
  (is (= :compose/missing-param (:error (sw/wave-until 0 60 480 "  "))))
  (is (= "note" (:param (sw/wave-until 0 60 480 nil)))))

(deftest a-stored-claim-without-until-still-loads
  (is (nil? (hive-compose.schema/explain hive-compose.schema/Claim {:since 1 :seen 2 :note "x" :swarm? false})))
  (is (nil? (hive-compose.schema/explain hive-compose.schema/Claim {:since 1 :seen 2 :until 3}))))

(deftest usage-names-every-reason
  (is (= {:in-use? false :by [] :sources []} (sw/usage {})))
  (is (= {:in-use? true
          :by [{:claim "a"} {:agent "ling-1"} {:port 7888 :peers 2}]
          :sources [:claim :agent :port]}
         (sw/usage {:claims {"a" {}} :agents [{:agent/id "ling-1"}]
                    :fresh {7888 #{"x" "y"} 9000 #{}}}))))

(deftest usage-names-the-source-of-each-port
  (is (= {:in-use? true
          :by [{:port 7888 :peers 1 :source :nrepl} {:port 18080 :peers 2 :source :published}]
          :sources [:nrepl :published]}
         (sw/usage {:fresh {7888 #{"a"} 18080 #{"b" "c"}}
                    :sources {7888 :nrepl 18080 :published}})))
  (is (= [:published] (:sources (sw/usage {:fresh {1 #{"a"} 2 #{"b"}} :sources {1 :published 2 :published}})))
      "a source is named once"))

(deftest peer-host-is-the-address-of-a-peer
  (is (= "172.18.0.3" (sw/peer-host "172.18.0.3:5432")))
  (is (= "::1" (sw/peer-host "[::1]:5432")))
  (is (nil? (sw/peer-host "nope"))))

(deftest a-sections-own-containers-are-not-its-use
  (let [ports {7888 :nrepl 18080 :published}
        fresh {7888 #{"172.18.0.3:1"} 18080 #{"172.18.0.3:2" "127.0.0.1:3"} 9999 #{"x:1"}}]
    (is (= {7888 #{"172.18.0.3:1"} 18080 #{"127.0.0.1:3"}}
           (sw/section-fresh fresh ports #{"172.18.0.3"}))
        "only the published port drops container peers; other sections' ports are not this one's")
    (is (= {7888 #{"172.18.0.3:1"}} (sw/section-fresh (update fresh 18080 disj "127.0.0.1:3") ports #{"172.18.0.3"}))
        "a published port left with only container peers is dropped")
    (is (= {7888 #{"172.18.0.3:1"} 18080 #{"172.18.0.3:2" "127.0.0.1:3"}} (sw/section-fresh fresh ports #{})))
    (is (sw/published-peers? fresh ports))
    (is (not (sw/published-peers? fresh {7888 :nrepl})))))

(deftest fresh-peers-counts-a-connection-once
  (is (= {7888 #{"b"}} (sw/fresh-peers {7888 #{"a"}} {7888 #{"a" "b"} 9000 #{}})))
  (is (= {} (sw/fresh-peers {7888 #{"a"}} {7888 #{"a"}})) "a pooled connection is not new use"))

(defspec fresh-peers-are-exactly-the-new-ones 100
  (prop/for-all [before (gen/map gen/nat (gen/set gen/string-alphanumeric))
                 now (gen/map gen/nat (gen/set gen/string-alphanumeric))]
    (let [fresh (sw/fresh-peers before now)]
      (and (every? (fn [[p peers]] (and (seq peers) (every? #(not (contains? (get before p #{}) %)) peers)
                                        (every? (get now p) peers)))
                   fresh)
           (every? (fn [[p peers]] (every? #(or (contains? (get before p #{}) %) (contains? (get fresh p) %)) peers))
                   now)))))

(deftest best-route-is-the-deepest-directory-holding-the-path
  (let [routes [{:dir "/w/shop" :nrepl-port 1}
                {:dir "/w/shop/frontend" :nrepl-port 2}
                {:dir "/w/blog" :nrepl-port 3}]]
    (is (= 2 (:nrepl-port (sw/best-route routes "/w/shop/frontend/src/app.cljs"))))
    (is (= 1 (:nrepl-port (sw/best-route routes "/w/shop/api/core.clj"))))
    (is (nil? (sw/best-route routes "/elsewhere/x.clj")))))

(deftest advice-says-what-to-drive-and-never-to-start-a-second-jvm
  (is (re-find #"port 7888" (sw/route-advice {:dir "/w" :nrepl-port 7888 :running? true :nrepl-ready? true}))) ;; pattern-exempt: test assertion on a literal
  (is (re-find #"wait" (sw/route-advice {:dir "/w" :nrepl-port 7888 :running? true}))) ;; pattern-exempt: test assertion on a literal
  (is (re-find #"claim" (sw/route-advice {:dir "/w" :nrepl-port 7888 :project "shop"}))) ;; pattern-exempt: test assertion on a literal
  (is (nil? (sw/route-advice nil))))

(deftest notices-go-once-per-agent-and-section-and-only-with-a-ready-route
  (let [working {"shop/api" [{:agent/id "ling-1"} {:agent/id "ling-2"}]
                 "blog/ghost" [{:agent/id "ling-3"}]}
        routes {"shop/api" [{:dir "/w/shop" :nrepl-port 1 :nrepl-ready? true}]
                "blog/ghost" [{:dir "/w/blog" :nrepl-port 3 :nrepl-ready? false}]}]
    (is (= [{:agent "ling-2" :section "shop/api" :routes (get routes "shop/api")}]
           (sw/notices working routes #{["ling-1" "shop/api"]})))))
