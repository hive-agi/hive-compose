(ns hive-compose.promote.swarm-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.promote.swarm :as sw]))

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

(deftest usage-names-every-reason
  (is (= {:in-use? false :by []} (sw/usage {})))
  (is (= {:in-use? true
          :by [{:claim "a"} {:agent "ling-1"} {:port 7888 :peers 2}]}
         (sw/usage {:claims {"a" {}} :agents [{:agent/id "ling-1"}]
                    :fresh {7888 #{"x" "y"} 9000 #{}}}))))

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
