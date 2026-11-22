(ns hive-compose.promote.faults-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.promote.faults :as sut]
            [hive-compose.schema :as schema]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def run "run1")

(defn- settings [needs-a needs-b]
  {:compose/projects
   {"db" {:project/id "db" :project/dir "/w/db"
          :project/provides {"pg" {:service "pg" :version "16" :port 5432}
                             "redis" {:service "redis" :port 6379}
                             "noport" {:service "np"}}}
    "a" {:project/id "a" :project/dir "/w/a" :project/needs needs-a}
    "b" {:project/id "b" :project/dir "/w/b" :project/needs needs-b}}
   :compose/suites {"smoke" {:suite/id "smoke" :suite/command "true"}}})

(def resolution
  {:resolution/instances
   {"db/pg" {:instance/id "db/pg" :instance/capability "pg" :instance/provider "db"
             :instance/service "pg" :instance/share? true :instance/consumers ["a" "b"]
             :instance/aliases ["pg"]}
    "db/redis" {:instance/id "db/redis" :instance/capability "redis" :instance/provider "db"
                :instance/service "redis" :instance/share? true :instance/consumers ["a"]
                :instance/aliases ["redis"]}
    "db/noport" {:instance/id "db/noport" :instance/capability "noport" :instance/provider "db"
                 :instance/service "np" :instance/share? true :instance/consumers ["a"]
                 :instance/aliases ["np"]}}
   :resolution/bindings {"a" {"pg" "db/pg" "redis" "db/redis"}
                         "b" {"pg" "db/pg"}
                         "suite:smoke" {}}})

(defn- wiring [consumer attach]
  {:wiring/consumer consumer :wiring/run run
   :wiring/network (schema/view-network run consumer)
   :wiring/attach attach :wiring/env {"HIVE_RUN_ID" run} :wiring/prefix (str run "." consumer ".")})

(def wirings
  {"a" (wiring "a" [{:instance "db/pg" :aliases ["pg"]} {:instance "db/redis" :aliases ["redis"]}])
   "b" (wiring "b" [{:instance "db/pg" :aliases ["pg"]}])
   "suite:smoke" (wiring "suite:smoke" [])})

(deftest no-via-no-proxy
  (let [p (:ok (sut/plan (settings {"pg" {}} {"pg" {}}) resolution run))]
    (is (not (sut/active? p)))
    (is (= [] (:faults/proxies p)))
    (is (= [] (sut/joins p)))
    (is (= wirings (:ok (sut/interpose p wirings))) "wirings untouched")))

(deftest one-proxy-per-consumer-and-capability
  (let [p (:ok (sut/plan (settings {"pg" {:via :toxiproxy} "redis" {}} {"pg" {:via :toxiproxy}})
                         resolution run))]
    (is (sut/active? p))
    (is (= sut/image (get-in p [:faults/toxiproxy :image])))
    (is (= ["a.pg" "b.pg"] (mapv :proxy/name (:faults/proxies p))))
    (is (= {:proxy/name "a.pg" :proxy/consumer "a" :proxy/capability "pg" :proxy/instance "db/pg"
            :proxy/aliases ["pg"] :proxy/port 5432
            :proxy/listen "toxiproxy-a:5432" :proxy/upstream "upstream-db-pg:5432"}
           (first (:faults/proxies p))))
    (testing "TOXIPROXY_URL for proxied consumers and suites"
      (is (= {"TOXIPROXY_URL" "http://toxiproxy-a:8474" "TOXIPROXY_PROXIES" "a.pg"}
             (get-in p [:faults/env "a"])))
      (is (= "a.pg,b.pg" (get-in p [:faults/env "suite:smoke" "TOXIPROXY_PROXIES"]))))
    (testing "toxiproxy takes the original alias; the instance only the faults network"
      (let [ws (:ok (sut/interpose p wirings))
            a (get ws "a")]
        (is (= [{:instance "db/redis" :aliases ["redis"]}
                {:instance "faults/toxiproxy" :aliases ["pg"] :internal-aliases ["toxiproxy-a"]}]
               (:wiring/attach a)))
        (is (= "http://toxiproxy-a:8474" (get-in a [:wiring/env "TOXIPROXY_URL"])))
        (is (= [] (:wiring/attach (get ws "suite:smoke"))))
        (is (every? #(nil? (schema/explain schema/Wiring %)) (vals ws)))))
    (is (= [{:network "hive-run1-a" :instance "faults/toxiproxy" :aliases ["pg" "toxiproxy-a"]}
            {:network "hive-run1-b" :instance "faults/toxiproxy" :aliases ["pg" "toxiproxy-b"]}
            {:network "hive-run1-suite-smoke" :instance "faults/toxiproxy" :aliases ["toxiproxy-suite-smoke"]}
            {:network "hive-run1-faults" :instance "db/pg" :aliases ["upstream-db-pg"]}]
           (sut/joins p)))))

(deftest consumer-env-wins
  (let [p (:ok (sut/plan (settings {"pg" {:via "toxiproxy"}} {}) resolution run))
        ws (:ok (sut/interpose p (assoc-in wirings ["a" :wiring/env "TOXIPROXY_URL"] "http://mine:1")))]
    (is (= "http://mine:1" (get-in ws ["a" :wiring/env "TOXIPROXY_URL"])))))

(deftest errors
  (testing "missing provider port"
    (let [res (sut/plan (settings {"noport" {:via :toxiproxy}} {})
                        (assoc-in resolution [:resolution/bindings "a" "noport"] "db/noport") run)]
      (is (= :compose/faults-missing-port (:error res)))
      (is (= "noport" (:capability res)))))
  (testing "unknown via"
    (is (= :compose/unknown-via (:error (sut/plan (settings {"pg" {:via :envoy}} {}) resolution run)))))
  (testing "invalid run"
    (is (= :compose/invalid-run (:error (sut/plan (settings {} {}) resolution "Bad-Run")))))
  (testing "unknown instance"
    (is (= :compose/unknown-instance
           (:error (sut/plan (settings {"pg" {:via :toxiproxy}} {})
                             (assoc-in resolution [:resolution/bindings "a" "pg"] "nope") run)))))
  (testing "one consumer, two capabilities on one port"
    (let [s (assoc-in (settings {"pg" {:via :toxiproxy} "redis" {:via :toxiproxy}} {})
                      [:compose/projects "db" :project/provides "redis" :port] 5432)]
      (is (= :compose/faults-port-clash (:error (sut/plan s resolution run)))))))

(deftest stack-needs-count
  (let [p (:ok (sut/plan (settings {} {}) resolution run {"b" {"pg" {:via :toxiproxy}}}))]
    (is (= ["b.pg"] (mapv :proxy/name (:faults/proxies p))))))

(deftest toxic-bodies
  (is (= {"name" "latency_downstream" "type" "latency" "stream" "downstream"
          "attributes" {"latency" 500 "jitter" 50}}
         (:ok (sut/toxic {:type "latency" :attributes {:latency 500 "jitter" 50}}))))
  (is (= 0.5 (get (:ok (sut/toxic {:type :timeout :attributes {:timeout 0} :toxicity 0.5
                                   :stream "upstream"})) "toxicity")))
  (is (= :compose/faults-unknown-toxic (:error (sut/toxic {:type "explode"}))))
  (is (= :compose/faults-invalid-toxic (:error (sut/toxic {:type "latency" :attributes {:rate 1}}))))
  (is (= :compose/faults-invalid-toxic (:error (sut/toxic {:type "latency" :stream "sideways"}))))
  (is (= :compose/faults-invalid-toxic (:error (sut/toxic {:type "latency" :toxicity 2})))))

(deftest spec-for-the-stack-engine
  (let [p (:ok (sut/plan (settings {"pg" {:via :toxiproxy}} {}) resolution run))
        spec (sut/project-spec p "/tmp/tp")]
    (is (= "hive-toxiproxy-faults-run1" (:project spec)))
    (is (= ["hive-run1-faults"] (:networks spec)))
    (is (= {"hive.run" "run1" "hive.instance" "faults/toxiproxy"} (:labels spec)))
    (is (:wait? spec))))

(defn- stack-plan
  "A StackPlan in the hc-4 shape (only the keys interpose-plan reads), as
   promote.stack/stack-plan builds it from `resolution`."
  [resolution]
  (let [bindings (:resolution/bindings resolution)
        binders (fn [iid] (sort (keep (fn [[c caps]] (when (some #{iid} (vals caps)) c)) bindings)))]
    {:stack/run run
     :stack/instances (into (sorted-map)
                            (map (fn [[iid i]]
                                   [iid (assoc i :instance/networks
                                               (mapv #(schema/view-network run %) (binders iid)))]))
                            (:resolution/instances resolution))
     :stack/consumers (into (sorted-map)
                            (map (fn [[c caps]]
                                   [c {:consumer/id c
                                       :consumer/network (schema/view-network run c)
                                       :consumer/attach (vec (for [iid (sort (distinct (vals caps)))]
                                                               {:instance iid
                                                                :aliases (get-in resolution [:resolution/instances iid :instance/aliases])}))}]))
                            bindings)
     :stack/networks (vec (sort (map #(schema/view-network run %) (keys bindings))))}))

(deftest the-stack-plan-follows-the-interposition
  (let [p (:ok (sut/plan (settings {"pg" {:via :toxiproxy}} {}) resolution run))
        sp (sut/interpose-plan p (stack-plan resolution))]
    (is (= ["hive-run1-b"] (get-in sp [:stack/instances "db/pg" :instance/networks]))
        "pg leaves a's view net and stays on b's, which is not interposed")
    (is (= [{:instance "db/redis" :aliases ["redis"]} {:instance "faults/toxiproxy" :aliases ["pg"]}]
           (get-in sp [:stack/consumers "a" :consumer/attach])))
    (is (= [{:instance "db/pg" :aliases ["pg"]}] (get-in sp [:stack/consumers "b" :consumer/attach])))
    (is (some #{"hive-run1-faults"} (:stack/networks sp)) "the faults network is created with the stack's")
    (is (= (stack-plan resolution)
           (sut/interpose-plan (:ok (sut/plan (settings {} {}) resolution run)) (stack-plan resolution))))))

(deftest teardown-reuses-the-project-spec
  (let [p (:ok (sut/plan (settings {"pg" {:via :toxiproxy}} {}) resolution run))
        [down rm] (sut/teardown p "/tmp/tp")]
    (is (= (assoc (sut/project-spec p "/tmp/tp") :all? true) (:spec down)))
    (is (= ["/tmp/tp/compose.yml"] (get-in down [:spec :files])))
    (is (= {:step :network-rm :network "hive-run1-faults"} rm))))

(deftest toxic-never-throws-on-json
  (doseq [spec [nil 5 "latency" {:type 5} {:type "latency" :stream 7} {:type "latency" :attributes [1 2]}
                {:type "latency" :attributes {5 1}} {:type {:x 1}} {:name 3}]]
    (is (r/err? (sut/toxic spec)) (pr-str spec)))
  (is (nil? (sut/toxic-name {:type 5})))
  (is (= "latency_downstream" (get (:ok (sut/toxic {:type "latency" :name 3})) "name")))
  (is (= "latency_upstream" (sut/toxic-name {:type :latency :stream "upstream"}))))

;; ---------------------------------------------------------------------------
;; Properties

(def gen-consumer (gen/elements ["a" "b" "c" "d" "suite:s1"]))

(def gen-world
  "Consumers, each bound to a random subset of three capabilities, and a
   random :via per (consumer, capability)."
  (gen/let [binds (gen/map gen-consumer (gen/set (gen/elements ["pg" "redis" "kafka"])) {:min-elements 1})
            vias (gen/vector gen/boolean 15)]
    (let [ports {"pg" 5432 "redis" 6379 "kafka" 9092}
          instances (into {} (for [cap (keys ports)]
                               [(str "p/" cap) {:instance/id (str "p/" cap) :instance/capability cap
                                                :instance/provider "p" :instance/service cap
                                                :instance/share? true :instance/consumers ["a"]
                                                :instance/aliases [cap (str cap "-alt")]}]))
          needs (into {} (map-indexed
                          (fn [i [c caps]]
                            [c (into {} (map-indexed (fn [j cap]
                                                       [cap (if (nth vias (mod (+ (* 3 i) j) 15))
                                                              {:via :toxiproxy} {})])
                                                     (sort caps)))])
                          (sort binds)))
          settings {:compose/projects
                    (merge {"p" {:project/id "p" :project/dir "/p"
                                 :project/provides (into {} (for [[cap port] ports]
                                                              [cap {:service cap :port port}]))}}
                           (into {} (for [[c n] needs :when (not (sut/suite? c))]
                                      [c {:project/id c :project/dir (str "/" c) :project/needs n}])))
                    :compose/suites (into {} (for [[c n] needs :when (sut/suite? c)]
                                               [(subs c 6) {:suite/id (subs c 6) :suite/command "x"
                                                            :suite/needs n}]))}
          resolution {:resolution/instances instances
                      :resolution/bindings (into {} (for [[c caps] binds]
                                                      [c (into {} (map (fn [cap] [cap (str "p/" cap)])) caps)]))}
          wirings (into {} (for [[c caps] binds]
                             [c (wiring c (vec (for [cap (sort caps)]
                                                 {:instance (str "p/" cap) :aliases [cap (str cap "-alt")]})))]))]
      {:settings settings :resolution resolution :wirings wirings :needs needs})))

(defn- alias-set [w] (set (mapcat :aliases (:wiring/attach w))))

(defspec interposition-never-changes-a-consumers-aliases 100
  (prop/for-all [{:keys [settings resolution wirings]} gen-world]
    (let [p (sut/plan settings resolution run)
          ws (r/bind p #(sut/interpose % wirings))]
      (and (r/ok? ws)
           (= (set (keys wirings)) (set (keys (:ok ws))))
           (every? (fn [[c w]] (= (alias-set w) (alias-set (get (:ok ws) c)))) wirings)))))

(defspec an-interposed-instance-never-joins-its-consumers-view-network 100
  (prop/for-all [{:keys [settings resolution]} gen-world]
    (let [p (:ok (sut/plan settings resolution run))
          sp (sut/interpose-plan p (stack-plan resolution))]
      (and (every? (fn [{:proxy/keys [consumer instance]}]
                     (not (some #{(schema/view-network run consumer)}
                                (get-in sp [:stack/instances instance :instance/networks]))))
                   (:faults/proxies p))
           (every? (fn [[c cp]]
                     (= (set (mapcat :aliases (get-in (stack-plan resolution) [:stack/consumers c :consumer/attach])))
                        (set (mapcat :aliases (:consumer/attach cp)))))
                   (:stack/consumers sp))))))

(defspec a-need-without-via-yields-no-proxy 100
  (prop/for-all [{:keys [settings resolution needs]} gen-world]
    (let [p (:ok (sut/plan settings resolution run))
          proxied (set (map (juxt :proxy/consumer :proxy/capability) (:faults/proxies p)))
          wanted (set (for [[c n] needs [cap need] n :when (:via need)] [c cap]))]
      (= wanted proxied))))
