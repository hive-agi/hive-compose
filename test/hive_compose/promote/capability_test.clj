(ns hive-compose.promote.capability-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.promote.capability :as cap]
            [hive-compose.schema :as schema]
            [malli.core :as m]))

;; SPDX-License-Identifier: MIT

(defn- project [id & {:as m}]
  (merge {:project/id id :project/dir (str "/w/" id)} m))

(def projects
  {"bus" (project "bus" :project/provides {"kafka" {:service "kafka" :version "3.7"}})
   "db" (project "db" :project/provides {"pg" {:service "postgres" :version "16.2"}})
   "shop" (project "shop" :project/needs {"kafka" {:version "^3"} "pg" {:version ">=15"}})
   "crm" (project "crm" :project/needs {"kafka" {:version ">=3.5 <4"}})
   "iso" (project "iso" :project/needs {"kafka" {:share? false}})})

(def two-kafkas
  (assoc projects "old" (project "old" :project/provides {"kafka" {:service "broker" :version "2.8"}})))

(defn- ok [r] (is (contains? r :ok) (pr-str r)) (:ok r))

(defn- needs [consumers & [extra]] (:ok (cap/consumer-needs projects consumers (or extra {}))))

(defn- are-ranges [cases]
  (doseq [[rng v expected] cases]
    (is (= expected (cap/version-satisfies? rng v)) (str (pr-str rng) " vs " v))))

;; ---------------------------------------------------------------------------
;; Versions and ranges

(deftest ranges-follow-the-grammar
  (testing "prefix, caret, tilde"
    (are-ranges [["*" "0.1" true] ["*" "99" true] [nil "1" true]
                 ["3" "3.7.1" true] ["3" "4.0" false] ["3" "30" false]
                 ["3.7" "3.7.9" true] ["3.7" "3.8" false] ["3.7.1" "3.7.1" true] ["3.7.1" "3.7.2" false]
                 ["^3" "3.0" true] ["^3" "3.99.1" true] ["^3" "4" false] ["^3" "2.9" false]
                 ["^3.7" "3.6" false] ["^3.7" "3.9" true] ["^3.7.1" "3.7.0" false] ["^3.7.1" "3.99" true]
                 ["^0.5" "0.5.3" true] ["^0.5" "0.6" false] ["^0.0.3" "0.0.4" false]
                 ["~3.7" "3.7.4" true] ["~3.7" "3.8" false] ["~3" "3.9" true] ["~3" "4.0" false]
                 ["~3.7.1" "3.7.9" true] ["~3.7.1" "3.7.0" false] ["~3.7.1" "3.8" false]]))
  (testing "comparators read partial versions as X-ranges"
    (are-ranges [[">=3.5 <4" "3.5" true] [">=3.5 <4" "3.4.9" false] [">=3.5 <4" "4.0" false]
                 [">3" "3.5" false] [">3" "4.0" true] [">3.0.0" "3.0.1" true]
                 ["<=3.7" "3.7.5" true] ["<=3.7" "3.8" false] ["<3.7" "3.6.9" true] ["<3.7" "3.7.0" false]
                 ["=3.7" "3.7.5" true] ["=3.7" "3.8" false] ["=3.7.0" "3.7.0" true]]))
  (testing "suffixes: tags inside the prefix block, pre-releases below the release"
    (are-ranges [["3.7" "3.7.1-alpine" true] ["3.7" "3.7.0-alpine" true] ["3.7.1" "3.7.1-alpine" true]
                 [">=3.7.1" "3.7.1-rc1" false] [">=3.7.1-rc1" "3.7.1-rc2" true] [">=3.7.1-rc.10" "3.7.1-rc.9" false]
                 [">=3.7.1" "3.7.1+build5" true] ["16" "16.2-alpine" true]]))
  (testing "invalid ranges and versions satisfy nothing"
    (doseq [rng ["" "x" "3 4" "^" ">=" "3.x" "v3" ">= 3.5" "3\t4" "99999999999999999999"]]
      (is (= :compose/invalid-range (:error (cap/parse-range rng))) (pr-str rng)))
    (is (= :compose/invalid-version (:error (cap/parse-version "latest"))))
    (is (= :compose/invalid-version (:error (cap/parse-version "99999999999999999999"))))
    (is (false? (cap/version-satisfies? "*" "latest"))))
  (testing "an unversioned provider satisfies only *"
    (is (true? (cap/version-satisfies? "*" nil)))
    (is (false? (cap/version-satisfies? "3" nil)))))

(deftest versions-compare-numerically-then-by-pre-release
  (let [v #(:ok (cap/parse-version %))]
    (is (zero? (cap/compare-versions (v "3") (v "3.0.0"))))
    (is (neg? (cap/compare-versions (v "3.9") (v "3.10"))))
    (is (neg? (cap/compare-versions (v "3.7.1-rc1") (v "3.7.1"))))
    (is (neg? (cap/compare-versions (v "1.0.0-alpha") (v "1.0.0-alpha.1"))))
    (is (neg? (cap/compare-versions (v "1.0.0-1") (v "1.0.0-alpha"))))
    (is (zero? (cap/compare-versions (v "1.0.0+a") (v "1.0.0+b"))))
    (is (= {:nums [3 7 1] :pre "alpine"} (v "3.7.1-alpine")))))

(def valid-range? (m/validator schema/VersionRange))

(defspec range-parser-agrees-with-the-schema 300
  (prop/for-all [s (gen/one-of [gen/string-ascii
                                (gen/fmap (fn [[op a b]] (str op a "." b))
                                          (gen/tuple (gen/elements ["" "^" "~" ">=" "<" "=" ">" "<=" "v" "*"])
                                                     gen/nat gen/nat))])]
    (= (boolean (valid-range? s)) (contains? (cap/parse-range s) :ok))))

;; ---------------------------------------------------------------------------
;; Consumers

(deftest consumer-needs-merge-key-by-key
  (is (= {"crm" {"kafka" {:version ">=3.5 <4" :share? false}}}
         (needs ["crm"] {"crm" {"kafka" {:share? false}}}))
      "an extra :share? false keeps the project's range")
  (is (= {"iso/web" {"kafka" {:share? false}}} (needs ["iso/web"])) "a section inherits its project's needs")
  (let [e (cap/consumer-needs projects ["shop" "shpo" "x/y"] {})]
    (is (= :compose/unknown-consumer (:error e)))
    (is (= ["shpo" "x/y"] (:consumers e))))
  (is (= {"suite:smoke" {"pg" {}}} (:ok (cap/consumer-needs projects ["suite:smoke"] {"suite:smoke" {"pg" {}}})))))

;; ---------------------------------------------------------------------------
;; Resolution examples

(deftest compatible-consumers-share-one-instance
  (let [res (ok (cap/resolve-needs projects (needs ["shop" "crm"])))]
    (is (= {"crm" {"kafka" "bus/kafka"}
            "shop" {"kafka" "bus/kafka" "pg" "db/pg"}}
           (:resolution/bindings res)))
    (is (= {:instance/id "bus/kafka" :instance/capability "kafka" :instance/provider "bus"
            :instance/service "kafka" :instance/version "3.7" :instance/share? true
            :instance/consumers ["crm" "shop"] :instance/aliases ["kafka"]}
           (get-in res [:resolution/instances "bus/kafka"])))))

(deftest opt-out-gets-a-private-instance
  (let [res (ok (cap/resolve-needs projects (needs ["shop" "iso/web"]) {:run "r1"}))
        priv (get-in res [:resolution/instances "bus/kafka@iso/web"])]
    (is (= "bus/kafka@iso/web" (get-in res [:resolution/bindings "iso/web" "kafka"])))
    (is (= ["iso/web"] (:instance/consumers priv)))
    (is (false? (:instance/share? priv)))
    (is (= "bus-iso-web-r1" (:instance/compose-project priv)) "schema/private-project")
    (is (= "bus" (get-in res [:resolution/instances "bus/kafka" :instance/compose-project])))
    (is (= ["shop"] (get-in res [:resolution/instances "bus/kafka" :instance/consumers])))
    (is (nil? (schema/explain schema/Resolution res)))))

(deftest stacks-bind-their-own-needs
  (let [res (ok (cap/resolve-stack projects {:stack/id "e2e" :stack/consumers ["crm"] :stack/needs {"pg" {}}}))]
    (is (= {"crm" {"kafka" "bus/kafka"} "stack:e2e" {"pg" "db/pg"}} (:resolution/bindings res))))
  (is (= {"bus" {}} (:resolution/bindings (ok (cap/resolve-stack projects {:stack/id "s" :stack/consumers ["bus"]})))))
  (is (= :compose/unknown-consumer (:error (cap/resolve-stack projects {:stack/id "s" :stack/consumers ["nope"]})))))

(deftest incompatible-sharers-are-never-split
  (testing "each range fits a different provider: an error, not two shared instances"
    (let [r (cap/resolve-needs two-kafkas {"a" {"kafka" {:version "^3"}} "b" {"kafka" {:version "^2"}}})]
      (is (= :compose/incompatible-version (:error r)))
      (is (= ["a" "b"] (:consumers r)))
      (is (= {"a" "^3" "b" "^2"} (:ranges r)))
      (is (true? (:split? r)))
      (is (re-find #"share\? false" (:message r)))))
  (testing "opting one out resolves both"
    (let [res (ok (cap/resolve-needs two-kafkas {"a" {"kafka" {:version "^3"}}
                                                 "b" {"kafka" {:version "^2" :share? false}}}))]
      (is (= {"a" {"kafka" "bus/kafka"} "b" {"kafka" "old/kafka@b"}} (:resolution/bindings res))))))

(deftest missing-provider-names-consumers
  (let [r (cap/resolve-needs projects {"a" {"redis" {}} "b" {"redis" {:version "7" :share? false}}})]
    (is (= :compose/no-provider (:error r)))
    (is (= "redis" (:capability r)))
    (is (= ["a" "b"] (:consumers r)) "shared and private needs merge into one error")
    (is (= {"a" "*" "b" "7"} (:ranges r)))
    (is (= "no project provides redis, needed by a, b" (:message r))))
  (testing "a pin naming a project that does not provide it"
    (let [r (cap/resolve-needs projects {"a" {"kafka" {:provider "db"}}})]
      (is (= :compose/no-provider (:error r)))
      (is (= "db" (:provider r))))))

(deftest incompatible-version-names-consumers-and-providers
  (let [r (cap/resolve-needs projects {"a" {"kafka" {:version "^2"}}
                                       "b" {"kafka" {:version "4"}}
                                       "c" {"kafka" {:version "^3"}}})]
    (is (= :compose/incompatible-version (:error r)))
    (is (= ["a" "b"] (:consumers r)) "c fits and is not named")
    (is (= {"a" "^2" "b" "4"} (:ranges r)))
    (is (= {"bus" "3.7"} (:providers r))))
  (testing "a pinned provider whose version misfits"
    (let [r (cap/resolve-needs two-kafkas {"a" {"kafka" {:provider "old" :version "^3"}}})]
      (is (= :compose/incompatible-version (:error r)))
      (is (= {"old" "2.8"} (:providers r)))
      (is (= "old" (:provider r)))))
  (testing "an unversioned provider only fits *"
    (let [ps {"x" (project "x" :project/provides {"redis" {:service "redis"}})}]
      (is (= :compose/incompatible-version (:error (cap/resolve-needs ps {"a" {"redis" {:version "^7"}}}))))
      (is (= {"a" {"redis" "x/redis"}} (:resolution/bindings (ok (cap/resolve-needs ps {"a" {"redis" {}}}))))))))

(deftest ambiguous-provider-needs-a-pin
  (let [ps (assoc projects "bus2" (project "bus2" :project/provides {"kafka" {:service "broker" :version "3.6"}}))
        r (cap/resolve-needs ps {"a" {"kafka" {:version "^3"}} "b" {"kafka" {}}})]
    (is (= :compose/ambiguous-provider (:error r)))
    (is (= ["bus" "bus2"] (:providers r)))
    (is (= ["a" "b"] (:consumers r)))
    (testing "one pin among sharers resolves every sharer"
      (is (= {"a" {"kafka" "bus2/kafka"} "b" {"kafka" "bus2/kafka"}}
             (:resolution/bindings (ok (cap/resolve-needs ps {"a" {"kafka" {:provider "bus2"}} "b" {"kafka" {}}}))))))
    (testing "conflicting pins among sharers"
      (let [r (cap/resolve-needs ps {"a" {"kafka" {:provider "bus2"}} "b" {"kafka" {:provider "bus"}}})]
        (is (= :compose/conflicting-pins (:error r)))
        (is (= {"a" "bus2" "b" "bus"} (:pins r)))))
    (testing "a range only one provider fits is not ambiguous"
      (is (= {"a" {"kafka" "bus2/kafka"}}
             (:resolution/bindings (ok (cap/resolve-needs ps {"a" {"kafka" {:version "3.6"}}}))))))))

(deftest invalid-inputs-are-errors
  (let [r (cap/resolve-needs projects {"a" {"kafka" {:version "three"}}})]
    (is (= :compose/invalid-range (:error r)))
    (is (= ["a"] (:consumers r)))
    (is (= {"a" "three"} (:ranges r)))
    (is (re-find #"three" (:message r))))
  (let [ps {"x" (project "x" :project/provides {"kafka" {:service "kafka" :version "latest"}})}
        r (cap/resolve-needs ps {"a" {"kafka" {}}})]
    (is (= :compose/invalid-version (:error r)))
    (is (= {"x" "latest"} (:providers r)))))

(deftest every-error-group-is-reported-in-order
  (let [r (cap/resolve-needs projects {"a" {"redis" {}} "b" {"kafka" {:version "^9"}} "c" {"pg" {:version "^9"}}})]
    (is (= [[:compose/no-provider "redis"] [:compose/incompatible-version "kafka"]
            [:compose/incompatible-version "pg"]]
           (mapv (juxt :error :capability) (:errors r))))
    (is (= (dissoc r :errors) (dissoc (first (:errors r)) :errors)))))

;; ---------------------------------------------------------------------------
;; Properties

(def caps ["kafka" "pg" "redis"])

(def gen-version
  (gen/let [a (gen/choose 1 3) b (gen/choose 0 3)
            v (gen/frequency [[8 (gen/return (str a "." b))] [1 (gen/return (str a))]])]
    v))

(def gen-range
  (gen/one-of [(gen/return "*")
               (gen/fmap str (gen/choose 1 3))
               (gen/fmap #(str "^" %) (gen/choose 1 3))
               (gen/fmap (fn [[a b]] (str "~" a "." b)) (gen/tuple (gen/choose 1 3) (gen/choose 0 3)))
               (gen/fmap (fn [[a b]] (str ">=" a "." b " <" (inc a))) (gen/tuple (gen/choose 1 3) (gen/choose 0 3)))]))

(def gen-world
  "Projects p0..pN. Each capability has 0-3 providers at random versions
   (sometimes none, sometimes unversioned); each project needs some
   capabilities with a random range, :share? and an occasional pin (now and
   then to a project that does not provide it)."
  (gen/let [n (gen/choose 1 5)
            provs (gen/vector (gen/tuple (gen/elements caps) (gen/choose 0 4)
                                         (gen/frequency [[6 gen-version] [1 (gen/return nil)]]))
                              0 6)
            needs (gen/vector (gen/map (gen/elements caps)
                                       (gen/let [v gen-range
                                                 s (gen/frequency [[3 (gen/return true)] [1 (gen/return false)]])
                                                 pin (gen/frequency [[5 (gen/return nil)] [1 (gen/choose 0 4)]])]
                                         (cond-> {:version v :share? s}
                                           pin (assoc :provider (str "p" (mod pin n)))))
                                       {:max-elements 3})
                              n)]
    (let [ids (mapv #(str "p" %) (range n))
          provides (reduce (fn [acc [c i v]]
                             (assoc-in acc [(ids (mod i n)) c] (cond-> {:service c} v (assoc :version v))))
                           {} provs)]
      {:projects (into {} (map-indexed (fn [i id]
                                         [id (cond-> (project id :project/needs (needs i))
                                               (get provides id) (assoc :project/provides (get provides id)))])
                                       ids))
       :consumers ids})))

(def gen-friendly-world
  "Mostly solvable: every capability has one provider at major 3, ranges
   mostly accept major 3, a quarter of the needs opt out, no pins."
  (gen/let [n (gen/choose 2 5)
            homes (gen/vector (gen/choose 0 4) (count caps))
            needs (gen/vector (gen/map (gen/elements caps)
                                       (gen/hash-map :version (gen/frequency [[6 (gen/elements ["*" "3" "^3" ">=3 <4" "~3"])]
                                                                              [1 gen-range]])
                                                     :share? (gen/frequency [[3 (gen/return true)] [1 (gen/return false)]]))
                                       {:min-elements 1 :max-elements 3})
                              n)]
    (let [ids (mapv #(str "p" %) (range n))
          provides (reduce (fn [acc [c i]] (assoc-in acc [(ids (mod i n)) c] {:service c :version "3.2"}))
                           {} (map vector caps homes))]
      {:projects (into {} (map-indexed (fn [i id]
                                         [id (cond-> (project id :project/needs (needs i))
                                               (get provides id) (assoc :project/provides (get provides id)))])
                                       ids))
       :consumers ids})))

(def gen-any-world (gen/one-of [gen-world gen-friendly-world]))

(defn- world-needs [w]
  (:ok (cap/consumer-needs (:projects w) (:consumers w) {})))

(defn- resolve-world [w]
  (cap/resolve-needs (:projects w) (world-needs w) {:run "r1"}))

(defn- unit-solvable?
  "Brute force: does some single provider fit every need of `rows`?"
  [projects cap rows]
  (let [pins (set (keep :provider rows))]
    (and (<= (count pins) 1)
         (= 1 (count (for [[id prov] (cap/providers-of projects cap)
                           :when (or (empty? pins) (contains? pins id))
                           :when (every? #(cap/version-satisfies? (:version %) (:version prov)) rows)]
                       id))))))

(defn- solvable? [w]
  (let [rows (for [[c ns] (world-needs w) [k need] ns] (assoc need :cap k :consumer c))]
    (and (every? (fn [[k rs]] (unit-solvable? (:projects w) k rs))
                 (group-by :cap (filter #(:share? % true) rows)))
         (every? #(unit-solvable? (:projects w) (:cap %) [%]) (remove #(:share? % true) rows)))))

(defn- well-formed-error? [r]
  (let [es (:errors r)]
    (and (keyword? (:error r))
         (= "compose" (namespace (:error r)))
         (seq es)
         (= (dissoc r :errors) (first es))
         (every? (fn [e] (and (string? (:capability e)) (string? (:message e))
                              (seq (:consumers e)) (= (sort (:consumers e)) (:consumers e))))
                 es))))

(defspec error-exactly-when-some-need-has-no-single-fitting-provider 300
  (prop/for-all [w gen-any-world]
    (let [r (resolve-world w)]
      (if (contains? r :error)
        (and (well-formed-error? r) (not (solvable? w)))
        (solvable? w)))))

(defspec sharing-is-a-union 300
  (prop/for-all [w gen-any-world]
    (let [r (resolve-world w)
          {:resolution/keys [bindings instances]} (:ok r)]
      (or (well-formed-error? r)
          (every? (fn [[k kcs]]
                    (let [cs (map second kcs)
                          ids (set (map #(get-in bindings [% k]) cs))
                          inst (get instances (first ids))]
                      (and (= 1 (count ids))
                           (true? (:instance/share? inst))
                           (= (sort cs) (:instance/consumers inst)))))
                  (group-by first (for [[c ns] (world-needs w) [k need] ns :when (:share? need true)] [k c])))))))

(defspec opt-out-never-shares 300
  (prop/for-all [w gen-any-world]
    (let [r (resolve-world w)
          {:resolution/keys [bindings instances]} (:ok r)]
      (or (well-formed-error? r)
          (every? (fn [[c ns]]
                    (every? (fn [[k need]]
                              (or (:share? need true)
                                  (let [inst (get instances (get-in bindings [c k]))]
                                    (and (= [c] (:instance/consumers inst))
                                         (false? (:instance/share? inst))))))
                            ns))
                  (world-needs w))))))

(defspec every-binding-points-at-an-instance-serving-it 300
  (prop/for-all [w gen-any-world]
    (let [r (resolve-world w)
          {:resolution/keys [bindings instances]} (:ok r)
          ns (world-needs w)]
      (or (well-formed-error? r)
          (and (every? (fn [[c bs]]
                         (every? (fn [[k id]]
                                   (let [inst (get instances id)]
                                     (and (= k (:instance/capability inst))
                                          (some #{c} (:instance/consumers inst))
                                          (cap/version-satisfies? (get-in ns [c k :version])
                                                                  (:instance/version inst)))))
                                 bs))
                       bindings)
               (= (set (keys instances)) (set (mapcat vals (vals bindings)))))))))

(defspec resolution-is-order-independent 300
  (prop/for-all [[w projects' consumers']
                 (gen/let [w gen-any-world
                           ps (gen/shuffle (seq (:projects w)))
                           cs (gen/shuffle (:consumers w))]
                   [w (into {} ps) cs])]
    (= (resolve-world w)
       (resolve-world {:projects projects' :consumers consumers'}))))

(defspec inputs-and-resolutions-conform-to-the-schema 300
  (prop/for-all [w gen-any-world]
    (let [r (resolve-world w)]
      (and (every? #(nil? (schema/explain schema/Project %)) (vals (:projects w)))
           (or (contains? r :error)
               (nil? (schema/explain schema/Resolution (:ok r))))))))
