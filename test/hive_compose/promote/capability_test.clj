(ns hive-compose.promote.capability-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.promote.capability :as cap]))

;; SPDX-License-Identifier: MIT

(defn- project [id & {:as m}]
  (merge {:project/id id :project/dir (str "/w/" id)} m))

(def projects
  {"bus" (project "bus" :project/provides {"kafka" {:service "kafka" :version "3.7"}})
   "db" (project "db" :project/provides {"pg" {:service "postgres" :version "16.2"}})
   "shop" (project "shop" :project/needs {"kafka" {:version "^3"} "pg" {:version ">=15"}})
   "crm" (project "crm" :project/needs {"kafka" {:version ">=3.5 <4"}})
   "iso" (project "iso" :project/needs {"kafka" {:share? false}})})

(defn- ok [r] (is (contains? r :ok) (pr-str r)) (:ok r))

(defn- are-ranges [cases]
  (doseq [[rng v expected] cases]
    (is (= expected (cap/version-satisfies? rng v)) (str (pr-str rng) " vs " v))))

;; ---------------------------------------------------------------------------
;; Versions and ranges

(deftest ranges-follow-the-grammar
  (testing "accepted"
    (are-ranges [["*" "0.1" true] ["*" "99" true]
                 ["3" "3.7.1" true] ["3" "4.0" false] ["3" "30" false]
                 ["3.7" "3.7.9" true] ["3.7" "3.8" false]
                 ["^3" "3.0" true] ["^3" "3.99.1" true] ["^3" "4" false] ["^3" "2.9" false]
                 ["^3.7" "3.6" false] ["^3.7" "3.9" true]
                 ["~3.7" "3.7.4" true] ["~3.7" "3.8" false] ["~3" "3.9" true]
                 [">=3.5 <4" "3.5" true] [">=3.5 <4" "3.4.9" false] [">=3.5 <4" "4.0" false]
                 [">3" "3.0.0" false] [">3" "3.0.1" true] ["<=3.7" "3.7" true] ["=3.7" "3.7.0" true]
                 ["3.7" "3.7.1-alpine" true] [nil "1" true]]))
  (testing "invalid ranges and versions satisfy nothing"
    (doseq [rng ["" "x" "3 4" "^" ">=" "3.x"]]
      (is (= :compose/invalid-range (:error (cap/parse-range rng))) rng))
    (is (= :compose/invalid-version (:error (cap/parse-version "latest"))))
    (is (false? (cap/version-satisfies? "*" "latest"))))
  (testing "an unversioned provider satisfies only *"
    (is (true? (cap/version-satisfies? "*" nil)))
    (is (false? (cap/version-satisfies? "3" nil)))))

(deftest versions-compare-numerically-with-missing-parts-as-zero
  (is (zero? (cap/compare-versions [3] [3 0 0])))
  (is (neg? (cap/compare-versions [3 9] [3 10])))
  (is (= [3 7 1] (:ok (cap/parse-version "3.7.1-alpine")))))

;; ---------------------------------------------------------------------------
;; Resolution examples

(deftest compatible-consumers-share-one-instance
  (let [res (ok (cap/resolve-needs projects (cap/consumer-needs projects ["shop" "crm"] {})))]
    (is (= {"crm" {"kafka" "bus/kafka"}
            "shop" {"kafka" "bus/kafka" "pg" "db/pg"}}
           (:resolution/bindings res)))
    (is (= {:instance/id "bus/kafka" :instance/capability "kafka" :instance/provider "bus"
            :instance/service "kafka" :instance/version "3.7" :instance/share? true
            :instance/consumers ["crm" "shop"] :instance/aliases ["kafka"]}
           (get-in res [:resolution/instances "bus/kafka"])))))

(deftest opt-out-gets-a-private-instance
  (let [res (ok (cap/resolve-needs projects (cap/consumer-needs projects ["shop" "iso/web"] {})))]
    (is (= "bus/kafka@iso/web" (get-in res [:resolution/bindings "iso/web" "kafka"]))
        "a section inherits its project's needs")
    (is (= ["iso/web"] (get-in res [:resolution/instances "bus/kafka@iso/web" :instance/consumers])))
    (is (false? (get-in res [:resolution/instances "bus/kafka@iso/web" :instance/share?])))
    (is (= ["shop"] (get-in res [:resolution/instances "bus/kafka" :instance/consumers])))))

(deftest extra-needs-and-stacks
  (testing "extra needs override the project's and add consumers"
    (let [res (ok (cap/resolve-needs projects (cap/consumer-needs projects ["crm"]
                                                                   {"crm" {"kafka" {:share? false}}
                                                                    "suite:smoke" {"pg" {}}})))]
      (is (= {"crm" {"kafka" "bus/kafka@crm"} "suite:smoke" {"pg" "db/pg"}}
             (:resolution/bindings res)))))
  (testing "a stack's own needs bind to stack:<id>"
    (let [res (ok (cap/resolve-stack projects {:stack/id "e2e" :stack/consumers ["crm"]
                                               :stack/needs {"pg" {}}}))]
      (is (= {"crm" {"kafka" "bus/kafka"} "stack:e2e" {"pg" "db/pg"}}
             (:resolution/bindings res)))))
  (testing "a consumer without needs binds nothing"
    (is (= {"bus" {}} (:resolution/bindings (ok (cap/resolve-stack projects {:stack/id "s" :stack/consumers ["bus"]})))))))

(deftest missing-provider-names-consumers
  (let [r (cap/resolve-needs projects {"a" {"redis" {}} "b" {"redis" {:version "7"}}})]
    (is (= :compose/no-provider (:error r)))
    (is (= "redis" (:capability r)))
    (is (= ["a" "b"] (:consumers r)))
    (is (= {"a" "*" "b" "7"} (:ranges r))))
  (testing "a pin naming a project that does not provide it"
    (let [r (cap/resolve-needs projects {"a" {"kafka" {:provider "db"}}})]
      (is (= :compose/no-provider (:error r)))
      (is (= "db" (:provider r))))))

(deftest incompatible-version-names-consumers-and-providers
  (let [r (cap/resolve-needs projects {"a" {"kafka" {:version "^2"}}
                                       "b" {"kafka" {:version "4"}}
                                       "c" {"kafka" {:version "^3"}}})]
    (is (= :compose/incompatible-version (:error r)))
    (is (= "kafka" (:capability r)))
    (is (= ["a" "b"] (:consumers r)) "c is compatible and not named")
    (is (= {"bus" "3.7"} (:providers r)))
    (is (string? (:message r)))))

(deftest ambiguous-provider-needs-a-pin
  (let [ps (assoc projects "bus2" (project "bus2" :project/provides {"kafka" {:service "broker" :version "3.6"}}))]
    (let [r (cap/resolve-needs ps {"a" {"kafka" {:version "^3"}}})]
      (is (= :compose/ambiguous-provider (:error r)))
      (is (= ["bus" "bus2"] (:providers r)))
      (is (= ["a"] (:consumers r))))
    (testing "a pin resolves it"
      (is (= {"a" {"kafka" "bus2/kafka"}}
             (:resolution/bindings (ok (cap/resolve-needs ps {"a" {"kafka" {:provider "bus2"}}}))))))
    (testing "a range only one provider fits is not ambiguous"
      (is (= {"a" {"kafka" "bus2/kafka"}}
             (:resolution/bindings (ok (cap/resolve-needs ps {"a" {"kafka" {:version "3.6"}}}))))))))

(deftest invalid-range-is-an-error
  (is (= :compose/invalid-range (:error (cap/resolve-needs projects {"a" {"kafka" {:version "three"}}})))))

(deftest every-error-group-is-reported
  (let [r (cap/resolve-needs projects {"a" {"redis" {}} "b" {"kafka" {:version "^9"}}})]
    (is (= [:compose/no-provider :compose/incompatible-version] (mapv :error (:errors r))))))

;; ---------------------------------------------------------------------------
;; Properties

(def gen-version
  (gen/fmap (fn [[a b]] (str a "." b)) (gen/tuple (gen/choose 1 4) (gen/choose 0 9))))

(def gen-range
  (gen/one-of [(gen/return "*")
               (gen/fmap str (gen/choose 1 4))
               (gen/fmap #(str "^" %) (gen/choose 1 4))
               (gen/fmap (fn [[a b]] (str "~" a "." b)) (gen/tuple (gen/choose 1 4) (gen/choose 0 9)))
               (gen/fmap (fn [[a b]] (str ">=" a "." b " <" (inc a))) (gen/tuple (gen/choose 1 4) (gen/choose 0 9)))]))

(def caps ["kafka" "pg" "redis"])

(def gen-world
  "Projects p0..pN. Every capability has one provider at one version (so only
   versions can fail); each project needs some capabilities, mostly with a
   range that fits the provider, sometimes with any range."
  (gen/let [n (gen/choose 1 5)
            versions (apply gen/hash-map (mapcat (fn [c] [c gen-version]) caps))
            homes (apply gen/hash-map (mapcat (fn [c] [c (gen/choose 0 (dec n))]) caps))
            needs (gen/vector
                   (gen/map (gen/elements caps)
                            (gen/hash-map :share? (gen/frequency [[3 (gen/return true)] [1 (gen/return false)]]))
                            {:max-elements 3})
                   n)
            ranges (gen/vector
                    (apply gen/hash-map
                           (mapcat (fn [c]
                                     (let [major (first (str/split (versions c) #"\."))]
                                       [c (gen/frequency [[3 (gen/elements ["*" major (str "^" major)
                                                                            (str ">=" major " <" (inc (parse-long major)))])]
                                                          [1 gen-range]])]))
                                   caps))
                    n)]
    (let [ids (mapv #(str "p" %) (range n))
          provides (reduce (fn [acc c] (assoc-in acc [(ids (homes c)) c] {:service c :version (versions c)}))
                           {} caps)]
      {:projects (into {} (map-indexed
                           (fn [i id]
                             (let [ns (into {} (map (fn [[c need]] [c (assoc need :version (get-in ranges [i c]))]))
                                            (needs i))]
                               [id (cond-> (project id :project/needs ns)
                                     (get provides id) (assoc :project/provides (get provides id)))]))
                           ids))
       :consumers ids})))

(defn- resolve-world [{:keys [projects consumers]}]
  (cap/resolve-needs projects (cap/consumer-needs projects consumers {})))

(defspec sharing-is-a-union 200
  (prop/for-all [w gen-world]
    (let [r (resolve-world w)]
      (or (contains? r :error)
          (let [{:resolution/keys [bindings instances]} (:ok r)
                sharers (group-by first
                                  (for [c (:consumers w)
                                        [k need] (get-in w [:projects c :project/needs])
                                        :when (:share? need true)]
                                    [k c]))]
            ;; per capability: one instance id for every sharing consumer, and
            ;; that shared instance serves exactly them
            (every? (fn [[k kcs]]
                      (let [cs (map second kcs)
                            ids (set (map #(get-in bindings [% k]) cs))
                            inst (get instances (first ids))]
                        (and (= 1 (count ids))
                             (true? (:instance/share? inst))
                             (= (sort cs) (:instance/consumers inst)))))
                    sharers))))))

(defspec opt-out-never-shares 200
  (prop/for-all [w gen-world]
    (let [r (resolve-world w)]
      (or (contains? r :error)
          (let [{:resolution/keys [bindings instances]} (:ok r)]
            (every? (fn [[c ns]]
                      (every? (fn [[k need]]
                                (or (:share? need true)
                                    (let [inst (get instances (get-in bindings [c k]))]
                                      (and (= [c] (:instance/consumers inst))
                                           (false? (:instance/share? inst))))))
                              ns))
                    (into {} (map (fn [c] [c (get-in w [:projects c :project/needs])])) (:consumers w))))))))

(defspec every-binding-points-at-an-instance-serving-it 200
  (prop/for-all [w gen-world]
    (let [r (resolve-world w)]
      (or (contains? r :error)
          (let [{:resolution/keys [bindings instances]} (:ok r)]
            (and (every? (fn [[c ns]]
                           (every? (fn [[k id]]
                                     (let [inst (get instances id)]
                                       (and (= k (:instance/capability inst))
                                            (some #{c} (:instance/consumers inst))
                                            (cap/version-satisfies?
                                             (get-in w [:projects c :project/needs k :version])
                                             (:instance/version inst)))))
                                   ns))
                         bindings)
                 (= (set (keys instances)) (set (mapcat vals (vals bindings))))))))))

(defspec resolution-is-order-independent 200
  (prop/for-all [w gen-world
                 seed gen/nat]
    (let [rnd (java.util.Random. seed)
          shuffle* (fn [xs] (let [l (java.util.ArrayList. ^java.util.Collection xs)]
                              (java.util.Collections/shuffle l rnd)
                              (vec l)))
          projects' (into {} (shuffle* (seq (:projects w))))
          consumers' (shuffle* (:consumers w))]
      (= (resolve-world w)
         (resolve-world {:projects projects' :consumers consumers'})))))
