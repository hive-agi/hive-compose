(ns hive-compose.port-test
  "Static guard over the ports: every protocol method of `hive-compose.port`
   is one var of that namespace, so two protocols declaring a method of the
   same name silently share it and the later defprotocol wins. Each protocol
   map keeps its own :sigs, so the clash shows there even after the var was
   overwritten."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.port]))

;; SPDX-License-Identifier: MIT

(defn- protocols
  "{protocol-name protocol-map} of every protocol `ns-sym` defines."
  [ns-sym]
  (into (sorted-map)
        (keep (fn [[sym v]]
                (let [p (when (var? v) @v)]
                  (when (and (map? p) (:on-interface p) (contains? p :sigs))
                    [sym p]))))
        (ns-publics ns-sym)))

(defn- method-owners
  "{method-name [protocol-name ...]}: which protocols declare each method."
  [protos]
  (reduce-kv (fn [acc pname p]
               (reduce (fn [acc m] (update acc (keyword (name m)) (fnil conj []) pname))
                       acc
                       (map :name (vals (:sigs p)))))
             {}
             protos))

(deftest protocol-method-names-are-unique
  (let [protos (protocols 'hive-compose.port)
        clashes (into (sorted-map) (filter (fn [[_ owners]] (< 1 (count owners))))
                      (method-owners protos))]
    (testing "the guard sees the ports it guards"
      (is (< 5 (count protos)))
      (is (contains? protos 'ISuiteRunner))
      (is (contains? protos 'ICommandRunner)))
    (testing "no method name is declared by two protocols"
      (is (empty? clashes)
          (str "protocol methods shared across protocols (rename one): " clashes)))))

(deftest the-guard-catches-a-clash
  (testing "two protocol maps declaring the same method are reported"
    (let [protos {'IA {:on-interface 'IA :sigs {:-go! {:name '-go! :arglists '([a])}}}
                  'IB {:on-interface 'IB :sigs {:-go! {:name '-go! :arglists '([b x])}
                                                :-other {:name '-other :arglists '([b])}}}}]
      (is (= {:-go! ['IA 'IB] :-other ['IB]} (method-owners protos))))))
