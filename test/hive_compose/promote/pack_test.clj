(ns hive-compose.promote.pack-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.promote.oom :as oom]
            [hive-compose.promote.pack :as pack]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def ^:private g (oom/mem-bytes "1g"))

(defn- cell [s p mem] {:suite/id s :project p :mem mem})

(defn- keys-of [waves] (mapv #(mapv pack/cell-key %) waves))

(deftest packs-into-the-budget-heaviest-first
  (let [cells [(cell "a" "x" g) (cell "b" "x" (* 2 g)) (cell "c" "x" g) (cell "d" "x" (* 3 g))]
        res (pack/pack cells {:budget (* 4 g)})]
    (is (r/ok? res))
    (testing "first-fit decreasing: 3g+1g, then 2g+1g"
      (is (= [[["d" "x"] ["a" "x"]] [["b" "x"] ["c" "x"]]] (keys-of (:waves (:ok res))))))
    (is (<= (pack/peak (:ok res)) (* 4 g)))))

(deftest the-stack-is-paid-first
  (let [cells [(cell "a" "x" g) (cell "b" "x" g)]]
    (is (= 1 (count (:waves (:ok (pack/pack cells {:budget (* 3 g)}))))))
    (is (= 2 (count (:waves (:ok (pack/pack cells {:budget (* 3 g) :stack-mem (* 2 g)}))))))))

(deftest an-uncapped-cell-weighs-the-default
  (let [res (:ok (pack/pack [(cell "a" "x" nil) (cell "b" "x" nil)]
                            {:budget g :default-mem (oom/mem-bytes "512m")}))]
    (is (= 1 (count (:waves res))))
    (is (= {["a" "x"] (oom/mem-bytes "512m") ["b" "x"] (oom/mem-bytes "512m")} (:weights res)))))

(deftest max-parallel-bounds-a-wave
  (is (= [2 2 1] (mapv count (:waves (:ok (pack/pack (for [s "abcde"] (cell (str s) "x" 1))
                                                     {:budget (* 10 g) :max-parallel 2})))))))

(deftest no-budget-is-one-wave
  (is (= 1 (count (:waves (:ok (pack/pack [(cell "a" "x" (* 64 g)) (cell "b" "x" g)] {})))))))

(deftest a-cell-over-the-budget-is-refused-by-name
  (let [res (pack/pack [(cell "big" "orders" (* 5 g)) (cell "a" "x" g)]
                       {:budget (* 4 g) :stack-mem g})]
    (is (= :compose/over-budget (:error res)))
    (is (= :budget (:basis res)))
    (is (= :matrix (:scope res)))
    (is (= (* 4 g) (:limit res)))
    (is (= (* 6 g) (:total-mem res)))
    (is (= (* 2 g) (:over res)))
    (is (= [{:instance "big/orders" :mem (* 5 g)}] (:instances res)))
    (is (re-find #"big/orders needs 5g" (:message res)))))

(def ^:private gen-cells
  (gen/fmap (fn [ms] (vec (map-indexed (fn [i m] (cell (str "s" i) "p" m)) ms)))
            (gen/vector (gen/choose 1 1000) 0 20)))

(defspec every-wave-fits-and-every-cell-runs-once 200
  (prop/for-all [cells gen-cells
                 budget (gen/choose 1000 5000)
                 stack (gen/choose 0 500)]
    (let [res (pack/pack cells {:budget budget :stack-mem stack})]
      (if (some #(> (:mem %) (- budget stack)) cells)
        (= :compose/over-budget (:error res))
        (let [{:keys [waves] :as p} (:ok res)]
          (and (<= (pack/peak p) budget)
               (= (sort (map pack/cell-key cells)) (sort (map pack/cell-key (apply concat waves))))))))))

(defspec order-of-cells-does-not-matter 100
  (prop/for-all [[cells shuffled] (gen/bind gen-cells
                                            (fn [cs] (gen/fmap (fn [s] [cs s]) (gen/shuffle cs))))]
    (= (pack/pack cells {:budget 3000})
       (pack/pack shuffled {:budget 3000}))))
