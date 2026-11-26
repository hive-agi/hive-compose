(ns hive-compose.promote.sync-test
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.promote.sync :as sync]))

;; SPDX-License-Identifier: MIT

(defn- entry [touch & {:as claims}]
  (cond-> {:profile {:profile/id "p"} :started-at 0 :last-touch touch}
    (seq claims) (assoc :claims (into {} (map (fn [[k seen]] [k {:since 0 :seen seen}])) claims))))

(deftest one-side-changes-win
  (let [base {:active {"a" (entry 1)} :current "a"}]
    (testing "theirs added an entry"
      (is (= {"a" (entry 1) "b" (entry 2)}
             (:active (sync/merge-state base base (assoc-in base [:active "b"] (entry 2)))))))
    (testing "mine removed an entry, theirs left it alone"
      (is (= {} (:active (sync/merge-state base (update base :active dissoc "a") base)))))
    (testing "theirs removed an entry, mine left it alone"
      (is (= {} (:active (sync/merge-state base base (update base :active dissoc "a"))))))
    (testing ":current follows the side that changed it"
      (is (= "b" (:current (sync/merge-state base base (assoc base :current "b")))))
      (is (= "c" (:current (sync/merge-state base (assoc base :current "c") base)))))))

(deftest stack-runs-merge-like-entries-and-stay-optional
  (let [base {:active {} :current nil}]
    (is (not (contains? (sync/merge-state base base base) :stacks)) "an old state file gains no key")
    (is (= {"r1x" (entry 2)}
           (:stacks (sync/merge-state base base (assoc base :stacks {"r1x" (entry 2)}))))
        "theirs started a run")
    (let [b (assoc base :stacks {"r1x" (entry 1 "a" 1)})]
      (is (= #{"a" "b"}
             (set (keys (get-in (sync/merge-state b (assoc b :stacks {"r1x" (entry 4 "a" 4)})
                                                  (assoc b :stacks {"r1x" (entry 3 "a" 1 "b" 3)}))
                                [:stacks "r1x" :claims]))))
          "claims of a run from both sides survive")
      (is (= {} (:stacks (sync/merge-state b (assoc b :stacks {}) b))) "mine took the run down"))))

(deftest preexistence-marks-merge-by-service
  (let [b {:active {} :current nil :preexisting {"infra/kafka" #{"kafka"}}}]
    (is (= {"infra/kafka" #{"kafka"} "infra/pg" #{"db"}}
           (:preexisting (sync/merge-state b (dissoc b :preexisting)
                                           (assoc-in b [:preexisting "infra/pg"] #{"db"}))))
        "a side without the key never removes a mark")
    (is (= {"infra/pg" #{"db"}}
           (:preexisting (sync/merge-state b (assoc b :preexisting {})
                                           (assoc-in b [:preexisting "infra/pg"] #{"db"}))))
        "a reconciled removal sticks, an addition from the other side lands")
    (is (= {"infra/kafka" #{"kafka" "zookeeper"}}
           (:preexisting (sync/merge-state b (update-in b [:preexisting "infra/kafka"] conj "zookeeper") b))))))

(deftest both-sides-changed-an-entry
  (let [base {:active {"a" (entry 1 "x" 1)}}
        mine {:active {"a" (entry 5 "x" 5)}}
        theirs {:active {"a" (entry 3 "x" 1 "y" 3)}}
        a (get-in (sync/merge-state base mine theirs) [:active "a"])]
    (is (= 5 (:last-touch a)))
    (is (= #{"x" "y"} (set (keys (:claims a)))) "a claim from each side survives")
    (is (= 5 (get-in a [:claims "x" :seen])))))

(deftest removal-against-a-change-keeps-the-entry
  (let [base {:active {"a" (entry 1)}}
        claimed {:active {"a" (entry 4 "x" 4)}}]
    (is (= (entry 4 "x" 4) (get-in (sync/merge-state base {:active {}} claimed) [:active "a"])))
    (is (= (entry 4 "x" 4) (get-in (sync/merge-state base claimed {:active {}}) [:active "a"])))))

(deftest released-claim-stays-released
  (let [base {:active {"a" (entry 1 "x" 1)}}
        mine {:active {"a" (entry 2)}}
        theirs {:active {"a" (entry 3 "x" 1 "y" 3)}}]
    (is (= #{"y"} (set (keys (get-in (sync/merge-state base mine theirs) [:active "a" :claims])))))))
