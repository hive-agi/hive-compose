(ns hive-compose.promote.flaky-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.promote.flaky :as flaky]
            [hive-compose.schema :as schema]))

;; SPDX-License-Identifier: MIT

(def cfg (flaky/config nil))

(def status-of {\P :passed \F :failed \E :error \S :skipped})

(defn- rows
  "History rows of case `case-id` from a trend string like \"PPFPF\", one run
   per character, run i at time 1000*i."
  ([trend] (rows "c1" trend))
  ([case-id trend]
   (vec (map-indexed (fn [i ch]
                       {:run/id (format "run-%04d" i)
                        :suite/id "s"
                        :project "p"
                        :case/id (str "s/" case-id)
                        :case/status (status-of ch)
                        :at (* 1000 (inc i))})
                     trend))))

(defn- class-of [trend]
  (:class (flaky/case-report cfg (rows trend))))

(deftest defaults-are-additive
  (is (= {:window 20 :min-runs 3 :flip-rate 0.1 :streak 3} (flaky/config nil)))
  (is (= 5 (:window (flaky/config {:window 5 :bogus 1}))))
  (is (not (contains? (flaky/config {:bogus 1}) :bogus))))

(deftest classes-of-typical-histories
  (is (= :stable (class-of "PPPPPP")))
  (is (= :stable (class-of "P")))
  (is (= :flaky (class-of "PFPFPF")))
  (is (= :flaky (class-of "PPFPPPFPPF")))
  (is (= :regression (class-of "PPPPFFF")))
  (is (= :regression (class-of "FFFF")) "never passed")
  (is (= :regression (class-of "PPPPPF")) "fails since a run, no flip back")
  (is (= :fixed (class-of "PPFFFPPP")))
  (is (= :fixed (class-of "FFP")) "failed, then passed, no flip back")
  (is (= :regression (class-of "PPFPPFFF")) "a streak of :streak fails after flapping is a regression")
  (is (= :flaky (class-of "PPFPPFF")) "a shorter streak after flapping is still a flake"))

(deftest error-counts-as-fail-and-skipped-is-not-judged
  (is (= :regression (class-of "PPPEEE")))
  (let [r (flaky/case-report cfg (rows "PSPSP"))]
    (is (= :stable (:class r)))
    (is (= 5 (:runs r)))
    (is (= 2 (:skipped r)))
    (is (= "PPP" (get-in r [:window :trend])))))

(deftest a-regression-names-the-first-failing-run-and-its-sha
  (let [rs (-> (rows "PPPFFF")
               (assoc-in [2 :run/sha] "aaa111")
               (assoc-in [3 :run/sha] "bbb222"))
        r (flaky/case-report cfg rs)]
    (is (= :regression (:class r)))
    (is (= {:run "run-0003" :at 4000 :status :failed :sha "bbb222"} (:since r)))
    (is (= "aaa111" (get-in r [:last-pass :sha])))
    (is (= 3 (:passes r)))
    (is (= 3 (:fails r)))
    (is (= 0.5 (:fail-rate r)))
    (is (= 1 (:flips r)))))

(deftest the-window-forgets-old-flapping
  (testing "flaps outside the last :window runs no longer count"
    (let [old-flaps "PFPFPFPF"
          r (flaky/case-report (flaky/config {:window 5}) (rows (str old-flaps "PPPPP")))]
      (is (= :stable (:class r)))
      (is (= 8 (:flips r)) "lifetime flips stay in the report")
      (is (= 0 (get-in r [:window :flips]))))))

(deftest a-run-recorded-twice-counts-once
  (let [rs (rows "PFP")
        r (flaky/case-report cfg (into rs rs))]
    (is (= 3 (:runs r)))))

(deftest rank-puts-flaky-first-and-filters-by-suite-and-project
  (let [all (concat (rows "steady" "PPPPPP")
                    (rows "flap" "PFPFPF")
                    (rows "rare" "PPPPPPPPFP")
                    (rows "broken" "PPPFFF")
                    [{:run/id "other-1" :suite/id "t" :project "q" :case/id "t/x"
                      :case/status :failed :at 1}])
        ranked (flaky/rank cfg all)]
    (is (= ["s/flap" "s/rare" "s/broken" "t/x" "s/steady"] (mapv :case ranked))
        "flaky, then regressions (t/x never passed), then stable")
    (is (= [:flaky :flaky :regression :regression :stable] (mapv :class ranked)))
    (is (> (:score (first ranked)) (:score (second ranked))))
    (is (= 1 (count (flaky/rank cfg (flaky/matching {:suite "t"} all)))))
    (is (= 4 (count (flaky/rank cfg (flaky/matching {:project "p"} all)))))))

(deftest run-outcomes-reads-the-hc8-run-result
  (let [run {:run/id "r-001" :suite/id :api :project "shop" :exit 1 :run/sha "deadbeef"
             :report {:report/format :junit
                      :report/cases [{:case/id "api/Login.ok" :case/name "ok" :case/classname "Login"
                                      :case/status :passed :case/time-s 0.25}
                                     {:case/id "api/Login.bad" :case/status "failed"
                                      :case/time-s nil :case/message "boom"}]
                      :report/totals {:passed 1 :failed 1}}}
        out (flaky/run-outcomes run 42)]
    (is (= [{:run/id "r-001" :suite/id "api" :project "shop" :run/sha "deadbeef" :at 42
             :case/id "api/Login.ok" :case/status :passed :case/time-s 0.25}
            {:run/id "r-001" :suite/id "api" :project "shop" :run/sha "deadbeef" :at 42
             :case/id "api/Login.bad" :case/status :failed}]
           out))
    (is (every? #(nil? (schema/explain schema/CaseOutcome %)) out)))
  (testing "a runner dying before its report is one :error row"
    (is (= [{:run/id "r-002" :suite/id "api" :at 7 :case/id "api/(run)" :case/status :error}]
           (flaky/run-outcomes {:run/id "r-002" :suite/id "api" :exit 137 :report nil} 7))))
  (testing "a clean run with no case records nothing"
    (is (= [] (flaky/run-outcomes {:run/id "r-003" :suite/id "api" :exit 0} 7)))))

;; ---------------------------------------------------------------------------
;; properties

(def gen-status (gen/elements [:passed :failed :error :skipped]))

(def gen-history
  "Rows of up to 4 cases over up to 30 runs with distinct run ids and
   timestamps (some runs share a timestamp; run id breaks the tie)."
  (gen/let [n (gen/choose 1 30)
            cases (gen/vector (gen/elements ["a" "b" "c" "d"]) 1 4)
            ats (gen/vector (gen/choose 0 50) n)
            statuses (gen/vector (gen/vector gen-status (count cases)) n)]
    (vec (for [i (range n)
               [j c] (map-indexed vector (distinct cases))]
           {:run/id (format "run-%04d" i)
            :suite/id "s"
            :case/id c
            :case/status (get-in statuses [i j] :passed)
            :at (nth ats i)}))))

(defspec append-order-never-changes-the-verdict 200
  (prop/for-all [hist gen-history
                 seed gen/int]
    (let [shuffled (let [r (java.util.Random. seed)
                         l (java.util.ArrayList. ^java.util.Collection hist)]
                     (java.util.Collections/shuffle l r)
                     (vec l))]
      (= (flaky/rank cfg hist) (flaky/rank cfg shuffled)))))

(defspec a-case-that-always-passes-is-stable 100
  (prop/for-all [n (gen/choose 1 60)
                 window (gen/choose 2 30)]
    (= :stable (:class (flaky/case-report (flaky/config {:window window})
                                          (rows (apply str (repeat n "P"))))))))

(defspec alternating-pass-fail-is-flaky 100
  (prop/for-all [n (gen/choose 4 60)
                 start (gen/elements [\P \F])]
    (let [other ({\P \F \F \P} start)
          trend (apply str (take n (cycle [start other])))]
      (= :flaky (class-of trend)))))

(defspec passes-then-a-streak-of-fails-is-a-regression 100
  (prop/for-all [passes (gen/choose 1 30)
                 fails (gen/choose (:streak cfg) 30)]
    (let [r (flaky/case-report cfg (rows (str (apply str (repeat passes "P"))
                                              (apply str (repeat fails "F")))))]
      (and (= :regression (:class r))
           (= (format "run-%04d" passes) (get-in r [:since :run]))))))
