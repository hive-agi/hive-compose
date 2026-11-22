(ns hive-compose.promote.flaky
  "Flake history -> a verdict per test case. Pure: the history rows
   (CaseOutcome, see `hive-compose.schema`) and the thresholds arrive as
   values.

   A case is identified by [suite project case-id]; its rows are put in run
   order (by `:at`, then `:run/id`), one per run (a run recorded twice keeps
   one row), so the order the rows were appended in never matters. :error
   counts as a fail; :skipped runs are counted but judged as neither.

   The verdict looks at the last `:window` judged runs, first rule wins:
     :stable      no fail in the window
     :regression  no pass in the window, or it ends with `:streak` or more
                  consecutive fails, or with fails after passes and no flip
                  back (P..PF..F): failing since `:since`, the first run after
                  the last pass (its `:run/sha` names the commit when recorded)
     :fixed       `:streak` or more fails then `:streak` or more passes
                  closing the window, or fails then passes and no flip back
     :flaky       passes and fails interleaved (two flips or more) over
                  `:min-runs` runs or more, with a flip rate (status changes
                  between consecutive runs over runs - 1) of `:flip-rate` or
                  more
     :stable      otherwise (too few runs, or fails too rare to call)

   OOM-killed rows (`:case/oom? true`, see `hive-compose.promote.oom`) are
   never judged: they neither fail nor flip a case, so an OOM is never read
   as a flake. A case with an OOM row in its last `:window` runs and no real
   fail there is classed :oom; real fails beside it keep their own class.
   Either way the report counts them under :ooms with :last-oom.

   The score ranking cases is the window's flip rate, scaled down while the
   window holds fewer than `:min-runs` runs.

   Thresholds: `defaults`, overridden by `:compose/flaky` in the addon config."
  (:require [clojure.string :as str]))

;; SPDX-License-Identifier: MIT

(def defaults
  {:window 20
   :min-runs 3
   :flip-rate 0.1
   :streak 3})

(defn config
  "The thresholds: `defaults` with the known keys of `overrides` (nil or a
   map) over them."
  [overrides]
  (merge defaults (select-keys overrides (keys defaults))))

;; ---------------------------------------------------------------------------
;; run results -> rows

(defn- id-str [x]
  (cond (nil? x) nil
        (keyword? x) (name x)
        :else (str x)))

(defn run-outcomes
  "The CaseOutcome rows of run result `run`, recorded at `at` (epoch ms)
   unless the run carries its own `:run/at`.

   `run` is the run result hc-8's suite runner answers:
     {:run/id RunId :suite/id s :project p :exit n :run/sha sha?
      :report {:report/format :junit|:tap
               :report/cases [{:case/id \"<suite>/<classname>.<name>\"
                               :case/status :passed|:failed|:error|:skipped
                               :case/time-s seconds-or-nil ..} ..]}}
   Only the keys above are read. One row per case, plus one for the runner
   itself, case `<suite>/(run)`: :error when it exited non-zero with no case
   in its report (it died before writing it), else :passed. So a runner that
   dies shows in the history, and one that recovers reads :fixed instead of
   staying a regression. A run with no case and no exit code records nothing."
  [run at]
  (let [suite (id-str (:suite/id run))
        base (cond-> {:run/id (id-str (:run/id run))
                      :suite/id suite
                      :at (or (:run/at run) at)}
               (some? (:project run)) (assoc :project (id-str (:project run)))
               (some? (:run/sha run)) (assoc :run/sha (str (:run/sha run)))
               ;; hc-16b: a run OOM-killed at its cap marks every row it records
               (some? (:oom run)) (assoc :case/oom? true))
        cases (get-in run [:report :report/cases])
        exit (:exit run)
        died? (and (number? exit) (not (zero? exit)) (empty? cases))
        runner (assoc base :case/id (str suite "/(run)")
                      :case/status (if died? :error :passed))]
    (cond-> (mapv (fn [c]
                    (cond-> (assoc base
                                   :case/id (id-str (:case/id c))
                                   :case/status (keyword (:case/status c)))
                      (some? (:case/time-s c)) (assoc :case/time-s (:case/time-s c))))
                  cases)
      (or (seq cases) (number? exit)) (conj runner))))

;; ---------------------------------------------------------------------------
;; one case

(defn case-key
  "[suite project case-id] of a row."
  [row]
  [(:suite/id row) (:project row) (:case/id row)])

(def ^:private status-rank {:skipped 0 :passed 1 :failed 2 :error 3})

(defn- tie-break
  "A total order over the rows of one run, so which one is kept never depends
   on the order they arrived in."
  [row]
  [(status-rank (:case/status row) -1) (pr-str (into (sorted-map) row))])

(defn ordered
  "The rows of one case in run order, one per run. A run recorded more than
   once keeps its worst row (:error > :failed > :passed > :skipped) at the
   earliest time it was recorded, so recording a run again never moves it
   in run order."
  [rows]
  (->> (group-by :run/id rows)
       vals
       (map (fn [rs]
              (assoc (last (sort-by tie-break rs)) :at (apply min (map :at rs)))))
       (sort-by (juxt :at :run/id))))

(defn- verdict
  "The judged verdict of a row: :pass | :fail, nil for skipped rows and for
   OOM-killed ones (`:case/oom? true`, hc-16b): a run killed at its memory
   cap says nothing about the test, so it neither fails nor flips it."
  [row]
  (when-not (:case/oom? row)
    (case (:case/status row)
      :passed :pass
      (:failed :error) :fail
      nil)))

(defn- flips [vs]
  (count (filter (fn [[a b]] (not= a b)) (partition 2 1 vs))))

(defn- trailing [v vs]
  (count (take-while #{v} (reverse vs))))

(defn- ratio [n d]
  (if (pos? d)
    (/ (Math/round (* 1000.0 (/ (double n) d))) 1000.0)
    0.0))

(defn classify
  "The class of `w`, the verdicts (:pass | :fail, oldest first) of a window,
   under thresholds `cfg` (see the namespace doc)."
  [{:keys [min-runs flip-rate streak]} w]
  (let [w (vec w)
        n (count w)
        fails (count (filter #{:fail} w))
        flipped (flips w)
        t-pass (trailing :pass w)
        fails-before (trailing :fail (subvec w 0 (- n t-pass)))]
    (cond
      (zero? fails) :stable
      (= fails n) :regression
      (>= (trailing :fail w) streak) :regression
      (and (>= t-pass streak) (>= fails-before streak)) :fixed
      (= 1 flipped) (if (= :fail (peek w)) :regression :fixed)
      (and (>= n min-runs) (>= (ratio flipped (dec n)) flip-rate)) :flaky
      :else :stable)))

(def ^:private trend-char {:passed "P" :failed "F" :error "E"})

(defn- run-ref
  "{:run :at :status :sha?} of a row, for the report."
  [row]
  (when row
    (cond-> {:run (:run/id row) :at (:at row) :status (:case/status row)}
      (:run/sha row) (assoc :sha (:run/sha row)))))

(defn case-report
  "The verdict on one case from all its `rows`, under thresholds `cfg`."
  [cfg rows]
  (let [rs (ordered rows)
        judged (filterv verdict rs)
        vs (mapv verdict judged)
        win (vec (take-last (:window cfg) judged))
        wv (vec (take-last (:window cfg) vs))
        n (count wv)
        ooms (filterv :case/oom? rs)
        recent-oom? (and (seq ooms)
                         (some :case/oom? (take-last (:window cfg) rs))
                         (not-any? #{:fail} wv))
        class (if recent-oom? :oom (classify cfg wv))
        passes (count (filter #{:pass} vs))
        fails (- (count vs) passes)
        w-rate (ratio (flips wv) (dec n))
        confidence (min 1.0 (/ (double n) (max 1 (:min-runs cfg))))
        tail-of (fn [v] (nth judged (- (count judged) (trailing v vs)) nil))
        last-pass (last (filter (comp #{:passed} :case/status) judged))
        [suite project case-id] (case-key (first rs))]
    (cond-> {:suite suite
             :case case-id
             :class class
             :score (ratio (* w-rate confidence) 1)
             :runs (count rs)
             :passes passes
             :fails fails
             :skipped (- (count rs) (count judged))
             :flips (flips vs)
             :fail-rate (ratio fails (count vs))
             :window {:runs n
                      :fails (count (filter #{:fail} wv))
                      :flips (flips wv)
                      :flip-rate w-rate
                      :trend (str/join (map (comp trend-char :case/status) win))}
             :last (run-ref (last rs))}
      project (assoc :project project)
      (seq ooms) (assoc :ooms (count ooms) :last-oom (run-ref (peek ooms)))
      (= :regression class) (assoc :since (run-ref (tail-of :fail)))
      (and (= :regression class) last-pass) (assoc :last-pass (run-ref last-pass))
      (= :fixed class) (assoc :fixed-since (run-ref (tail-of :pass))))))

;; ---------------------------------------------------------------------------
;; every case

(defn matching
  "The rows of `rows` that `query` {:suite s :project p} selects; a nil or
   absent key selects any."
  [{:keys [suite project]} rows]
  (filter (fn [row]
            (and (or (nil? suite) (= suite (:suite/id row)))
                 (or (nil? project) (= project (:project row)))))
          rows))

(def ^:private class-order {:flaky 0 :regression 1 :oom 2 :fixed 3 :stable 4})

(defn rank
  "The verdict on every case of `rows` under thresholds `cfg`: :flaky cases
   first, then :regression, :fixed and :stable; within a class by score, then
   window fails, then fails, then case key."
  [cfg rows]
  (->> (group-by case-key rows)
       vals
       (map #(case-report cfg %))
       (sort-by (juxt (comp class-order :class)
                      (comp - :score)
                      (comp - :fails :window)
                      (comp - :fails)
                      (juxt :suite #(or (:project %) "") :case)))
       vec))
