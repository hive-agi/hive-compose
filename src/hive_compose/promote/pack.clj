(ns hive-compose.promote.pack
  "Pure: the suite x project matrix packed into a memory budget (hc-16b),
   instead of run at a fixed concurrency.

   Each cell weighs its runner's memory cap. `pack` answers waves: the cells
   of one wave run together, and the caps of one wave sum to at most the
   budget left once the stack itself (its capped instances) is paid for.
   Cells go heaviest first, each into the first wave it fits (first-fit
   decreasing), so the number of waves stays near the minimum. Ties break on
   the cell key, so the answer never depends on the order the cells came in.

   A cell with no cap weighs `:default-mem`. A cell heavier than the whole
   budget can never run; `pack` refuses the matrix and names it rather than
   run it over the budget. With no budget at all there is one wave of every
   cell, capped by `:max-parallel` when it is given.

   No IO: the budget, the stack's capped total and the caps arrive as bytes."
  (:require [hive-compose.promote.oom :as oom]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn cell-key
  "[suite project] of a cell."
  [cell]
  [(str (:suite/id cell)) (str (:project cell))])

(defn- chunks [n xs]
  (if (and n (pos? n)) (mapv vec (partition-all n xs)) [(vec xs)]))

(defn pack
  "Result<{:waves [[cell ..] ..] :budget/limit bytes-or-nil :budget/stack bytes
   :budget/cells bytes :weights {[suite project] bytes}}> of `cells` (each
   {:suite/id :project :mem bytes-or-nil}).

   `opts`:
     :budget        bytes the matrix may hold at once, stack included (nil:
                    no budget)
     :stack-mem     the stack's capped memory, paid before any cell (0)
     :default-mem   the weight of a cell with no cap (512m)
     :max-parallel  at most this many cells in one wave (nil: no limit)

   Errors: :compose/over-budget (hc-16a OverBudget, :basis :budget, plus
   :scope :matrix) {:limit :total-mem :over
   :instances [{:instance test-id :mem bytes}] :message} when the stack alone,
   or the stack plus one cell, exceeds the budget."
  [cells {:keys [budget stack-mem default-mem max-parallel]}]
  (let [stack (long (or stack-mem 0))
        dflt (long (or default-mem (oom/mem-bytes "512m")))
        weight #(long (or (:mem %) dflt))
        sorted (sort-by (juxt (comp - weight) cell-key) cells)
        weights (into (sorted-map) (map (juxt cell-key weight)) cells)
        base {:budget/limit budget :budget/stack stack :weights weights
              :budget/cells (reduce + 0 (vals weights))}]
    (if (nil? budget)
      (r/ok (assoc base :waves (chunks max-parallel sorted)))
      (let [room (- (long budget) stack)
            too-big (filterv #(> (weight %) room) sorted)]
        (if (seq too-big)
          (let [worst (first too-big)
                total (+ stack (weight worst))]
            (r/err :compose/over-budget
                   {:basis :budget
                    :scope :matrix
                    :limit (long budget)
                    :total-mem total
                    :over (- total (long budget))
                    :instances (mapv (fn [c] {:instance (str (:suite/id c) "/" (:project c))
                                              :mem (weight c)})
                                     too-big)
                    :message (str (count too-big) " suite run(s) cannot fit the budget of "
                                  (oom/mem-str budget)
                                  (when (pos? stack) (str " beside the stack's " (oom/mem-str stack)))
                                  ": " (:suite/id worst) "/" (:project worst)
                                  " needs " (oom/mem-str (weight worst)))}))
          (let [waves (reduce (fn [ws c]
                                (let [w (weight c)
                                      i (first (keep-indexed
                                                (fn [i wave]
                                                  (when (and (<= (+ w (:used wave)) room)
                                                             (or (nil? max-parallel)
                                                                 (< (count (:cells wave)) max-parallel)))
                                                    i))
                                                ws))]
                                  (if i
                                    (-> ws (update-in [i :cells] conj c) (update-in [i :used] + w))
                                    (conj ws {:cells [c] :used w}))))
                              []
                              sorted)]
            (r/ok (assoc base :waves (mapv :cells waves)))))))))

(defn peak
  "The most memory any wave of `packed` holds, the stack included."
  [{:keys [waves weights] :budget/keys [stack]}]
  (+ (long (or stack 0))
     (reduce max 0 (map (fn [w] (reduce + 0 (map #(get weights (cell-key %) 0) w))) waves))))
