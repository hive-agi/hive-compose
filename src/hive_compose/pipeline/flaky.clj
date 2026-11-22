(ns hive-compose.pipeline.flaky
  "Flake history: record a suite run's case outcomes, rank the cases by how
   intermittently they fail.

   ctx keys read: `:history` (an IFlakeHistory), `:now` (0-arity epoch ms)
   and `[:settings :compose/flaky]` (thresholds, see
   `hive-compose.promote.flaky`).

   Integration point for hc-8 (suites): once a runner's report is parsed,
   hand the run result {:run/id :suite/id :project :report Report :exit n}
   to `record-run!`; nothing else is needed for `compose flaky` to rank it."
  (:require [hive-compose.port :as port]
            [hive-compose.promote.flaky :as flaky]
            [hive-compose.schema :as schema]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- no-history []
  (r/err :compose/no-history {:hint "the addon was built without a flake history (:compose/history)"}))

(defn record-run!
  "Append the case outcomes of run result `run` (see `flaky/run-outcomes`)
   to the history. Every row is checked against CaseOutcome first; one that
   does not conform refuses the whole run, naming the problems, and nothing
   is written. Result<{:run :suite :project :appended n}>."
  [{:keys [history now]} run]
  (if (nil? history)
    (no-history)
    (let [rows (flaky/run-outcomes run (now))
          problems (keep-indexed (fn [i row]
                                   (when-let [p (schema/explain schema/CaseOutcome row)]
                                     {:case (:case/id row) :index i :problems p}))
                                 rows)]
      (if (seq problems)
        (r/err :compose/invalid-run {:run (:run/id run) :invalid (vec (take 10 problems))})
        (r/let-ok [res (port/-append! history rows)]
          (r/ok (cond-> {:run (:run/id run) :suite (some-> (:suite/id run) name)
                         :appended (:appended res)}
                  (:project run) (assoc :project (:project run)))))))))

(defn flaky
  "Result<{:cases [CaseReport] :total n :classes {class n} ..}>: every case
   the history holds for `query` {:suite :project}, ranked (see
   `flaky/rank`), the first `limit` of them (all when nil)."
  [{:keys [history settings]} query limit]
  (if (nil? history)
    (no-history)
    (r/let-ok [rows (port/-history history query)]
      (let [cfg (flaky/config (:compose/flaky settings))
            ranked (flaky/rank cfg rows)
            skipped (:skipped (meta rows))]
        (r/ok (cond-> {:cases (if limit (vec (take limit ranked)) ranked)
                       :total (count ranked)
                       :classes (frequencies (map :class ranked))
                       :config cfg}
                (some? (:suite query)) (assoc :suite (:suite query))
                (some? (:project query)) (assoc :project (:project query))
                (and skipped (pos? skipped)) (assoc :unreadable-lines skipped)
                (empty? ranked) (assoc :note "no run recorded for this suite and project yet")))))))
