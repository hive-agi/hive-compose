(ns hive-compose.pipeline.data
  "Runs the data lifecycle (hc-9) of a recorded stack run: the steps
   `hive-compose.promote.data/lifecycle-plan` renders for a phase, in order,
   through the ports. An :exec step goes to the engine's `port/IDataExec`
   (docker exec into the instance's running container), a :host step to the
   runner's `port/ICommandRunner` in the provider project's dir, a :state
   step (a prefix rotation) only advances the recorded state.

   The data state of a run (generations per instance and consumer) lives under
   `[:stacks run :data]` in the state, beside the run's plan, and is committed
   only when every step of the phase succeeded: a failed phase run again
   renders the same steps (template databases tolerate `already exists`).
   Volumes are never removed: the plan refuses any argv that would.

   hc8 suites call `run-phase!` [ctx run phase] around and between suites."
  (:require [clojure.string :as str]
            [hive-compose.port :as port]
            [hive-compose.promote.data :as data]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- persist! [{:keys [state save!]}]
  (when save! (save! (select-keys @state [:active :current :stacks]))))

(defn- run-entry [ctx run]
  (if-let [e (get-in @(:state ctx) [:stacks run])]
    (r/ok e)
    (r/err :compose/unknown-run {:run run :known (vec (sort (keys (:stacks @(:state ctx)))))})))

(defn data-state
  "The data state recorded for `run` ({} before any phase ran)."
  [ctx run]
  (get-in @(:state ctx) [:stacks run :data] {}))

(defn current-prefix
  "The tenancy prefix `consumer` uses on instance `iid` in `run` now."
  [ctx run iid consumer]
  (data/rotated-prefix run consumer (data/generation (data-state ctx run) iid consumer)))

(defn plan-phase
  "Result<{:phase :run :steps :state}>: the steps `phase` would run for `run`
   now. Runs nothing."
  [ctx run phase]
  (r/let-ok [e (run-entry ctx run)]
    (data/lifecycle-plan (:settings ctx) (:plan e) phase (data-state ctx run))))

(defn- tolerated? [step res]
  (and (r/err? res) (:step/tolerate step)
       (str/includes? (str (:stderr res)) (:step/tolerate step))))

(defn- opts-of [step]
  (cond-> {:env (:step/env step)}
    (:step/timeout-ms step) (assoc :timeout-ms (:step/timeout-ms step))))

(defn- provider-dir [ctx sp step]
  (let [provider (get-in sp [:stack/instances (:step/instance step) :instance/provider])]
    (get-in ctx [:settings :compose/projects provider :project/dir])))

(defn- run-step
  "Result of one step through its port."
  [ctx sp step]
  (case (:step/target step)
    :exec (let [e (:engine ctx)]
            (if (satisfies? port/IDataExec e)
              (port/-exec! e {:project (:step/compose-project step) :service (:step/service step)}
                           (:step/argv step) (opts-of step))
              (r/err :compose/data-exec-unsupported {:step (:step/id step)
                                                     :hint "the engine has no IDataExec (docker exec)"})))
    :host (let [rn (:runner ctx)]
            (if (satisfies? port/ICommandRunner rn)
              (port/-run-command! rn (:step/argv step)
                          (cond-> (opts-of step)
                            (provider-dir ctx sp step) (assoc :dir (provider-dir ctx sp step))))
              (r/err :compose/data-runner-unsupported {:step (:step/id step)
                                                       :hint "the runner has no ICommandRunner"})))
    :state (r/ok {:prefix (:step/prefix step)})))

(defn- row [step res]
  (cond-> {:step (:step/id step) :kind (:step/kind step) :instance (:step/instance step)
           :consumer (:step/consumer step) :target (:step/target step)}
    (r/ok? res) (assoc :ok? true)
    (tolerated? step res) (assoc :ok? true :tolerated (:step/tolerate step))
    (and (r/err? res) (not (tolerated? step res))) (assoc :ok? false :error (dissoc res :argv))
    (:step/database step) (assoc :database (:step/database step))
    (:step/prefix step) (assoc :prefix (:step/prefix step))))

(defn run-phase!
  "Result<{:run :phase :steps [row] :state}>: run the data lifecycle `phase`
   (:before-suite :between-suites :after) of recorded stack run `run`. Steps
   run in plan order and stop at the first failure, which answers
   :compose/data-step-failed carrying the rows so far; the data state is then
   left as it was. A run whose providers declare no lifecycle answers no steps."
  [ctx run phase]
  (r/let-ok [e (run-entry ctx run)
             p (data/lifecycle-plan (:settings ctx) (:plan e) phase (data-state ctx run))]
    (let [sp (:plan e)
          rows (reduce (fn [acc step]
                         (let [x (row step (run-step ctx sp step))]
                           (if (:ok? x) (conj acc x) (reduced (conj acc x)))))
                       [] (:steps p))]
      (if (every? :ok? rows)
        ;; only a run still recorded: one taken down meanwhile is never revived
        ;; as a bare {:data ..} entry
        (let [[old _] (swap-vals! (:state ctx)
                                  #(if (contains? (:stacks %) run)
                                     (assoc-in % [:stacks run :data] (:state p))
                                     %))]
          (if (contains? (:stacks old) run)
            (do (persist! ctx)
                (r/ok {:run run :phase phase :steps rows :state (:state p)}))
            (r/err :compose/unknown-run {:run run :phase phase :steps rows
                                         :hint "the run was taken down while its data phase ran"})))
        (r/err :compose/data-step-failed {:run run :phase phase :steps rows
                                          :failed (:step (last rows))
                                          :hint "the data state is unchanged; fix and run the phase again"})))))
