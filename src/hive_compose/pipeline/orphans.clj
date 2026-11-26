(ns hive-compose.pipeline.orphans
  "The guard `up`, `switch` and `restart` pass before they start or recreate
   anything: refuse while processes a dead rootless dockerd left behind still
   hold a volume or a host port of the services about to start (incident
   20261105135748-5afc73f2: a recreated postgres ran beside the orphan
   postmaster on the same data dir).

   The guard runs only when the context has a process table (:processes,
   IProcessTable) and the engine answers IContainerOwnership. When the
   process table or the engine cannot answer, the guard says so in the
   result (:orphan-check) and lets the call through: it never blocks on its
   own failure. `allow?` (the tool's allow_orphans) lets a call through
   that it would refuse, naming what it overrode."
  (:require [hive-compose.port :as port]
            [hive-compose.promote.orphan :as orphan]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def override-param "allow_orphans")

(defn- active-services [ctx p]
  (get-in @(:state ctx) [:active (:profile/id p) :services]))

(defn- services-of
  "Result<[service]>: what the call may start: the named services, else the
   active entry's, else the depends_on closure."
  [ctx p]
  (if-let [s (or (seq (:profile/services p)) (seq (active-services ctx p)))]
    (r/ok (vec s))
    (port/-closure (:engine ctx) p)))

(defn enabled?
  "True when `ctx` can look for orphans."
  [ctx]
  (boolean (and (:processes ctx) (satisfies? port/IContainerOwnership (:engine ctx)))))

(defn check
  "Result<{:orphans [Orphan] :resources}> for starting `p`, Result err when a
   read failed."
  [ctx p]
  (let [engine (:engine ctx)]
    (r/let-ok [services (services-of ctx p)
               resources (port/-resources engine p services)
               live (port/-live-containers engine)
               snap (port/-snapshot (:processes ctx))]
      (r/ok {:orphans (orphan/orphans snap live resources) :resources resources}))))

(defn refusal
  "The error refusing `p` because `found` orphans hold its resources."
  [p found]
  (r/err :compose/orphans-hold-resources
         {:profile (:profile/id p)
          :orphans (mapv #(select-keys % [:pid :name :pids :container :holds]) found)
          :message (str "processes of a dead docker daemon still hold this section's resources: "
                        (orphan/describe found)
                        ". Starting or recreating now would run a second copy on the same volume.")
          :hint (str "stop them first (check each is from this stack: postgres SIGQUIT, redis SIGKILL, "
                     "others SIGTERM), or pass " override-param "=true to go ahead anyway")}))

(defn guarded
  "Run `f` (a thunk answering a Result) unless orphans hold what `p` would
   start; `allow?` runs it anyway. The ok answer carries :orphans-overridden
   when it overrode, :orphan-check when the check itself could not run."
  [ctx p allow? f]
  ;; a profile with no compose project starts no container: nothing to hold
  (if (or (not (enabled? ctx)) (false? (:profile/compose? p)))
    (f)
    (let [c (try (check ctx p)
                 (catch Throwable t (r/err :compose/orphan-check-failed {:message (str t)})))
          found (when (r/ok? c) (seq (:orphans (:ok c))))]
      (cond
        (and found (not allow?)) (refusal p found)
        found (r/map-ok (f) #(assoc % :orphans-overridden (mapv (fn [o] (select-keys o [:pid :name :holds])) found)))
        (r/err? c) (r/map-ok (f) #(assoc % :orphan-check (select-keys c [:error :message :category])))
        :else (f)))))
