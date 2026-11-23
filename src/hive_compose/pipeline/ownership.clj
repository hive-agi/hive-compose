(ns hive-compose.pipeline.ownership
  "The guard `up`, `switch` and `restart` pass before they create or recreate
   containers: refuse when a service the call would recreate is held by
   another live section, naming the section and the agents that own it (card
   20261113112930-07a70584). The decision is `promote.ownership/conflicts`;
   this namespace only gathers its inputs: the services the call touches
   and the active map.

   `allow?` (the tool's allow_shared) lets a refused call through and the
   answer names what it overrode. A failure to work out the services (compose
   rejects the config) is reported under :ownership-check and never blocks:
   the call itself will meet the same failure."
  (:require [hive-compose.port :as port]
            [hive-compose.promote.ownership :as ownership]
            [hive-compose.promote.plan :as plan]
            [hive-compose.promote.profile :as profile]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def override-param "allow_shared")

(defn- active-services [ctx p]
  (get-in @(:state ctx) [:active (:profile/id p) :services]))

(defn touched
  "Result<[service]>: what call `mode` (:up or :restart) on `p` would create
   or recreate. :restart touches the named services, else the active entry's,
   else the closure (as `restart-profile!`). :up touches the named services
   under no_deps, else their depends_on closure."
  [ctx p mode]
  (case mode
    :restart (if-let [s (or (seq (:profile/services p)) (seq (active-services ctx p)))]
               (r/ok (vec s))
               (port/-closure (:engine ctx) p))
    :up (if (and (:profile/no-deps? p) (seq (:profile/services p)))
          (r/ok (profile/up-services p nil))
          (port/-closure (:engine ctx) p))))

(defn- others-hold-anything?
  "True when another live section of `p`'s project holds any service at all:
   only then is the compose query for the touched services worth running."
  [ctx p]
  (let [active (:active @(:state ctx))]
    (boolean (seq (ownership/holders active (:profile/id p) (profile/project p)
                                     (mapcat plan/footprint (vals active)))))))

(defn check
  "Result<[Conflict]> of call `mode` on `p` (`recreate?` for a recreating
   restart). A plain restart recreates nothing and answers []."
  [ctx p mode recreate?]
  (if (or (and (= :restart mode) (not recreate?))
          (not (others-hold-anything? ctx p)))
    (r/ok [])
    (r/let-ok [services (touched ctx p mode)]
      (r/ok (ownership/conflicts {:active (:active @(:state ctx))
                                  :self (:profile/id p)
                                  :project (profile/project p)
                                  :p p
                                  :services services
                                  :recreate? (boolean recreate?)})))))

(defn refusal
  "The error refusing `p` because live sections hold what it would recreate."
  [p found]
  (r/err :compose/held-by-another-section
         {:profile (:profile/id p)
          :holders (mapv #(select-keys % [:section :agents :services :reason :differs :orphaned?]) found)
          :message (str "another live section holds what this call would recreate: "
                        (ownership/describe found)
                        ". Recreating it now takes it from under that section.")
          :hint (str "ask the owning agent first (or work in its section: claim its target), "
                     "or pass " override-param "=true to recreate anyway")}))

(defn- container-ids
  "{service id} of `services` of `p`'s project now, nil when the engine cannot
   tell (no IContainerIds) or the read failed: the outcome is then unknown."
  [ctx p services]
  (let [engine (:engine ctx)]
    (when (satisfies? port/IContainerIds engine)
      (let [res (try (port/-container-ids engine p services)
                     (catch Throwable _ nil))]
        (when (r/ok? res) (:ok res))))))

(defn guarded
  "Run `f` (a thunk answering a Result) unless another live section holds what
   call `mode` on `p` would recreate; `allow?` runs it anyway and the ok
   answer carries :shared-overridden, each holder row with :outcome {service
   \"recreated\"|\"kept\"} read from the container ids before and after the
   call (what the engine did), or :outcome-unknown true when the engine
   cannot name its containers."
  [ctx p mode recreate? allow? f]
  (let [c (try (check ctx p mode recreate?)
               (catch Throwable t (r/err :compose/ownership-check-failed {:message (str t)})))
        found (when (r/ok? c) (seq (:ok c)))]
    (cond
      (and found (not allow?)) (refusal p found)
      found (let [shared (vec (distinct (mapcat :services found)))
                  before (container-ids ctx p shared)
                  res (f)
                  after (when (and before (r/ok? res)) (container-ids ctx p shared))]
              (r/map-ok res #(assoc % :shared-overridden
                                    (mapv (fn [h]
                                            (cond-> (select-keys h [:section :agents :services :reason])
                                              after (assoc :outcome (ownership/outcomes (:services h) before after))
                                              (not after) (assoc :outcome-unknown true)))
                                          found))))
      (r/err? c) (r/map-ok (f) #(assoc % :ownership-check (select-keys c [:error :message :category])))
      :else (f))))
