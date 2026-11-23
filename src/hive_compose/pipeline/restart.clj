(ns hive-compose.pipeline.restart
  "Recycling a section's services in place. Unlike down/stop, a restart takes
   nothing away from another active section that needs the service, so it
   runs even when the service is shared: a container whose process died
   inside (a dev server under a file watcher) is recycled through the tool
   instead of by hand."
  (:require [clojure.string :as str]
            [hive-compose.pipeline.ops :as ops]
            [hive-compose.port :as port]
            [hive-compose.promote.ownership :as ownership]
            [hive-compose.promote.profile :as profile]
            [hive-compose.promote.target :as target]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- active-entry [ctx id]
  (get-in @(:state ctx) [:active id]))

(defn with-holder
  "Result<Profile>: `p` (a section resolved from a target) re-addressed at the
   live section that holds the services it names, so a restart (recreate or
   not) runs in that section's dir with its files and env and never from
   another checkout (card 20261123112129-701a7d69). Held by none: `p`, the
   project default. Held by several that agree on dir, files and env: the
   first by id. Held by several that disagree: an error naming them; the
   caller picks one with `only` (profile=<id>). The id stays the target's, so
   the ownership guard still sees a call by another section (recreate=true on
   a held service needs allow_shared, as before)."
  [ctx p only]
  (let [services (or (seq (:profile/services p)) [])
        pick (ownership/pick-holder (:active @(:state ctx)) (profile/project p) services only)]
    (cond
      (:holder pick) (r/ok (assoc (target/effective-profile p {:holder (:holder pick)})
                                  :profile/services (vec services)))
      (:disagree pick) (r/err :compose/holders-disagree
                              {:target (:profile/id p) :services (vec services)
                               :holders (:disagree pick)
                               :message (str "the live sections holding " (str/join ", " services)
                                             " run with different compose inputs: "
                                             (str/join ", " (map :section (:disagree pick))))
                               :hint "name the section to restart from with profile=<id>"})
      only (r/err :compose/not-active {:profile only
                                       :hint (str only " holds none of " (str/join ", " services))})
      :else (r/ok p))))

(defn restart-profile!
  "Restart the services `p` names in place, or recreate them when `recreate?`.
   Their own dependencies are not touched. Services: the ones `p` names, else
   the active entry's, else its depends_on closure. The active map is
   unchanged; an active `p` has its idle clock reset."
  [ctx p recreate?]
  (let [id (:profile/id p)]
    (r/let-ok [services (if-let [s (seq (:profile/services p))]
                          (r/ok (vec s))
                          (if-let [a (active-entry ctx id)]
                            (r/ok (vec (:services a)))
                            (port/-closure (:engine ctx) p)))
               out (port/-restart! (:engine ctx) p services (boolean recreate?))]
      ;; a recreate with a call env: the section keeps that env for later calls
      (when (and recreate? (seq (:profile/call-env p)) (:profile (active-entry ctx id)))
        (swap! (:state ctx) update-in [:active id :profile] target/with-call-env (:profile/call-env p)))
      (when (active-entry ctx id) (ops/touch! ctx id))
      (r/ok (cond-> {:profile id
                     :project (profile/project p)
                     :services services
                     :action (if recreate? :recreate :restart)
                     :out (:out out)}
              (:skipped out) (assoc :note "no service to restart"))))))
