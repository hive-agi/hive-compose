(ns hive-compose.pipeline.restart
  "Recycling a section's services in place. Unlike down/stop, a restart takes
   nothing away from another active section that needs the service, so it
   runs even when the service is shared: a container whose process died
   inside (a dev server under a file watcher) is recycled through the tool
   instead of by hand."
  (:require [hive-compose.pipeline.ops :as ops]
            [hive-compose.port :as port]
            [hive-compose.promote.profile :as profile]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- active-entry [ctx id]
  (get-in @(:state ctx) [:active id]))

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
      (when (active-entry ctx id) (ops/touch! ctx id))
      (r/ok (cond-> {:profile id
                     :project (profile/project p)
                     :services services
                     :action (if recreate? :recreate :restart)
                     :out (:out out)}
              (:skipped out) (assoc :note "no service to restart"))))))
