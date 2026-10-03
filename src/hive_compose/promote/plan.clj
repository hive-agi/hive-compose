(ns hive-compose.promote.plan
  "Pure planning over the active map ({profile-id Active}): which services a
   profile can release without starving another active profile, what a switch
   releases, and what the reaper reaps. Plans are vectors of Release steps."
  (:require [hive-compose.promote.profile :as profile]))

;; SPDX-License-Identifier: MIT

(defn footprint
  "What active entry `a` keeps alive: the services it runs and the depends_on
   closure they need (`:needs`), which may hold services another entry started."
  [a]
  (into (set (:services a)) (:needs a)))

(defn needed-by-others
  "Services of `project` that active profiles other than `id` need."
  [active project id]
  (into #{}
        (comp (remove (fn [[k _]] (= k id)))
              (filter (fn [[_ a]] (= project (:project a))))
              (mapcat (fn [[_ a]] (footprint a))))
        active))

(defn release-services
  "Sorted services of active profile `id`'s footprint that no other active
   profile of its project needs."
  [active id]
  (let [{:keys [project] :as a} (get active id)]
    (vec (sort (remove (needed-by-others active project id) (footprint a))))))

(defn switch-releases
  "Release steps for moving from `current` to `next-id`, which needs
   `next-services` in `next-project`. What the next profile shares stays up."
  [active current next-id next-project next-services]
  (if (or (nil? current) (= current next-id) (not (contains? active current)))
    []
    (let [{:keys [project]} (get active current)
          keep (if (= project next-project) (set next-services) #{})]
      [{:profile/id current
        :project project
        :action :stop
        :services (vec (remove keep (release-services active current)))}])))

(defn down-release
  "The Release step for taking profile `id` down with `action` (:stop or :down).
   An active profile releases what it alone needs; an inactive one releases
   `services` minus what active profiles need."
  [active id project services action]
  {:profile/id id
   :project project
   :action action
   :services (if (contains? active id)
               (release-services active id)
               (vec (sort (remove (needed-by-others active project id) services))))})

(defn idle?
  "True when `ttl-ms` has elapsed since `last-touch`."
  [now-ms last-touch ttl-ms]
  (>= (- now-ms last-touch) ttl-ms))

(defn reap-releases
  "Release steps for every idle profile, applied in id order against a shrinking
   active map, so a service two idle profiles share is released by the last one.
   Profiles whose idle action is :none are never reaped."
  [settings now-ms active]
  (let [profiles (:compose/profiles settings)
        idle-ids (->> active
                      (filter (fn [[id a]]
                                (let [p (or (get profiles id) (:profile a))]
                                  (and (not= :none (profile/idle-action settings p))
                                       (idle? now-ms (:last-touch a) (profile/ttl-ms settings p))))))
                      (map key)
                      sort)]
    (first
     (reduce (fn [[steps remaining] id]
               (let [p (or (get profiles id) (get-in remaining [id :profile]))]
                 [(conj steps {:profile/id id
                               :project (get-in remaining [id :project])
                               :action (profile/idle-action settings p)
                               :services (release-services remaining id)})
                  (dissoc remaining id)]))
             [[] active]
             idle-ids))))
