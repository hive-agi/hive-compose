(ns hive-compose.promote.ownership
  "Pure: which live section holds a service another section is about to
   recreate (card 20261113112930-07a70584, DC-SHARED).

   Several sections can run in one compose project (`dc`): an escala session's
   section and a gestao-usuarios session's section share envoy, auth-service
   and the web relay. compose has one container per service per project, so a
   section that recreates a shared service takes it from under every other
   section holding it: on 2026-11-13 an `up` with other compose files and
   another HOST_GATEWAY_IP recreated dc-envoy and every page of the other
   session answered 503.

   A section HOLDS a service when the service is in its footprint (the
   services it runs and the depends_on closure it needs, see
   `promote.plan/footprint`). A holder is LIVE while it is active and not
   orphaned (its last claim ended; the reaper takes it soon), or while it is
   orphaned but something still uses it.

   A call recreates a held service when:
   - it is `restart recreate=true`: compose recreates the named containers
     whatever their config;
   - it is `up`/`switch` and the call's compose inputs differ from those the
     holder ran with: compose recreates a running container whose config hash
     changed, and the hash follows the files, the project directory and the
     interpolated env. Equal inputs leave the container as it is, so two
     sections of the same project share their dependencies freely.

   Inputs are compared as configured, references rendered (never read): a
   variable that differs is a conflict even when the service does not read
   it, so the guard errs on the side of refusing, and the call can override."
  (:require [clojure.string :as str]
            [hive-compose.promote.env :as env]
            [hive-compose.promote.plan :as plan]))

;; SPDX-License-Identifier: MIT

(defn inputs
  "What compose derives a service's config from, of Profile `p`: project
   directory, files and env (references rendered). Nil when `p` is nil."
  [p]
  (when p
    {:dir (some-> (:profile/dir p) (str/replace #"/+$" ""))
     :files (vec (:profile/files p))
     :env (env/rendered (:profile/env p))}))

(defn differences
  "What differs between compose inputs `mine` and `theirs`, as
   {:dir bool? :files bool? :env [var]}: only the keys that differ, env as the
   sorted names of the variables that differ (never a value). {:unknown true}
   when `theirs` is nil (an entry that kept no profile). {} when equal."
  [mine theirs]
  (if (nil? theirs)
    {:unknown true}
    (let [a (:env mine) b (:env theirs)
          vars (vec (sort (for [k (distinct (concat (keys a) (keys b)))
                                :when (not= (get a k) (get b k))]
                            k)))]
      (cond-> {}
        (not= (:dir mine) (:dir theirs)) (assoc :dir true)
        (not= (:files mine) (:files theirs)) (assoc :files true)
        (seq vars) (assoc :env vars)))))

(defn live?
  "True when active entry `a` still answers for what it holds: not orphaned,
   or orphaned but seen in use by the last usage pass."
  [a]
  (boolean (or (nil? (:orphaned-at a)) (get-in a [:usage :in-use?]))))

(defn agents-of
  "Sorted agent ids that own active entry `a`: those claiming it, then those
   the last usage pass saw working in its project."
  [a]
  (vec (distinct (concat (sort (keys (:claims a)))
                         (sort (keep :agent (get-in a [:usage :by])))))))

(defn holders
  "[{:section id :agents [id] :services [s]}] of the live active entries
   other than `self` in compose project `project` whose footprint holds any
   of `services`, by section id. Pure."
  [active self project services]
  (let [wanted (set services)]
    (vec (for [[id a] (sort-by key active)
               :when (and (not= id self) (= project (:project a)) (live? a))
               :let [held (vec (sort (filter wanted (plan/footprint a))))]
               :when (seq held)]
           {:section id :agents (agents-of a) :services held :entry a}))))

(defn conflicts
  "[Conflict] of a call by section `self` (Profile `p`, compose project
   `project`) about to bring `services` up, or recreate them when
   `recreate?`, over `active` ({id Active}). A Conflict:
   {:section holder-id :agents [id] :services [s] :reason :recreate|:config-differs
    :differs {..}} (see `differences`). Empty when nothing held is touched.
   Pure."
  [{:keys [active self project p services recreate?]}]
  (let [mine (inputs p)]
    (vec (for [{:keys [agents entry] :as h} (holders active self project services)
               :let [differs (differences mine (inputs (:profile entry)))]
               :when (or recreate? (seq differs))]
           (cond-> (-> h
                       (dissoc :entry)
                       (assoc :reason (if recreate? :recreate :config-differs)))
             (seq differs) (assoc :differs differs)
             (:orphaned-at entry) (assoc :orphaned? true)
             (empty? agents) (assoc :agents []))))))

(defn section-holders
  "[{:section id :services [s] :profile Profile}] of the live active entries
   of compose project `project` (`self` included) whose footprint holds any of
   `services` and that kept the Profile they ran with, by section id. Pure."
  [active project services]
  (let [wanted (set services)]
    (vec (for [[id a] (sort-by key active)
               :when (and (= project (:project a)) (live? a) (:profile a))
               :let [held (vec (sort (filter wanted (plan/footprint a))))]
               :when (seq held)]
           {:section id :services held :profile (:profile a)}))))

(defn pick-holder
  "Which live section's compose inputs a call on `services` of `project` runs
   with (card 20261123112129-701a7d69): {:holder Profile :section id} when the
   sections holding them agree on dir, files and env (one section, or several
   with equal inputs; the first by id answers), {:none true} when none does,
   {:disagree [{:section :services :differs}]} when they differ, each row
   saying what differs from the first. `only` (a section id or nil) narrows
   the holders to that one section. Pure."
  [active project services only]
  (let [hs (cond->> (section-holders active project services)
             only (filterv #(= only (:section %))))
        [h & more] hs]
    (cond
      (nil? h) {:none true}
      (every? #(= (inputs (:profile h)) (inputs (:profile %))) more)
      {:holder (:profile h) :section (:section h)}
      :else {:disagree (vec (for [x hs]
                              (cond-> (select-keys x [:section :services])
                                (not= x h) (assoc :differs (differences (inputs (:profile x))
                                                                        (inputs (:profile h)))))))})))

(defn outcomes
  "{service \"recreated\"|\"kept\"} of `services` from the container ids before
   and after a call ({service id}): kept when the same container still runs it,
   recreated otherwise. What the engine did, not what was allowed. Pure."
  [services before after]
  (into (sorted-map)
        (for [s services]
          [s (if (and (some? (get before s)) (= (get before s) (get after s))) "kept" "recreated")])))

(defn describe
  "One line naming each holder of `found`, its agents and what differs."
  [found]
  (str/join "; "
            (for [{:keys [section agents services reason differs]} found]
              (str (str/join ", " services) " held by section " section
                   (if (seq agents) (str " (agent " (str/join ", " agents) ")") " (no claiming agent)")
                   (case reason
                     :recreate ", which recreate=true would replace"
                     (str ", whose containers were created from other compose inputs ("
                          (str/join ", " (concat (when (:unknown differs) ["unknown"])
                                                 (when (:dir differs) ["project dir"])
                                                 (when (:files differs) ["files"])
                                                 (when (seq (:env differs))
                                                   [(str "env " (str/join " " (:env differs)))])))
                          "), so up would recreate them"))))))
