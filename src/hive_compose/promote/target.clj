(ns hive-compose.promote.target
  "Pure: a target names services or native compose profiles of a configured
   Project; a section is the Profile that runs exactly what it names, closed
   over the compose file's depends_on."
  (:require [clojure.string :as str]
            [hive-compose.promote.env :as env]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn parse
  "{:project id-or-nil :names [name ..]} of \"sisf-web\", \"sisf/sisf-web\" or
   \"sisf/sisf-web,sisf-crm\" (\"+\" separates too)."
  [s]
  (let [[a b] (str/split (str/trim (or s "")) #"/" 2)
        [project names] (if b [a b] [nil a])]
    {:project (when-not (str/blank? project) project)
     :names (vec (distinct (remove str/blank? (map str/trim (str/split (or names "") #"[,+]")))))}))

(defn section-id
  "`<project>/<name>[+<name>..]`, names sorted so one section has one id."
  [project-id names]
  (str project-id "/" (str/join "+" (sort names))))

(defn project-profile
  "Profile addressing the whole of `project`, with no service subset."
  [project]
  (cond-> {:profile/id (:project/id project)
           :profile/dir (:project/dir project)}
    (:project/files project) (assoc :profile/files (:project/files project))
    (:project/name project) (assoc :profile/project (:project/name project))
    (seq (:project/programs project)) (assoc :profile/programs (vec (:project/programs project)))
    (:project/env project) (assoc :profile/env (:project/env project))
    (some? (:project/wait? project)) (assoc :profile/wait? (:project/wait? project))
    (:project/ttl-minutes project) (assoc :profile/ttl-minutes (:project/ttl-minutes project))
    (:project/idle-action project) (assoc :profile/idle-action (:project/idle-action project))
    (:project/activity project) (assoc :profile/activity (:project/activity project))))

(defn section
  "The Profile of the section of `project` that `names` resolved to `services`."
  [project names services]
  (assoc (project-profile project)
         :profile/id (section-id (:project/id project) names)
         :profile/services (vec (distinct services))))

(def ^:private project-derived
  "Profile keys a section takes from its project rather than from the call."
  [:profile/dir :profile/files :profile/project :profile/programs :profile/env
   :profile/ttl-minutes :profile/idle-action :profile/activity])

(defn rebase-section
  "Snapshot `snap` of an active section re-based on its project as configured
   now (`new-proj`; `old-proj` as it was when the snapshot was taken): dir,
   files, compose name, programs, env, TTL and idle action follow `new-proj`.
   The id, services and per-call flags stay; env entries the call added over
   `old-proj`'s env stay over `new-proj`'s."
  [old-proj new-proj snap]
  (let [old-env (:project/env old-proj)
        extra (into {} (remove (fn [[k v]] (= v (get old-env k)))) (:profile/env snap))
        base (dissoc (project-profile new-proj) :profile/id :profile/wait?)
        ;; a remembered call env stays strongest, even where it equals the old project's
        env (merge (:profile/env base) extra (:profile/call-env snap))]
    (cond-> (merge (apply dissoc snap project-derived) (dissoc base :profile/env))
      (seq env) (assoc :profile/env env))))

;; ---------------------------------------------------------------------------
;; effective compose inputs (card 20261123112129-701a7d69)

(defn apply-call-env
  "Remembered call env `remembered` after the env `call` of one call: a value
   sets its var, a nil forgets it (a nil for a var not remembered is a
   no-op). References stay references. Pure."
  [remembered call]
  (reduce-kv (fn [m k v] (if (nil? v) (dissoc m k) (assoc m k v)))
             (or remembered {})
             (or call {})))

(defn set-values
  "`env` without its nil values: what compose is given."
  [env]
  (into {} (remove (comp nil? val)) env))

(defn with-call-env
  "`p` with call env `env` over its own, and `env` remembered under
   `:profile/call-env`, so a later up, restart or reap of the same section
   re-applies it instead of falling back to weaker env. A nil value forgets
   that var from the remembered env. References stay references. `p` itself
   when `env` is empty."
  [p env]
  (if (empty? env)
    p
    (let [ce (apply-call-env (:profile/call-env p) env)]
      (cond-> (update p :profile/env merge (set-values env))
        true (dissoc :profile/call-env)
        (seq ce) (assoc :profile/call-env ce)))))

(defn declared-env
  "The env section or preset `id` declares in `settings` now: a configured
   preset's :profile/env, else the :project/env of the project a
   `<project>/<names>` section belongs to; nil when neither is configured."
  [settings id]
  (if-let [c (get-in settings [:compose/profiles id])]
    (:profile/env c)
    (let [[pid sec] (str/split (str id) #"/" 2)]
      (when sec (get-in settings [:compose/projects pid :project/env])))))

(defn peel-call-env
  "Snapshot `snap`, whose :profile/env holds its remembered call env, with
   that layer taken off: each remembered var gets its value in `declared`, or
   goes when `declared` has none. :profile/call-env is kept. Pure."
  [snap declared]
  (let [env (reduce (fn [e k] (if (contains? declared k) (assoc e k (get declared k)) (dissoc e k)))
                    (:profile/env snap)
                    (keys (:profile/call-env snap)))]
    (if (seq env) (assoc snap :profile/env env) (dissoc snap :profile/env))))

(defn stale-call-env-keys
  "Sorted keys of remembered call env `remembered` whose DECLARED value
   changed from `old-declared` to `new-declared` (added, removed or edited):
   the edit is the newer intent, so a reload forgets them. A key whose
   declared value did not change stays. Pure."
  [old-declared new-declared remembered]
  (vec (sort (filter #(not= (get old-declared % ::none) (get new-declared % ::none))
                     (keys remembered)))))

(defn remember-call-env
  "Snapshot `snap` after a call with env `call` that recreated its services:
   the call's values remembered over the section's, a nil forgetting that var
   (back to its value in `declared`, or gone). Pure."
  [snap call declared]
  (let [ce (apply-call-env (:profile/call-env snap) call)
        env (merge (:profile/env (peel-call-env snap declared)) ce)]
    (cond-> (dissoc snap :profile/call-env :profile/env)
      (seq env) (assoc :profile/env env)
      (seq ce) (assoc :profile/call-env ce))))

(defn forget-call-env
  "Snapshot `snap` with remembered keys `ks` forgotten: gone from
   :profile/call-env, and in :profile/env back to their value in `declared`
   (or gone when it has none). Pure."
  [snap ks declared]
  (let [ce (apply dissoc (:profile/call-env snap) ks)
        env (reduce (fn [e k] (if (contains? declared k) (assoc e k (get declared k)) (dissoc e k)))
                    (:profile/env snap) ks)]
    (cond-> (dissoc snap :profile/call-env :profile/env)
      (seq env) (assoc :profile/env env)
      (seq ce) (assoc :profile/call-env ce))))

(def ^:private holder-inputs
  "What a holding section's snapshot decides for a call on one of its services."
  [:profile/dir :profile/files :profile/project :profile/compose-profiles])

(defn effective-profile
  "The Profile an engine call on `p` runs with: its dir, files and env. Pure.

   `holder`: the Profile of the one live section that holds `p`'s services
   (nil when none). The call then runs with that section's compose inputs:
   its dir, files, compose name and native profiles replace `p`'s (a key it
   lacks is dropped, so compose uses its default) and its env replaces `p`'s
   own, so no project value the section did not run with leaks into the
   recreate.
   Env precedence, weakest to strongest:
     the base env: `holder`'s :profile/env when there is a holder, else `p`'s
     own (a target section: its project's :project/env; a preset: its own
     :profile/env, which already beats the project's: a preset never
     inherits :project/env)
     < the remembered :profile/call-env (`holder`'s, then `p`'s)
     < `call-env`, the env of this call.
   A call env is remembered under :profile/call-env; a nil value in it
   forgets that var (see `apply-call-env`), so the base value applies again.
   The base env must not already carry the remembered layer (see
   `peel-call-env`) for a forgotten var to fall back."
  [p {:keys [holder call-env]}]
  (let [base (if holder
               (merge (apply dissoc p holder-inputs) (select-keys holder holder-inputs))
               p)
        remembered (apply-call-env (merge (:profile/call-env holder) (:profile/call-env p)) call-env)
        env (merge (if holder (:profile/env holder) (:profile/env p)) remembered)]
    (cond-> (dissoc base :profile/env :profile/call-env)
      (seq env) (assoc :profile/env env)
      (seq remembered) (assoc :profile/call-env remembered))))

(defn src-var?
  "True when env var `k` names the path of code under test (`*_SRC`): status
   shows its value, every other var by name only."
  [k]
  (str/ends-with? (str k) "_SRC"))

(defn- path-like? [v]
  (and (string? v) (boolean (re-find #"^[./~]" v))))

(defn inputs-summary
  "{:dir :files :env-keys :call-env-keys :env-src} of Profile `p` for status:
   the checkout and files it runs with, the NAMES of its env vars and of those
   a call gave it (remembered, see `with-call-env`), and the value of the
   `*_SRC` ones that look like a path (a reference as its rendering, never
   read; any other value by name only)."
  [p]
  (let [env (:profile/env p)]
    {:dir (:profile/dir p)
     :files (vec (:profile/files p))
     :env-keys (vec (sort (keys env)))
     :call-env-keys (vec (sort (keys (:profile/call-env p))))
     :env-src (into (sorted-map)
                    (for [[k v] env
                          :when (and (src-var? k) (or (env/ref? v) (path-like? v)))]
                      [k (if (env/ref? v) (env/render v) v)]))}))

(defn profile-members
  "Services a native compose profile adds over the always-on ones."
  [with-profile always-on]
  (vec (sort (remove (set always-on) with-profile))))

(defn unknown-service?
  "True when `res` is compose refusing a service name it does not define."
  [res]
  (and (r/err? res) (str/includes? (str (:stderr res)) "no such service")))
