(ns hive-compose.promote.data
  "Pure data lifecycle of a stack run (hc-9): seed, migrate and reset per
   capability, rendered as an ordered step list a pipeline runs.

   A provider declares, beside its Provide (unqualified keys, an overlay on
   schema/Provide validated here, see `ProvideData`):

     {:migrate DataCommand :seed DataCommand :reset DataCommand
      :reset-strategy :command|:template-db|:prefix-rotate
      :template {:user \"postgres\" :admin-db \"postgres\" :database-env \"PGDATABASE\"}}

   A DataCommand is {:command [argv]} (a host process) or {:exec {:service svc
   :argv [..]}} (run inside the instance's container of `svc`), with optional
   :env and :timeout-ms.

   Phases (`lifecycle-plan`):
     :before-suite    migrate, then seed (template-db: into <tenant>_tpl, then
                      CREATE DATABASE <tenant>_1 TEMPLATE <tenant>_tpl).
     :between-suites  reset: :command runs :reset then :seed; :template-db
                      creates <tenant>_<n> from the template; :prefix-rotate
                      moves the consumer to the next tenancy prefix and seeds it.
     :after           :command runs :reset; the others change nothing.

   Every step of a SHARED instance is scoped to one consumer's tenancy (its
   prefix is in the step env, `:step/scope :tenancy`): a shared instance is
   never reset as a whole. Volumes are never removed: a plan holding an argv
   that would (`-v`, `--volumes`, `down -v`, `volume rm|prune`) is refused.
   Secret references in :env stay references; a pipeline reads them at spawn."
  (:require [clojure.string :as str]
            [hive-compose.promote.names :as names]
            [hive-compose.schema :as schema]
            [hive-dsl.result :as r]
            [malli.core :as m]))

;; SPDX-License-Identifier: MIT

;; ---------------------------------------------------------------------------
;; schema overlay (local until hc-lane-core folds it into schema/Provide)

(def Argv [:vector {:min 1} [:string {:min 1}]])

(def DataCommand
  "One data command: a host `:command` argv or an `:exec` inside a service of
   the instance, exactly one of them."
  [:and
   [:map
    [:command {:optional true} Argv]
    [:exec {:optional true} [:map
                             [:service [:string {:min 1}]]
                             [:argv Argv]]]
    [:env {:optional true} schema/Env]
    [:timeout-ms {:optional true} [:int {:min 1}]]]
   [:fn {:error/message "a data command has exactly one of :command or :exec"}
    (fn [c] (= 1 (count (filter #(contains? c %) [:command :exec]))))]])

(def ResetStrategy [:enum :command :template-db :prefix-rotate])

(def Template
  "Postgres template settings: the role psql runs as, the database it
   connects to for CREATE DATABASE, and the env var a consumer reads its
   database name from."
  [:map
   [:user {:optional true} [:string {:min 1}]]
   [:admin-db {:optional true} [:string {:min 1}]]
   [:database-env {:optional true} [:string {:min 1}]]])

(def ProvideData
  "The data keys of a Provide. All optional: a Provide without them has no
   data lifecycle."
  [:map
   [:migrate {:optional true} DataCommand]
   [:seed {:optional true} DataCommand]
   [:reset {:optional true} DataCommand]
   [:reset-strategy {:optional true} ResetStrategy]
   [:template {:optional true} Template]])

(def Phase [:enum :before-suite :between-suites :after])

(def phases [:before-suite :between-suites :after])

(def Step
  "One rendered step. `:step/target` :exec runs `:step/argv` in the container
   of `:step/service` under compose project `:step/compose-project`; :host
   runs it on the host; :state has no argv (a prefix rotation: the pipeline
   records `:step/prefix`). `:step/tolerate` is stderr text that makes a
   failure count as done (an existing template database)."
  [:map
   [:step/id [:string {:min 1}]]
   [:step/phase Phase]
   [:step/kind [:enum :migrate :seed :reset :create-template :create-database :rotate-prefix]]
   [:step/instance [:string {:min 1}]]
   [:step/consumer [:string {:min 1}]]
   [:step/capability [:string {:min 1}]]
   [:step/scope [:enum :tenancy :instance]]
   [:step/target [:enum :exec :host :state]]
   [:step/argv {:optional true} Argv]
   [:step/service {:optional true} [:string {:min 1}]]
   [:step/compose-project {:optional true} [:string {:min 1}]]
   [:step/env schema/Env]
   [:step/timeout-ms {:optional true} [:int {:min 1}]]
   [:step/tolerate {:optional true} [:string {:min 1}]]
   [:step/database {:optional true} [:string {:min 1}]]
   [:step/prefix {:optional true} [:string {:min 1}]]])

;; ---------------------------------------------------------------------------
;; volume safety

(defn removes-volumes?
  "True when `argv` could remove a volume: a `-v` or `--volumes` flag (any
   `--volumes=..` too), a short-flag cluster holding `v` after `down` or `rm`
   (`down -fv`, `rm -fv`), or `volume rm|remove|prune`. Deliberately coarse:
   a tool that spells something else `-v` (psql's variables) must use its
   long form (`--set=`), as the rendered steps do."
  [argv]
  (let [a (mapv str argv)
        destroyer? (some #{"down" "rm"} a)]
    (boolean
     (or (some #(or (= "-v" %) (= "--volumes" %) (str/starts-with? % "--volumes=")) a)
         (and destroyer? (some #(re-matches #"-[a-zA-Z]*v[a-zA-Z]*" %) a))
         (some (fn [[x y]] (and (= "volume" x) (#{"rm" "remove" "prune"} y)))
               (partition 2 1 a))))))

;; ---------------------------------------------------------------------------
;; tenancy names: Kafka prefix rotation, Postgres databases

(defn rotated-prefix
  "The tenancy prefix of `consumer` in `run` at `generation`: generation 0 is
   the wiring's prefix `<run>.<consumer>.` (names/tenancy-prefix), generation
   n > 0 is `<run>.<consumer>.g<n>.`. Distinct per generation, and every one
   starts with the consumer's base prefix, so ACLs on the base still hold."
  [run consumer generation]
  (let [base (names/tenancy-prefix run consumer)]
    (if (pos? generation) (str base "g" generation ".") base)))

(defn prefix-generation
  "The generation `prefix` is at for `consumer` in `run` (inverse of
   `rotated-prefix`), nil when it is not one of theirs."
  [run consumer prefix]
  (let [base (names/tenancy-prefix run consumer)]
    (cond
      (= base prefix) 0
      (and (string? prefix) (str/starts-with? prefix base))
      (some-> (re-matches #"g([1-9][0-9]*)\." (subs prefix (count base))) second parse-long)
      :else nil)))

(defn next-prefix
  "The prefix that follows `prefix` for `consumer` in `run`: one generation
   on. A prefix that is not theirs restarts at generation 1."
  [run consumer prefix]
  (rotated-prefix run consumer (inc (or (prefix-generation run consumer prefix) 0))))

(def ^:private max-db-name 50)

(defn tenant-database
  "Postgres database base name of `consumer` in `run`: `<run>_<consumer>`,
   only [a-z0-9_], kept within 50 chars (63 is Postgres' limit, and `_tpl` or
   `_<n>` follow) by a stable hash suffix."
  [run consumer]
  (let [s (str/replace (str (schema/slug run) "_" (schema/slug consumer)) #"[^a-z0-9_]" "_")]
    (if (<= (count s) max-db-name)
      s
      (str (subs s 0 (- max-db-name 9)) "_" (format "%08x" (bit-and (hash s) 0xffffffff))))))

(defn template-database [tenant] (str tenant "_tpl"))

(defn generation-database
  "The database a consumer uses at `generation`: `<tenant>_<generation+1>`."
  [tenant generation]
  (str tenant "_" (inc generation)))

(defn- quote-ident [s] (str "\"" (str/replace s "\"" "\"\"") "\""))

(defn create-template-sql [tenant]
  (str "CREATE DATABASE " (quote-ident (template-database tenant))))

(defn create-database-sql
  "CREATE DATABASE <tenant>_<n> TEMPLATE <tenant>_tpl."
  [tenant generation]
  (str "CREATE DATABASE " (quote-ident (generation-database tenant generation))
       " TEMPLATE " (quote-ident (template-database tenant))))

(defn psql-argv
  "psql running `sql` as `user` against `admin-db`, stopping on the first
   error. `--set=` rather than `-v`: no step carries `-v`."
  [{:keys [user admin-db] :or {user "postgres" admin-db "postgres"}} sql]
  ["psql" "-U" user "-d" admin-db "--set=ON_ERROR_STOP=1" "-c" sql])

;; ---------------------------------------------------------------------------
;; data state: generations per instance and consumer

(def DataState
  "What the lifecycle of a run has done so far: the generation each consumer
   is at on each instance (0 before any reset). A pipeline keeps it per run."
  [:map
   [:generations {:optional true} [:map-of :string [:map-of :string [:int {:min 0}]]]]])

(defn generation
  "The generation `consumer` is at on instance `iid` in `state`."
  [state iid consumer]
  (get-in state [:generations iid consumer] 0))

(def tenant-prefix-var "HIVE_TENANT_PREFIX")
(def tenant-database-var "HIVE_TENANT_DATABASE")

(defn strategy
  "The reset strategy of a ProvideData: the declared one, else :command when
   it has a :reset, else nil (no reset)."
  [pd]
  (or (:reset-strategy pd) (when (:reset pd) :command)))

(defn- data-keys [provide] (select-keys provide [:migrate :seed :reset :reset-strategy :template]))

(defn- command-argv [cmd] (or (:command cmd) (get-in cmd [:exec :argv])))

(defn provide-data
  "Result<ProvideData|nil> of the Provide of `capability` by project
   `provider` in `settings`: nil when it declares no data lifecycle. Any
   declared command that would remove a volume is refused, whichever phase
   would run it."
  [settings provider capability]
  (let [pd (data-keys (get-in settings [:compose/projects provider :project/provides capability] {}))
        wipe (first (filter #(removes-volumes? (command-argv (get pd %))) [:migrate :seed :reset]))]
    (cond
      (empty? pd) (r/ok nil)
      (not (m/validate ProvideData pd))
      (r/err :compose/invalid-data-lifecycle {:provider provider :capability capability
                                              :problems (schema/explain ProvideData pd)})
      wipe
      (r/err :compose/volume-removal-refused {:provider provider :capability capability :command wipe
                                              :argv (command-argv (get pd wipe))
                                              :hint "data commands never remove volumes; reset by tenancy"})
      (and (= :command (:reset-strategy pd)) (not (:reset pd)))
      (r/err :compose/invalid-data-lifecycle {:provider provider :capability capability
                                              :problems {:reset ["reset-strategy :command needs a :reset command"]}})
      :else (r/ok pd))))

(defn- tenancy-var [settings ip]
  (get-in settings [:compose/projects (:instance/provider ip) :project/provides
                    (:instance/capability ip) :tenancy :env]))

(defn pairs
  "Result<[{:instance ip :consumer c :data pd :tenancy-var var}]>: every
   consumer bound to an instance whose Provide declares a data lifecycle, in
   the stack's start order of instances, then by consumer. An instance a
   consumer itself provides is its own business and left out."
  [settings sp]
  (let [order (into {} (map-indexed (fn [i s] [(:step/id s) i])) (:stack/start-order sp))
        rows (for [[c cp] (sort-by key (:stack/consumers sp))
                   [_ iid] (sort-by key (:consumer/bindings cp))
                   :let [ip (get-in sp [:stack/instances iid])]
                   :when (and ip (not (some #{iid} (:consumer/provider-of cp))))]
               [c ip])]
    (r/map-ok
     (reduce (fn [acc [c ip]]
               (let [pd (provide-data settings (:instance/provider ip) (:instance/capability ip))]
                 (cond (r/err? pd) (reduced pd)
                       (nil? (:ok pd)) acc
                       :else (r/map-ok acc #(conj % {:instance ip :consumer c :data (:ok pd)
                                                    :tenancy-var (tenancy-var settings ip)})))))
             (r/ok []) rows)
     (fn [ps] (vec (sort-by (fn [{:keys [instance consumer]}]
                              [(get order (:instance/id instance) Long/MAX_VALUE) (:instance/id instance) consumer])
                            ps))))))

;; ---------------------------------------------------------------------------
;; steps

(defn- tenancy-env
  "The env that scopes a step to `consumer`'s tenancy at `gen`: its prefix
   (HIVE_TENANT_PREFIX and the provider's tenancy var) and, with a database,
   HIVE_TENANT_DATABASE and the template's database var."
  [{:keys [instance consumer data tenancy-var]} run gen database]
  (let [prefix (rotated-prefix run consumer gen)
        db-var (get-in data [:template :database-env])]
    (cond-> {tenant-prefix-var prefix}
      tenancy-var (assoc tenancy-var prefix)
      database (assoc tenant-database-var database)
      (and database db-var) (assoc db-var database)
      true (assoc "HIVE_INSTANCE" (:instance/id instance)))))

(defn- base-step [{:keys [instance consumer]} phase kind]
  {:step/id (str (name phase) "/" (:instance/id instance) "/" consumer "/" (name kind))
   :step/phase phase
   :step/kind kind
   :step/instance (:instance/id instance)
   :step/consumer consumer
   :step/capability (:instance/capability instance)
   :step/scope (if (:instance/share? instance) :tenancy :instance)
   :step/compose-project (:instance/compose-project instance)})

(defn- command-step
  "The step running DataCommand `cmd` as `kind` for pair `p`, its env the
   command's own with the tenancy env forced over it."
  [p phase kind cmd tenv]
  (let [b (base-step p phase kind)]
    (cond-> (assoc b :step/env (merge (:env cmd {}) tenv))
      (:timeout-ms cmd) (assoc :step/timeout-ms (:timeout-ms cmd))
      (:command cmd) (assoc :step/target :host :step/argv (vec (:command cmd)))
      (:exec cmd) (assoc :step/target :exec
                         :step/service (get-in cmd [:exec :service])
                         :step/argv (vec (get-in cmd [:exec :argv]))))))

(defn- sql-step [p phase kind sql tenv database]
  (assoc (base-step p phase kind)
         :step/target :exec
         :step/service (:instance/service (:instance p))
         :step/argv (psql-argv (get-in p [:data :template]) sql)
         :step/env tenv
         :step/database database
         :step/tolerate "already exists"))

(defn- migrate+seed
  "The migrate then seed steps of `p` (whichever it declares), with `tenv`."
  [p phase tenv]
  (let [d (:data p)]
    (cond-> []
      (:migrate d) (conj (command-step p phase :migrate (:migrate d) tenv))
      (:seed d) (conj (command-step p phase :seed (:seed d) tenv)))))

(defn- pair-steps
  "{:steps [Step] :generation n} of pair `p` in `phase`, `gen` being the
   generation it is at."
  [p run phase gen]
  (let [d (:data p)
        strat (strategy d)
        c (:consumer p)
        tenant (tenant-database run c)
        tpl (template-database tenant)]
    (case [phase (if (= :template-db strat) :template-db :other)]
      [:before-suite :template-db]
      (let [tpl-env (tenancy-env p run gen tpl)
            db (generation-database tenant gen)]
        {:generation gen
         :steps (-> [(sql-step p phase :create-template (create-template-sql tenant) tpl-env tpl)]
                    (into (migrate+seed p phase tpl-env))
                    (conj (sql-step p phase :create-database (create-database-sql tenant gen)
                                    (tenancy-env p run gen db) db)))})

      [:before-suite :other]
      {:generation gen :steps (migrate+seed p phase (tenancy-env p run gen nil))}

      [:between-suites :template-db]
      (let [g (inc gen) db (generation-database tenant g)]
        {:generation g
         :steps [(sql-step p phase :create-database (create-database-sql tenant g)
                           (tenancy-env p run g db) db)]})

      [:between-suites :other]
      (case strat
        :command (let [tenv (tenancy-env p run gen nil)]
                   {:generation gen
                    :steps (into [(command-step p phase :reset (:reset d) tenv)]
                                 (migrate+seed p phase tenv))})
        :prefix-rotate (let [g (inc gen) tenv (tenancy-env p run g nil)]
                         {:generation g
                          :steps (into [(assoc (base-step p phase :rotate-prefix)
                                               :step/target :state
                                               :step/env tenv
                                               :step/prefix (rotated-prefix run c g))]
                                       (migrate+seed p phase tenv))})
        {:generation gen :steps []})

      ([:after :template-db] [:after :other])
      {:generation gen
       :steps (if (= :command strat)
                [(command-step p phase :reset (:reset d) (tenancy-env p run gen nil))]
                [])})))

(defn- check-steps
  "Result<steps>: every step a valid Step, an :exec step naming a service of
   its instance, and none removing a volume."
  [sp steps]
  (let [services (fn [iid] (let [ip (get-in sp [:stack/instances iid])]
                             (into (set (:instance/services ip [(:instance/service ip)]))
                                   (:instance/needs ip))))
        bad (first (remove #(m/validate Step %) steps))
        stray (first (filter #(and (= :exec (:step/target %))
                                   (not (contains? (services (:step/instance %)) (:step/service %))))
                             steps))
        wipe (first (filter #(removes-volumes? (:step/argv %)) steps))]
    (cond
      wipe (r/err :compose/volume-removal-refused {:step (:step/id wipe) :argv (:step/argv wipe)
                                                   :hint "data commands never remove volumes; reset by tenancy"})
      bad (r/err :compose/invalid-data-step {:step (:step/id bad) :problems (schema/explain Step bad)})
      stray (r/err :compose/unknown-data-service {:step (:step/id stray) :service (:step/service stray)
                                                  :instance (:step/instance stray)})
      :else (r/ok steps))))

(defn lifecycle-plan
  "Result<{:phase :run :steps [Step] :state DataState}> of StackPlan `sp` in
   `phase` (:before-suite :between-suites :after), from data `state` (what
   earlier phases of the run did, {} at first). Pure and deterministic: the
   same arguments always answer the same plan. Pairs (instance, consumer) run
   in the stack's start order; within a pair migrate precedes seed. `:state`
   is the state once the steps have run (generations advanced by a reset)."
  ([settings sp phase] (lifecycle-plan settings sp phase {}))
  ([settings sp phase state]
   (cond
     (not (m/validate Phase phase)) (r/err :compose/invalid-phase {:phase phase :phases phases})
     (not (m/validate DataState (or state {}))) (r/err :compose/invalid-data-state
                                                       {:problems (schema/explain DataState state)})
     :else
     (let [run (:stack/run sp)]
       (r/let-ok [ps (pairs settings sp)]
         (let [rows (mapv (fn [p]
                            (let [iid (:instance/id (:instance p))
                                  c (:consumer p)]
                              (assoc (pair-steps p run phase (generation state iid c)) :iid iid :consumer c)))
                          ps)]
           (r/let-ok [steps (check-steps sp (vec (mapcat :steps rows)))]
             (r/ok {:phase phase :run run :steps steps
                    :state (reduce (fn [s {:keys [iid consumer generation]}]
                                     (assoc-in s [:generations iid consumer] generation))
                                   (or state {}) rows)}))))))))
