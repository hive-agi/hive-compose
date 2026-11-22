(ns hive-compose.promote.capability
  "Pure capability resolver: which provider answers each need of every
   consumer in a stack, and which instances that makes.

   Projects declare `:project/provides {cap Provide}` and `:project/needs
   {cap Need}` (hive-compose.schema). A consumer is a project id, a section id
   `<project>/<target>` (it needs what its project needs) or any other id
   carrying extra needs (a stack's own needs, a suite's).

   Rules (decision 20261122141106-2d375c4f):
   - one shared instance per (capability, provider), bound by every sharing
     consumer whose range the provider's version satisfies;
   - `:share? false` gives that consumer a private instance nobody else uses;
   - a missing provider, a provider whose version fails a range, or several
     compatible providers with no `:provider` pin are Result errors naming
     the capability and the consumers.

   No IO, no host interop beyond parse-long: cljc-friendly. Output is sorted
   and independent of input order."
  (:require [clojure.string :as str]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

;; ---------------------------------------------------------------------------
;; Versions and ranges

(defn parse-version
  "Result<[major minor? patch?]>: the numeric parts of a Version string
   (\"3.7.1-alpine\" -> [3 7 1]); a suffix after - or + is ignored."
  [s]
  (if-let [[_ nums] (and (string? s)
                         (re-matches #"([0-9]+(?:\.[0-9]+){0,2})(?:[-+][0-9A-Za-z.-]+)?" s))]
    (r/ok (mapv parse-long (str/split nums #"\.")))
    (r/err :compose/invalid-version {:version s
                                     :message (str "not a version: " (pr-str s))})))

(defn- pad
  "`parts` padded with zeros to length `n`."
  [parts n]
  (into (vec parts) (repeat (- n (count parts)) 0)))

(defn compare-versions
  "Compares two parsed versions, missing parts reading as 0: [3] = [3 0 0]."
  [a b]
  (let [n (max (count a) (count b))]
    (compare (pad a n) (pad b n))))

(def ^:private comparator-ops
  {">=" #(>= % 0) "<=" #(<= % 0) ">" pos? "<" neg? "=" zero?})

(defn- bump
  "The first version above every version sharing the first `n` parts of `parts`."
  [parts n]
  (let [p (pad parts n)]
    (conj (subvec p 0 (dec n)) (inc (nth p (dec n))))))

(defn- parse-clause
  "One range token as a vector of [op parts] constraints, or nil when invalid."
  [token]
  (let [nums #(mapv parse-long (str/split % #"\."))]
    (cond
      (= "*" token) []

      (re-matches #"[0-9]+(\.[0-9]+){0,2}" token)
      (let [v (nums token)]
        [[">=" v] ["<" (bump v (count v))]])

      (re-matches #"\^[0-9]+(\.[0-9]+){0,2}" token)
      (let [v (nums (subs token 1))]
        [[">=" v] ["<" (bump v 1)]])

      (re-matches #"~[0-9]+(\.[0-9]+){0,2}" token)
      (let [v (nums (subs token 1))]
        [[">=" v] ["<" (bump v (min 2 (count v)))]])

      :else
      (when-let [[_ op v] (re-matches #"(>=|<=|>|<|=)([0-9]+(?:\.[0-9]+){0,2})" token)]
        [[op (nums v)]]))))

(defn parse-range
  "Result<[[op parts]..]>: a VersionRange as the constraints a version must all
   meet. \"*\" -> []; \"3.7\" -> >=3.7 <3.8 (prefix); \"^3.7\" -> >=3.7 <4 (same
   major); \"~3.7\" -> >=3.7 <3.8 (same minor; \"~3\" same major); comparators
   \">=3.5 <4\" are ANDed. nil reads as \"*\"."
  [s]
  (let [s (or s "*")
        tokens (when (string? s) (remove str/blank? (str/split (str/trim s) #" +")))
        clauses (map parse-clause tokens)
        bare? (and (seq tokens) (not-every? #(re-matches #"(>=|<=|>|<|=).*" %) tokens))]
    (if (or (empty? tokens) (some nil? clauses) (and bare? (> (count tokens) 1)))
      (r/err :compose/invalid-range {:range s
                                     :message (str "not a version range: " (pr-str s))})
      (r/ok (vec (apply concat clauses))))))

(defn in-range?
  "True when parsed version `v` meets every constraint of parsed range `rng`."
  [rng v]
  (every? (fn [[op parts]] ((comparator-ops op) (compare-versions v parts))) rng))

(defn version-satisfies?
  "True when Version string `version` is in VersionRange string `range`. A
   provider with no version satisfies only \"*\" (or no range): what it runs
   is unknown. Invalid input satisfies nothing."
  [range version]
  (let [rng (parse-range range)]
    (and (r/ok? rng)
         (if (nil? version)
           (empty? (:ok rng))
           (let [v (parse-version version)]
             (and (r/ok? v) (in-range? (:ok rng) (:ok v))))))))

;; ---------------------------------------------------------------------------
;; Consumers and their needs

(defn consumer-project
  "The project id whose `:project/needs` consumer `c` inherits: `c` itself
   when it names a project, the part before / for a section id, else nil."
  [projects c]
  (cond
    (contains? projects c) c
    :else (let [p (first (str/split c #"/" 2))]
            (when (and (not= p c) (contains? projects p)) p))))

(defn consumer-needs
  "{consumer Needs} for a stack: each of `consumers` (project or section ids)
   with its project's `:project/needs`, merged with `extra` ({consumer
   Needs}, e.g. a stack's or suite's own needs; an extra need of a capability
   wins over the project's). `projects` is {project-id Project}."
  [projects consumers extra]
  (let [base (into {} (map (fn [c] [c (or (some->> (consumer-project projects c)
                                                   (get projects)
                                                   :project/needs)
                                          {})]))
                   consumers)]
    (merge-with merge base extra)))

;; ---------------------------------------------------------------------------
;; Resolution

(defn providers-of
  "Sorted [project-id Provide] of every project providing `cap`."
  [projects cap]
  (->> projects
       (keep (fn [[id p]] (when-let [prov (get-in p [:project/provides cap])] [id prov])))
       (sort-by first)
       vec))

(defn instance-id
  "`<provider>/<cap>` for a shared instance, `<provider>/<cap>@<consumer>`
   for a private one."
  ([provider cap] (str provider "/" cap))
  ([provider cap consumer] (str provider "/" cap "@" consumer)))

(defn- choose
  "How one need of consumer `c` resolves: {:provider id :provide Provide} or
   {:error kw ...}."
  [projects c cap need]
  (let [range (or (:version need) "*")
        pin (:provider need)
        all (providers-of projects cap)
        candidates (if pin (filterv #(= pin (first %)) all) all)
        fits (filterv #(version-satisfies? range (:version (second %))) candidates)
        base {:capability cap :consumer c :range range}]
    (cond
      (r/err? (parse-range range))
      (assoc base :error :compose/invalid-range)

      (empty? candidates)
      (cond-> (assoc base :error :compose/no-provider)
        pin (assoc :provider pin))

      (empty? fits)
      (assoc base :error :compose/incompatible-version
             :providers (into (sorted-map)
                              (map (fn [[id p]] [id (:version p)]) candidates)))

      (> (count fits) 1)
      (assoc base :error :compose/ambiguous-provider :providers (mapv first fits))

      :else
      (let [[id p] (first fits)] {:provider id :provide p}))))

(def ^:private error-rank
  "Which error group is reported first."
  (zipmap [:compose/invalid-range :compose/no-provider :compose/incompatible-version
           :compose/ambiguous-provider]
          (range)))

(defn- error-message
  [{:keys [error capability consumers providers provider]}]
  (let [who (str/join ", " consumers)]
    (case error
      :compose/invalid-range
      (str who " ask for " capability " with an invalid version range")
      :compose/no-provider
      (str "no project provides " capability (when provider (str " as pinned provider " provider))
           ", needed by " who)
      :compose/incompatible-version
      (str "no provider of " capability " satisfies the version range of " who
           " (available: " (str/join ", " (map (fn [[id v]] (str id " " (or v "unversioned"))) providers)) ")")
      :compose/ambiguous-provider
      (str capability " is provided by " (str/join ", " providers)
           "; pin one with :provider in the needs of " who))))

(defn- group-errors
  "Per-need errors grouped by (error, capability, providers), each naming its
   consumers and their ranges, sorted."
  [errors]
  (->> errors
       (group-by (juxt :error :capability :providers :provider))
       (map (fn [[[e cap providers provider] es]]
              (let [g (cond-> {:error e
                               :capability cap
                               :consumers (vec (sort (distinct (map :consumer es))))
                               :ranges (into (sorted-map) (map (juxt :consumer :range)) es)}
                        providers (assoc :providers providers)
                        provider (assoc :provider provider))]
                (assoc g :message (error-message g)))))
       (sort-by (juxt #(get error-rank (:error %)) :capability
                      (comp pr-str :consumers)))
       vec))

(defn- instance
  [provider cap provide id share? consumers]
  (cond-> {:instance/id id
           :instance/capability cap
           :instance/provider provider
           :instance/service (:service provide)
           :instance/share? share?
           :instance/consumers (vec (sort consumers))
           :instance/aliases [(:service provide)]}
    (:version provide) (assoc :instance/version (:version provide))))

(defn resolve-needs
  "Result<Resolution> for `needs` ({consumer Needs}, see `consumer-needs`)
   against `projects` ({project-id Project}).

   Ok: {:resolution/instances (sorted {id Instance})
        :resolution/bindings (sorted {consumer (sorted {cap instance-id})})}.
   Err: the first error group, sorted, carrying every group under :errors.
   An error is :compose/no-provider, :compose/incompatible-version,
   :compose/ambiguous-provider or :compose/invalid-range, with :capability,
   :consumers, :ranges and :message."
  [projects needs]
  (let [choices (for [[c ns] needs
                      [cap need] ns]
                  [c cap need (choose projects c cap need)])
        errors (keep (fn [[_ _ _ ch]] (when (:error ch) ch)) choices)]
    (if (seq errors)
      (let [groups (group-errors errors)]
        (r/err (:error (first groups)) (assoc (first groups) :errors groups)))
      (let [placed (for [[c cap need {:keys [provider provide]}] choices
                         :let [share? (:share? need true)]]
                     {:consumer c :capability cap :provider provider :provide provide
                      :share? share?
                      :id (if share? (instance-id provider cap) (instance-id provider cap c))})
            instances (->> (group-by :id placed)
                           (map (fn [[id ps]]
                                  (let [{:keys [provider capability provide share?]} (first ps)]
                                    [id (instance provider capability provide id share?
                                                  (distinct (map :consumer ps)))])))
                           (into (sorted-map)))
            bindings (reduce (fn [acc {:keys [consumer capability id]}]
                               (assoc-in acc [consumer capability] id))
                             (into (sorted-map) (map (fn [c] [c (sorted-map)])) (keys needs))
                             placed)]
        (r/ok {:resolution/instances instances
               :resolution/bindings bindings})))))

(defn stack-consumer
  "The consumer id a stack's own `:stack/needs` are bound to: `stack:<id>`."
  [stack]
  (str "stack:" (:stack/id stack)))

(defn resolve-stack
  "Result<Resolution> for Stack `stack` over `projects`: its consumers with
   their projects' needs, plus `:stack/needs` under `stack-consumer`, plus
   `extra` ({consumer Needs}, e.g. suites)."
  ([projects stack] (resolve-stack projects stack {}))
  ([projects stack extra]
   (resolve-needs projects
                  (consumer-needs projects (:stack/consumers stack)
                                  (cond-> extra
                                    (seq (:stack/needs stack))
                                    (update (stack-consumer stack) merge (:stack/needs stack)))))))
