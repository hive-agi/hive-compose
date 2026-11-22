(ns hive-compose.promote.capability
  "Pure capability resolver: which provider answers each need of every
   consumer in a stack, and which instances that makes.

   Projects declare `:project/provides {cap Provide}` and `:project/needs
   {cap Need}` (hive-compose.schema). A consumer is a project id, a section id
   `<project>/<target>` (it needs what its project needs) or any other id
   carrying extra needs (a stack's own needs, a suite's).

   Rules (decision 20261122141106-2d375c4f):
   - per capability, every consumer that shares (`:share?` true, the
     default) is resolved TOGETHER: exactly one provider must satisfy all of
     their ranges and pins, and they bind to its one shared instance
     `<provider>/<cap>`. Incompatible ranges are never split into two shared
     instances: they are an error naming every sharer;
   - `:share? false` resolves that consumer alone, to a private instance
     `<provider>/<cap>@<consumer>` nobody else uses;
   - errors are Results naming the capability and the consumers:
     :compose/invalid-range, :compose/invalid-version (a provider's),
     :compose/unknown-consumer, :compose/conflicting-pins,
     :compose/no-provider, :compose/incompatible-version,
     :compose/ambiguous-provider.

   No IO and no host interop (portable Clojure, ready to become .cljc).
   Output is sorted and independent of input order."
  (:require [clojure.string :as str]
            [hive-compose.promote.profile :as profile]
            [hive-compose.schema :as schema]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

;; ---------------------------------------------------------------------------
;; Versions

(def ^:private version-re
  #"([0-9]+(?:\.[0-9]+){0,2})(?:([-+])([0-9A-Za-z.+-]+))?")

(defn- parse-nums
  "The dot-separated numbers of `s`, or nil when one overflows a long."
  [s]
  (let [parts (mapv parse-long (str/split s #"\."))]
    (when (every? some? parts) parts)))

(defn- parse-version*
  [s]
  (when-let [[_ nums sign suffix] (and (string? s) (re-matches version-re s))]
    (when-let [parts (parse-nums nums)]
      {:nums parts
       :pre (when (= "-" sign) (not-empty (first (str/split suffix #"\+" 2))))})))

(defn parse-version
  "Result<{:nums [major minor? patch?] :pre pre-release-or-nil}> for a Version
   string. A `-` suffix is a pre-release (\"3.7.1-alpine\" -> :pre \"alpine\"),
   ordered below the bare release as semver orders them; a `+` build suffix
   is ignored."
  [s]
  (if-let [v (parse-version* s)]
    (r/ok v)
    (r/err :compose/invalid-version {:version s
                                     :message (str "not a version: " (pr-str s))})))

(defn- pad
  "`parts` padded with zeros to three."
  [parts]
  (into (vec parts) (repeat (- 3 (count parts)) 0)))

(defn- compare-ident
  "Semver pre-release identifier order: numeric ones numerically and below
   alphanumeric ones, which compare as strings."
  [a b]
  (let [na (re-matches #"[0-9]+" a)
        nb (re-matches #"[0-9]+" b)]
    (cond
      (and na nb) (compare [(count a) a] [(count b) b])
      na -1
      nb 1
      :else (compare a b))))

(defn- compare-pre
  "nil (a release) is above every pre-release; :min is below every one."
  [a b]
  (cond
    (= a b) 0
    (= :min a) -1
    (= :min b) 1
    (nil? a) 1
    (nil? b) -1
    :else (let [as (str/split a #"\.")
                bs (str/split b #"\.")]
            (or (first (remove zero? (map compare-ident as bs)))
                (compare (count as) (count bs))))))

(defn compare-versions
  "Orders two parsed versions: by their numbers, missing parts reading as 0
   ([3] = [3 0 0]), then by pre-release."
  [a b]
  (let [c (compare (pad (:nums a)) (pad (:nums b)))]
    (if (zero? c) (compare-pre (:pre a) (:pre b)) c)))

;; ---------------------------------------------------------------------------
;; Ranges

(def ^:private comparator-ops
  {">=" #(>= % 0) "<=" #(<= % 0) ">" pos? "<" neg? "=" zero?})

(defn- lowest
  "The lowest version with numbers `parts`, below its pre-releases too."
  [parts]
  {:nums (pad parts) :pre :min})

(defn- bump
  "The numbers just above every version agreeing with `parts` up to index `i`."
  [parts i]
  (let [p (pad parts)]
    (conj (subvec p 0 i) (inc' (nth p i)))))

(defn- block
  "[>= low, < next] over every version agreeing with `v` up to index `i`. A
   version written with a pre-release starts exactly there."
  [{:keys [nums pre] :as v} i]
  [[">=" (if pre (update v :nums pad) (lowest nums))]
   ["<" (lowest (bump nums i))]])

(defn- caret-index
  "^ keeps the leftmost non-zero part: ^3.7 below 4, ^0.5 below 0.6."
  [nums]
  (or (first (keep-indexed (fn [i x] (when (pos? x) i)) nums))
      (dec (count nums))))

(defn- comparator-clause
  "One comparator. Against a partial version it reads like an X-range:
   `<=3.7` is <3.8, `>3` is >=4, `=3.7` is the 3.7 block."
  [op {:keys [nums pre] :as v}]
  (if (or pre (= 3 (count nums)))
    [[op (update v :nums pad)]]
    (case op
      ">=" [[">=" (lowest nums)]]
      "<" [["<" (lowest nums)]]
      ">" [[">=" (lowest (bump nums (dec (count nums))))]]
      "<=" [["<" (lowest (bump nums (dec (count nums))))]]
      "=" (block v (dec (count nums))))))

(defn- parse-clause
  "One range token as constraints, or nil when invalid."
  [token]
  (let [[_ head body] (re-matches #"(>=|<=|>|<|=|\^|~)?(.*)" token)
        v (parse-version* body)]
    (cond
      (= "*" token) []
      (nil? v) nil
      (nil? head) (block v (dec (count (:nums v))))
      (= "^" head) (block v (caret-index (:nums v)))
      (= "~" head) (block v (if (= 1 (count (:nums v))) 0 1))
      :else (comparator-clause head v))))

(defn parse-range
  "Result<[[op version]..]>: a VersionRange as the constraints a version must
   all meet. nil and \"*\" accept anything. \"3.7\" is the 3.7 block (>=3.7.0
   <3.8, its pre-releases such as 3.7.0-alpine included); \"3.7.1\" is that
   patch with its suffixed tags. \"^3.7\" keeps the leftmost non-zero part
   (<4; \"^0.5\" <0.6). \"~3.7\" keeps the minor (<3.8; \"~3\" <4). Space
   separated comparators \">=3.5 <4\" are ANDed. Anything else, including an
   overflowing number, is :compose/invalid-range."
  [s]
  (let [s (or s "*")
        tokens (when (string? s) (str/split s #" +" -1))
        clauses (map parse-clause tokens)
        cmp? #(re-matches #"(>=|<=|>|<|=)[0-9].*" %)]
    (if (or (not (string? s))
            (str/blank? s)
            (some nil? clauses)
            (and (> (count tokens) 1) (not-every? cmp? tokens)))
      (r/err :compose/invalid-range {:range s
                                     :message (str "not a version range: " (pr-str s))})
      (r/ok (vec (apply concat clauses))))))

(defn in-range?
  "True when parsed version `v` meets every constraint of parsed range `rng`."
  [rng v]
  (every? (fn [[op c]] ((comparator-ops op) (compare-versions v c))) rng))

(defn version-satisfies?
  "True when Version string `version` is in VersionRange string `rng-str`. A
   provider with no version satisfies only \"*\" (or no range): what it runs
   is unknown. Invalid input satisfies nothing."
  [rng-str version]
  (let [rng (parse-range rng-str)]
    (and (r/ok? rng)
         (if (nil? version)
           (empty? (:ok rng))
           (let [v (parse-version* version)]
             (boolean (and v (in-range? (:ok rng) v))))))))

;; ---------------------------------------------------------------------------
;; Consumers and their needs

(defn consumer-project
  "The project id whose `:project/needs` consumer `c` inherits: `c` itself
   when it names a project, the part before / for a section id, else nil."
  [projects c]
  (if (contains? projects c)
    c
    (let [p (first (str/split c #"/" 2))]
      (when (and (not= p c) (contains? projects p)) p))))

(defn consumer-needs
  "Result<{consumer Needs}> for a stack: each of `consumers` (project or
   section ids) with its project's `:project/needs`, merged with `extra`
   ({consumer Needs}, e.g. a stack's or a suite's own needs). Merging is
   key by key: an extra `{\"kafka\" {:share? false}}` keeps the project's
   :version. A consumer with no project and no extra needs is
   :compose/unknown-consumer. `projects` is {project-id Project}."
  [projects consumers extra]
  (let [unknown (->> consumers
                     (remove #(or (consumer-project projects %) (contains? extra %)))
                     distinct sort vec)]
    (if (seq unknown)
      (r/err :compose/unknown-consumer
             {:consumers unknown
              :message (str "no project and no needs for consumer(s) " (str/join ", " unknown))})
      (r/ok (merge-with (partial merge-with merge)
                        (into {} (map (fn [c] [c (or (some->> (consumer-project projects c)
                                                              (get projects)
                                                              :project/needs)
                                                     {})]))
                              consumers)
                        extra)))))

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

(defn compose-project-name
  "The compose project name Project `p` runs under, as compose derives it."
  [p]
  (or (:project/name p) (profile/default-project (:project/dir p))))

(defn- unit-error
  "An error of resolution unit `rows` (needs of one capability resolved
   together), naming its consumers and their ranges."
  [kind cap rows & {:as more}]
  (merge {:error kind
          :capability cap
          :consumers (vec (sort (map :consumer rows)))
          :ranges (into (sorted-map) (map (juxt :consumer :range)) rows)}
         more))

(defn- fits?
  [row [_ provide]]
  (version-satisfies? (:range row) (:version provide)))

(defn- resolve-unit
  "{:provider id :provide Provide} for `rows`, the needs of capability `cap`
   resolved together, else {:errors [..]}."
  [projects cap rows]
  (let [all (providers-of projects cap)
        bad-versions (filterv (fn [[_ p]] (and (:version p) (nil? (parse-version* (:version p))))) all)
        bad-ranges (filterv #(r/err? (parse-range (:range %))) rows)
        pins (into (sorted-map) (keep (fn [row] (when (:pin row) [(:consumer row) (:pin row)]))) rows)
        pin (first (vals pins))
        candidates (if pin (filterv #(= pin (first %)) all) all)
        misfits (filterv (fn [row] (not-any? #(fits? row %) candidates)) rows)
        common (filterv (fn [c] (every? #(fits? % c) rows)) candidates)]
    (cond
      (seq bad-ranges)
      {:errors [(unit-error :compose/invalid-range cap bad-ranges)]}

      (seq bad-versions)
      {:errors [(unit-error :compose/invalid-version cap rows
                            :providers (into (sorted-map) (map (fn [[id p]] [id (:version p)])) bad-versions))]}

      (< 1 (count (distinct (vals pins))))
      {:errors [(unit-error :compose/conflicting-pins cap rows :pins pins)]}

      (empty? candidates)
      {:errors [(cond-> (unit-error :compose/no-provider cap rows)
                  pin (assoc :provider pin))]}

      (or (seq misfits) (empty? common))
      {:errors [(cond-> (unit-error :compose/incompatible-version cap (if (seq misfits) misfits rows)
                                    :providers (into (sorted-map) (map (fn [[id p]] [id (:version p)])) candidates))
                  pin (assoc :provider pin)
                  (empty? misfits) (assoc :split? true))]}

      (< 1 (count common))
      {:errors [(unit-error :compose/ambiguous-provider cap rows :providers (mapv first common))]}

      :else
      (let [[id p] (first common)] {:provider id :provide p}))))

(def ^:private error-rank
  "Which error group is reported first; unknown kinds last."
  (zipmap [:compose/unknown-consumer :compose/invalid-range :compose/invalid-version
           :compose/conflicting-pins :compose/no-provider :compose/incompatible-version
           :compose/ambiguous-provider]
          (range)))

(defn- error-message
  [{:keys [error capability consumers ranges providers provider pins split?]}]
  (let [who (str/join ", " consumers)
        avail #(str/join ", " (map (fn [[id v]] (str id " " (or v "unversioned"))) providers))]
    (case error
      :compose/invalid-range
      (str who " need " capability " with invalid version range(s) " (pr-str ranges))
      :compose/invalid-version
      (str "provider(s) of " capability " declare invalid versions: " (avail))
      :compose/conflicting-pins
      (str who " pin different providers of " capability ": " (pr-str pins))
      :compose/no-provider
      (str "no project provides " capability (when provider (str " as pinned provider " provider))
           ", needed by " who)
      :compose/incompatible-version
      (if split?
        (str "no single provider of " capability " satisfies all of " who " " (pr-str ranges)
             " (available: " (avail) "); align the ranges or opt one out with :share? false")
        (str "no provider of " capability " satisfies " who " " (pr-str ranges)
             " (available: " (avail) ")"))
      :compose/ambiguous-provider
      (str capability " is provided by " (str/join ", " providers)
           "; pin one with :provider in the needs of " who)
      (str (name error) " for " capability ", consumers " who))))

(defn- merge-errors
  "Errors sorted, each with its :message. No-provider errors of one
   capability (a fact about the capability, not about a unit) are merged."
  [errors]
  (let [{merge-able true other false}
        (group-by #(= :compose/no-provider (:error %)) errors)
        merged (for [[_ es] (group-by (juxt :capability :provider) merge-able)]
                 (assoc (first es)
                        :consumers (vec (sort (distinct (mapcat :consumers es))))
                        :ranges (into (sorted-map) (mapcat :ranges es))))]
    (->> (concat merged other)
         (map #(assoc % :message (error-message %)))
         (sort-by (juxt #(get error-rank (:error %) 99) :capability (comp pr-str :consumers)))
         vec)))

(defn- instance
  [projects cap {:keys [provider provide]} id share? consumers run]
  (let [pname (compose-project-name (get projects provider))
        cp (cond
             (nil? run) nil
             share? pname
             :else (schema/private-project pname (first consumers) run))]
    (cond-> {:instance/id id
             :instance/capability cap
             :instance/provider provider
             :instance/service (:service provide)
             :instance/share? share?
             :instance/consumers (vec (sort consumers))
             :instance/aliases [(:service provide)]}
      (:version provide) (assoc :instance/version (:version provide))
      (seq cp) (assoc :instance/compose-project cp))))

(defn resolve-needs
  "Result<Resolution> for `needs` ({consumer Needs}, see `consumer-needs`)
   against `projects` ({project-id Project}). `opts`: `:run` a RunId; with it
   every instance carries `:instance/compose-project` (the provider's for a
   shared one, `schema/private-project` for a private one), which
   schema/Instance requires of a private instance.

   Ok: {:resolution/instances (sorted {id Instance})
        :resolution/bindings (sorted {consumer (sorted {cap instance-id})})}.
   Err: the first error, sorted, carrying every error under :errors; each
   names :capability, :consumers, :ranges and a :message."
  ([projects needs] (resolve-needs projects needs {}))
  ([projects needs {:keys [run]}]
   (let [rows (for [[c c-needs] needs
                    [cap need] c-needs]
                {:consumer c :capability cap
                 :range (or (:version need) "*")
                 :pin (:provider need)
                 :share? (:share? need true)})
         units (concat
                (for [[cap rs] (group-by :capability (filter :share? rows))]
                  {:capability cap :share? true :rows rs})
                (for [row (remove :share? rows)]
                  {:capability (:capability row) :share? false :rows [row]}))
         resolved (map #(assoc % :choice (resolve-unit projects (:capability %) (:rows %))) units)
         errors (mapcat (comp :errors :choice) resolved)]
     (if (seq errors)
       (let [es (merge-errors errors)]
         (r/err (:error (first es)) (assoc (first es) :errors es)))
       (let [placed (for [{:keys [capability share? rows choice]} resolved
                          :let [consumers (map :consumer rows)
                                id (if share?
                                     (instance-id (:provider choice) capability)
                                     (instance-id (:provider choice) capability (first consumers)))]]
                      {:id id
                       :rows rows
                       :instance (instance projects capability choice id share? consumers run)})]
         (r/ok {:resolution/instances (into (sorted-map) (map (juxt :id :instance)) placed)
                :resolution/bindings
                (reduce (fn [acc {:keys [id rows]}]
                          (reduce #(assoc-in %1 [(:consumer %2) (:capability %2)] id) acc rows))
                        (into (sorted-map) (map (fn [c] [c (sorted-map)])) (keys needs))
                        placed)}))))))

(defn stack-consumer
  "The consumer id a stack's own `:stack/needs` are bound to: `stack:<id>`."
  [stack]
  (str "stack:" (:stack/id stack)))

(defn resolve-stack
  "Result<Resolution> for Stack `stack` over `projects`: its consumers with
   their projects' needs, plus `:stack/needs` under `stack-consumer`, plus
   `(:extra opts)` ({consumer Needs}, e.g. suites). `(:run opts)` as in
   `resolve-needs`."
  ([projects stack] (resolve-stack projects stack {}))
  ([projects stack {:keys [extra] :as opts}]
   (r/let-ok [needs (consumer-needs projects (:stack/consumers stack)
                                    (cond-> (or extra {})
                                      (seq (:stack/needs stack))
                                      (update (stack-consumer stack) merge (:stack/needs stack))))]
     (resolve-needs projects needs opts))))
