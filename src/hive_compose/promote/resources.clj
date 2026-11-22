(ns hive-compose.promote.resources
  "Pure: resource caps of a stack's instances, their budget and admission
   (hc-16a). See the Resource caps section of `hive-compose.schema`.

   Caps of one consumer's view of an instance, per key, high to low: the
   stack's `:stack/resources` for the capability, the consumer's Need
   `:resources`, the Provide's `:resources`, `:compose/default-resources`.
   Levels are merged low to high; an `:unbounded` level clears every cap
   below it (a higher level may set caps again).

   A shared instance serves several consumers, so it gets what each would
   have had alone, combined per key by the largest: `:mem` and `:cpus` are
   the maximum over the views that set them (a view with no cap has no
   opinion), `:jvm?` is true when one says so, and the instance is unbounded
   when one view is explicitly :unbounded. Hence a shared instance never
   sums above the private copies it replaces (`budget` is monotone under
   sharing).

   Admission (`admit`) refuses with :compose/over-budget (data:
   schema/OverBudget) when the capped total exceeds `:stack/budget`,
   `:compose/budget`, or, for what is not yet running, the host's
   MemAvailable minus `:compose/host-reserve`. MemAvailable comes from the
   caller (port/IHostMemory), never read here."
  (:require [clojure.string :as str]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

;; ---------------------------------------------------------------------------
;; Sizes

(def ^:private units {"" 1 "b" 1 "k" 1024 "m" (* 1024 1024) "g" (* 1024 1024 1024)
                      "t" (* 1024 1024 1024 1024)})

(defn parse-mem
  "Bytes of MemSize `v` (an int of bytes, or \"512m\", \"2g\", \"64kb\" ..),
   nil when `v` is nil or no MemSize."
  [v]
  (cond
    (int? v) (when (pos? v) v)
    (string? v) (when-let [[_ n u] (re-matches #"(?i)([0-9]+)([kmgt]?)b?" v)]
                  (let [b (* (bigint n) (get units (str/lower-case u)))]
                    (when (and (pos? b) (<= b Long/MAX_VALUE)) (long b))))
    :else nil))

(defn human-mem
  "`bytes` as a short binary size: 1073741824 => \"1g\", 1610612736 => \"1.5g\"."
  [bytes]
  (let [[u d] (or (first (filter (fn [[_ d]] (>= bytes d))
                                 [["t" (get units "t")] ["g" (get units "g")]
                                  ["m" (get units "m")] ["k" (get units "k")]]))
                  ["b" 1])
        q (/ (double bytes) d)
        s (if (== q (Math/rint q)) (str (long q)) (format "%.1f" q))]
    (str s u)))

(defn parse-meminfo
  "MemAvailable in bytes from /proc/meminfo text (`MemAvailable: 123 kB`),
   nil when absent."
  [text]
  (some->> (re-find #"(?m)^MemAvailable:\s+(\d+)\s*kB" (str text)) second parse-long (* 1024)))

;; ---------------------------------------------------------------------------
;; Caps of one consumer's view

(def ^:private level-order [:default :provide :need :stack])

(defn- merge-levels
  "InstanceCaps from `levels` ({level Resources}), merged low to high. An
   :unbounded level drops the caps below it and keeps `:jvm?`."
  [levels]
  (reduce (fn [acc lvl]
            (let [v (get levels lvl)]
              (cond
                (nil? v) acc
                (= :unbounded v) (-> acc (dissoc :mem :cpus) (assoc :unbounded? true :source :none))
                :else
                (let [mem (parse-mem (:mem v))]
                  (cond-> acc
                    mem (assoc :mem mem :source lvl :unbounded? false)
                    (:cpus v) (assoc :cpus (:cpus v) :unbounded? false)
                    (contains? v :jvm?) (assoc :jvm? (:jvm? v)))))))
          {:unbounded? false :source :none}
          level-order))

(defn- finish
  "An unbounded result carries no cap; :source is :none without a mem cap."
  [caps]
  (cond-> caps
    (:unbounded? caps) (dissoc :mem :cpus)
    (not (:mem caps)) (assoc :source :none)))

(defn consumer-caps
  "InstanceCaps consumer `consumer` alone would give the instance of `cap`
   from `provider`. `needs` is {consumer Needs} (as resolved), `stack` the
   Stack or nil."
  [settings stack needs consumer provider cap]
  (finish
   (merge-levels {:default (:compose/default-resources settings)
                  :provide (get-in settings [:compose/projects provider :project/provides cap :resources])
                  :need (get-in needs [consumer cap :resources])
                  :stack (get-in stack [:stack/resources cap])})))

(defn combine
  "The caps of an instance shared by consumers whose own views are `views`
   (non-empty): the largest cap per key over the views that set one (a view
   without a cap has no opinion); one explicitly unbounded view unbounds it
   (see the ns doc). `:jvm?` is true when one view says true, false
   when every view says false, else absent (detected from the image)."
  [views]
  (let [unbounded? (boolean (some :unbounded? views))
        capped (filter :mem views)
        by-mem (when (seq capped) (apply max-key :mem capped))
        cpus (seq (keep :cpus views))
        jvm (cond (some #(true? (:jvm? %)) views) true
                  (every? #(false? (:jvm? %)) views) false)]
    (cond-> {:unbounded? unbounded?
             :source (if (and by-mem (not unbounded?)) (:source by-mem) :none)}
      (some? jvm) (assoc :jvm? jvm)
      (and by-mem (not unbounded?)) (assoc :mem (:mem by-mem))
      (and cpus (not unbounded?)) (assoc :cpus (apply max cpus)))))

(defn instance-caps
  "InstanceCaps of Instance `inst`: `combine` over its consumers' views."
  [settings stack needs inst]
  (combine (mapv #(consumer-caps settings stack needs % (:instance/provider inst)
                                 (:instance/capability inst))
                 (:instance/consumers inst))))

;; ---------------------------------------------------------------------------
;; Budget

(defn budget
  "Budget (schema/Budget) of `resolution` for `stack` (or nil): the caps of
   each instance, the capped total (shared instances once), the total one
   private copy per consumer would have summed, what sharing saved, and the
   instances left out of the sum (unbounded or without a mem cap)."
  [settings stack needs resolution]
  (let [insts (vals (:resolution/instances resolution))
        rows (for [i insts
                   :let [consumers (:instance/consumers i)
                         views (mapv #(consumer-caps settings stack needs % (:instance/provider i)
                                                     (:instance/capability i))
                                     consumers)]]
               {:id (:instance/id i)
                :caps (combine views)
                :unshared (reduce + 0 (keep #(when-not (:unbounded? %) (:mem %)) views))})
        total (reduce + 0 (keep (comp :mem :caps) rows))
        unshared (reduce + 0 (map :unshared rows))]
    {:budget/instances (into (sorted-map) (map (juxt :id :caps)) rows)
     :budget/total-mem total
     :budget/unshared-mem (max unshared total)
     :budget/saved-mem (max 0 (- unshared total))
     :budget/uncapped (vec (sort (keep #(when-not (:mem (:caps %)) (:id %)) rows)))}))

(defn inst-mem
  "Bytes InstanceCaps `c` takes: its own mem cap plus its closure services'
   (`:closure` {service bytes}, private instances, see `with-closure`)."
  [c]
  (+ (or (:mem c) 0) (reduce + 0 (vals (:closure c)))))

(defn with-closure
  "Budget `b` with the depends_on closure of every PRIVATE instance of
   StackPlan `sp` (`:instance/needs` beyond `:instance/services`) counted:
   each closure service at its own compose mem_limit (`facts` {instance-id
   {service {:mem-limit ..}}}) or, when it has none, at
   :compose/default-resources :mem; a service with neither stays uncounted.
   They land under the instance's `:closure` and in the totals. An unbounded
   instance gets none. A shared instance's closure runs from its provider's
   own files and is left out."
  [b settings sp facts]
  (let [d (:compose/default-resources settings)
        dflt (when (map? d) (parse-mem (:mem d)))
        extra (into {}
                    (for [[iid ip] (:stack/instances sp)
                          :when (not (:instance/share? ip))
                          :let [c (get-in b [:budget/instances iid])
                                svcs (set (:instance/services ip))
                                cl (into (sorted-map)
                                         (keep (fn [s]
                                                 (when-let [m (or (parse-mem (get-in facts [iid s :mem-limit])) dflt)]
                                                   [s m])))
                                         (remove svcs (distinct (:instance/needs ip))))]
                          :when (and c (not (:unbounded? c)) (seq cl))]
                      [iid cl]))
        add (reduce + 0 (mapcat vals (vals extra)))]
    (-> b
        (update :budget/instances #(reduce-kv (fn [m iid cl] (assoc-in m [iid :closure] cl)) % extra))
        (update :budget/total-mem + add)
        (update :budget/unshared-mem + add))))

;; ---------------------------------------------------------------------------
;; Admission

(defn limits
  "The memory limits a plan is admitted against, in bytes: `:stack` from
   `:stack/budget`, `:budget` from `:compose/budget`, and `:host` the host's
   `available` (MemAvailable, nil when unknown) minus `:compose/host-reserve`
   (0 when unset), floored at 0. Absent limits are left out."
  [settings stack available]
  (let [reserve (or (parse-mem (:compose/host-reserve settings)) 0)
        s (parse-mem (:stack/budget stack))
        b (parse-mem (:compose/budget settings))]
    (cond-> {}
      s (assoc :stack s)
      b (assoc :budget b)
      (some? available) (assoc :host (max 0 (- available reserve))
                               :available available
                               :reserve reserve))))

(defn- over-message
  [basis limit total]
  (str (if (= :budget basis) "the capped memory of every live stack plus this one " "the stack's capped memory ")
       (human-mem total)
       (case basis
         :stack " exceeds its :stack/budget "
         :budget " exceeds :compose/budget "
         :host " (what is not yet running) exceeds the host's MemAvailable minus :compose/host-reserve, ")
       (human-mem limit) " by " (human-mem (- total limit))
       "; lower caps, share more, or stop another stack"))

(defn admit
  "Result<Budget>: `b` (from `budget`) when its capped total fits every one
   of `lims` (from `limits`), else :compose/over-budget with
   schema/OverBudget data for the first limit it exceeds, in the order
   :stack, :budget, :host. :stack compares this stack's total; :budget is
   host-wide: `others-mem` (the capped memory of every other live run's
   instances this stack does not share, each counted once) plus this total.
   The host limit is compared with the memory of the instances not in
   `running` (ids already up take no new memory)."
  ([b lims] (admit b lims #{} 0))
  ([b lims running] (admit b lims running 0))
  ([b lims running others-mem]
   (let [total (:budget/total-mem b)
         running-mem (reduce + 0 (keep (fn [[id c]] (when (contains? (set running) id) (inst-mem c)))
                                       (:budget/instances b)))
         demand {:stack total :budget (+ total (or others-mem 0)) :host (- total running-mem)}
         [basis limit] (first (filter (fn [[k lim]] (> (get demand k) lim))
                                      (keep (fn [k] (when-let [l (get lims k)] [k l]))
                                            [:stack :budget :host])))]
     (if-not basis
       (r/ok b)
       (let [t (get demand basis)]
         (r/err :compose/over-budget
                (cond-> {:basis basis
                         :limit limit
                         :total-mem t
                         :over (- t limit)
                         :instances (->> (:budget/instances b)
                                         (keep (fn [[id c]] (when (pos? (inst-mem c)) {:instance id :mem (inst-mem c)})))
                                         (sort-by (juxt (comp - :mem) :instance))
                                         vec)
                         :message (over-message basis limit t)}
                  (= :budget basis) (assoc :others-mem (or others-mem 0))
                  (= :host basis) (assoc :available (:available lims)
                                         :reserve (:reserve lims)
                                         :running-mem running-mem))))))))

;; ---------------------------------------------------------------------------
;; Reaper pressure guard

(defn under-pressure?
  "True when the host's `available` bytes (nil: unknown, never pressure) is
   below `:compose/host-reserve` (unset: no guard)."
  [settings available]
  (let [reserve (parse-mem (:compose/host-reserve settings))]
    (boolean (and reserve (some? available) (< available reserve)))))

(defn freed-mem
  "{:bytes n :unknown? bool}: what taking run `rid` of `runs` ({run {:plan
   StackPlan :budget Budget}}) down frees: the capped memory (closure
   included) of its instances no other run binds; a shared instance another
   run holds stays up. :unknown? when one of the released instances has no
   mem cap (or the run recorded no budget), so it may free memory nobody
   counted."
  [runs rid]
  (let [e (get runs rid)
        others (set (for [[r o] runs :when (not= r rid)
                          [iid ip] (get-in o [:plan :stack/instances])
                          :when (:instance/share? ip)]
                      iid))
        released (for [[iid _] (get-in e [:plan :stack/instances]) :when (not (others iid))]
                   (get-in e [:budget :budget/instances iid]))]
    {:bytes (reduce + 0 (map inst-mem (remove nil? released)))
     :unknown? (boolean (or (nil? (:budget e))
                            (some #(or (nil? %) (nil? (:mem %))) released)))}))

(defn pressure-victim
  "The run the pressure guard stops, nil when none qualifies. A candidate of
   `runs` ({run {:stack/id :claims {agent {:seen ms}} :last-touch ms
   :idle-action .. :plan :budget}}) is IDLE: no claim seen within
   `claim-ttl-ms` of `now`, untouched for at least `idle-ms`, and not
   `:idle-action :none`; and taking it down frees memory (`freed-mem`: some
   bytes, or an uncapped release). Among them the one whose stack has the
   lowest `:stack/priority` (default 0), then the longest idle, then the
   smallest run id."
  [settings runs now {:keys [claim-ttl-ms idle-ms]}]
  (let [prio #(get-in settings [:compose/stacks % :stack/priority] 0)
        live? (fn [e] (some #(< (- now (:seen % 0)) claim-ttl-ms) (vals (:claims e))))
        frees? (fn [run] (let [{:keys [bytes unknown?]} (freed-mem runs run)] (or unknown? (pos? bytes))))]
    (->> runs
         (remove (fn [[_ e]] (or (live? e)
                                 (= :none (:idle-action e))
                                 (< (- now (:last-touch e 0)) (or idle-ms 0)))))
         (filter (comp frees? key))
         (sort-by (fn [[run e]] [(prio (:stack/id e)) (:last-touch e 0) run]))
         ffirst)))

;; ---------------------------------------------------------------------------
;; Compose rendering and the JVM heap flag

(def jvm-flag "-XX:MaxRAMPercentage=75")

(def java-tool-options "JAVA_TOOL_OPTIONS")

(defn java-image?
  "True when `image` names a JVM image (openjdk, eclipse-temurin, corretto,
   clojure, maven, gradle, semeru, zulu, graalvm, liberica, or a jdk/jre tag)."
  [image]
  (boolean
   (and (string? image)
        (re-find #"(?i)(?:^|[/:_.-])(?:openjdk|eclipse-temurin|temurin|amazoncorretto|corretto|clojure|maven|gradle|ibm-semeru-runtimes|semeru|sapmachine|zulu|graalvm|liberica|jdk|jre)(?:\d+)?(?:$|[/:_.-])"
                 image))))

(defn jvm?
  "Whether the JVM flag goes into a service with `caps` running `image`: an
   explicit `:jvm?` decides; else the image is detected."
  [caps image]
  (if (contains? caps :jvm?)
    (boolean (:jvm? caps))
    (java-image? image)))

(defn- flag-key
  "What a JVM flag sets: -XX:Name=v, -XX:+Name and -XX:-Name all set
   -XX:Name; -Dk=v sets -Dk; -Xmx512m sets -Xmx (likewise -Xms -Xss)."
  [flag]
  (cond
    (str/starts-with? flag "-XX:") (let [body (subs flag 4)
                                         body (if (#{\+ \-} (first body)) (subs body 1) body)]
                                     (str "-XX:" (first (str/split body #"=" 2))))
    (str/starts-with? flag "-D") (first (str/split flag #"=" 2))
    :else (or (second (re-matches #"(-X(?:mx|ms|ss|mn)).*" flag))
              (first (str/split flag #"=" 2)))))

(defn merge-jvm-options
  "JAVA_TOOL_OPTIONS of `defaults` (flag strings) merged under `own` (the
   service's own value, nil when unset): per flag, `own` wins, its order
   kept, defaults it does not set appended first."
  [defaults own]
  (let [own-flags (remove str/blank? (str/split (or own "") #"\s+"))
        taken (set (map flag-key own-flags))]
    (str/join " " (concat (remove #(taken (flag-key %)) defaults) own-flags))))

(defn service-override
  "The compose override entries of one capped service: \"mem_limit\" (bytes)
   and \"cpus\" from `caps`, and, when `jvm?` AND a mem_limit is written,
   \"environment\" {JAVA_TOOL_OPTIONS ..} merged with the service's own value
   `own-opts`. Without a mem_limit the JVM keeps its default heap (25% of
   the host): 75% of an uncapped host would be worse. {} when nothing applies."
  [caps jvm? own-opts]
  (let [mem? (and (:mem caps) (not (:unbounded? caps)))]
    (cond-> {}
      mem? (assoc "mem_limit" (:mem caps))
      (and (:cpus caps) (not (:unbounded? caps))) (assoc "cpus" (:cpus caps))
      (and jvm? mem?) (assoc "environment" {java-tool-options (merge-jvm-options [jvm-flag] own-opts)}))))

(defn parse-xmx
  "Bytes of the last -Xmx in JVM options `opts`, nil when none."
  [opts]
  (some-> (last (re-seq #"-Xmx([0-9]+[kKmMgGtT]?)" (str opts))) second parse-mem))

(defn xmx-warnings
  "Lines warning of a service whose own -Xmx exceeds the mem_limit it gets:
   `b` a Budget, `facts` {instance-id {service {:java-tool-options}}}."
  [b facts]
  (vec (for [[iid fs] (sort-by key facts)
             :let [c (get-in b [:budget/instances iid])]
             [svc f] (sort-by key fs)
             :let [cap (or (get-in c [:closure svc]) (:mem c) (parse-mem (:mem-limit f)))
                   xmx (parse-xmx (:java-tool-options f))]
             :when (and cap xmx (> xmx cap))]
         (str "WARN " iid " " svc ": its own -Xmx " (human-mem xmx)
              " exceeds its mem_limit " (human-mem cap) " (it will be OOM-killed under load)"))))

;; ---------------------------------------------------------------------------
;; Rendering for `compose plan`

(defn caps-line
  "One line of `compose plan` for instance `id` with `caps`."
  [id caps]
  (str id "  "
       (cond
         (:unbounded? caps) "unbounded"
         (:mem caps) (str "mem " (human-mem (:mem caps)) " (" (name (:source caps)) ")")
         :else "no mem cap")
       (when (seq (:closure caps))
         (str ", closure " (str/join " " (map (fn [[s m]] (str s "=" (human-mem m))) (:closure caps)))))
       (when (:cpus caps) (str ", cpus " (:cpus caps)))
       (when (true? (:jvm? caps)) ", jvm")))

(defn summary
  "The budget block of `compose plan`: one line per instance, then the total
   with what sharing saved, then the uncapped instances."
  [b]
  (str/join "\n"
            (concat (map (fn [[id c]] (caps-line id c)) (:budget/instances b))
                    [(str "total capped memory " (human-mem (:budget/total-mem b))
                          (when (pos? (:budget/saved-mem b))
                            (str " (sharing saves " (human-mem (:budget/saved-mem b))
                                 " against " (human-mem (:budget/unshared-mem b)) " unshared)")))]
                    (when (seq (:budget/uncapped b))
                      [(str "not in the total (no mem cap): " (str/join ", " (:budget/uncapped b)))]))))
