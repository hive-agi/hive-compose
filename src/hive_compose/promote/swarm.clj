(ns hive-compose.promote.swarm
  "Pure: who works on what a section runs, which shared nREPL drives which
   module, and whether a section is in use.

   Agents are {:agent/id :agent/status :agent/cwd}. A claim is what an agent
   declared itself, kept on the active entry under :claims
   {agent-id {:since ms :seen ms :note s :swarm? bool}}. A route is one
   program with an nREPL: {:dir :program/id :project :nrepl-port ..}."
  (:require [clojure.string :as str]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

;; ---------------------------------------------------------------------------
;; paths

(defn clean-dir
  "`dir` normalized: `.` and `..` segments resolved lexically (no filesystem
   read, links untouched) and no trailing slash (\"/\" stays itself); nil
   when blank. A configured dir like `dc/../../wt/app` and an agent's cwd
   `/wt/app/src` then compare as the same tree."
  [dir]
  (when-not (str/blank? dir)
    (str (.normalize (java.nio.file.Paths/get ^String dir (make-array String 0))))))

(defn under?
  "True when `path` is `dir` or lies below it."
  [dir path]
  (let [d (clean-dir dir) p (clean-dir path)]
    (boolean (and d p (or (= d p) (str/starts-with? p (if (= "/" d) d (str d "/"))))))))

;; ---------------------------------------------------------------------------
;; agents

(defn project-dirs
  "The directories a configured project's work happens in: its own and those
   of its programs, sorted, distinct."
  [proj]
  (->> (cons (:project/dir proj) (map :program/dir (:project/programs proj)))
       (keep clean-dir)
       distinct
       sort
       vec))

(defn agents-in
  "The agents of `agents` whose cwd lies under one of `dirs`, by id."
  [agents dirs]
  (vec (sort-by :agent/id
                (filter (fn [a] (some #(under? % (:agent/cwd a)) dirs)) agents))))

(defn concerned
  "The agents a section's notices concern: `agents` (those working in its
   project's directories) and the `claimants` (agent ids holding a claim on
   it), distinct and sorted by id. A claimant the swarm does not list stands
   as {:agent/id id}."
  [agents claimants]
  (let [by-id (into {} (map (juxt :agent/id identity)) agents)]
    (vec (sort-by :agent/id
                  (vals (reduce (fn [m id] (if (contains? m id) m (assoc m id {:agent/id id})))
                                by-id
                                (remove nil? claimants)))))))

;; ---------------------------------------------------------------------------
;; claims

(defn wave?
  "True when `claim` is a wave claim: it carries an absolute :until."
  [claim]
  (int? (:until claim)))

(defn lapsed-wave
  "True when renewing `claim` at `now` without a new `until` finds a wave
   claim whose :until has already passed: the renewal then makes it a plain
   claim from `now`, which is what a caller who leaves out ttl_minutes means.
   Without this the renewal would keep the stale :until, answer ok and be
   dropped as :expired on the next pass."
  [claim now until]
  (boolean (and (nil? until) (wave? claim) (<= (:until claim) now))))

(defn claim
  "`claims` with `agent` claiming at `now`. A renewed claim keeps its :since,
   and its note unless a new one is given. `until` (absolute ms) makes it a
   WAVE claim, live until then whatever the swarm says; a renewal with an
   `until` moves it. One without leaves a live wave claim as it was, and
   turns a wave claim whose :until has passed into a plain claim
   (`lapsed-wave`)."
  [claims agent now {:keys [note swarm? until]}]
  (update claims agent
          (fn [c] (cond-> (assoc (or c {:since now}) :seen now)
                    ;; a renewal without ttl after the wave ended: a plain claim from now
                    (lapsed-wave c now until) (dissoc :until)
                    (some? note) (assoc :note note)
                    (some? swarm?) (assoc :swarm? (boolean swarm?))
                    (some? until) (assoc :until until)))))

(defn claim-end
  "Why `claim` of `agent` no longer holds at `now`, nil while it holds. THE
   one liveness law for section and stack-run claims:
     - a wave claim (:until) holds while `now` < :until, and nothing else
       counts: not the swarm roster, not :seen. Past it: :expired;
     - any other claim ends :gone when its agent was in the swarm when it
       claimed (:swarm?) and the swarm (`live`, a set of ids; nil when no
       swarm answers or none is asked) no longer lists it, and :stale when
       not renewed for `ttl-ms`.
   Monotone in `now`: once ended, a claim stays ended at every later now."
  [agent claim live now ttl-ms]
  (let [{:keys [seen swarm? until]} claim]
    (cond
      (wave? claim) (when (>= now until) :expired)
      (and swarm? live (not (contains? live agent))) :gone
      (>= (- now (or seen 0)) ttl-ms) :stale
      :else nil)))

(defn claim-live?
  "True while `claim` of `agent` holds at `now` (see `claim-end`)."
  [agent claim live now ttl-ms]
  (nil? (claim-end agent claim live now ttl-ms)))

(defn expired-claims
  "{agent reason} of the claims in `claims` that no longer hold at `now`
   (`claim-end`): :expired, :gone or :stale."
  [claims live now ttl-ms]
  (into (sorted-map)
        (keep (fn [[agent c]] (when-let [why (claim-end agent c live now ttl-ms)] [agent why])))
        claims))

(defn expires-in-seconds
  "Seconds until `claim` ends by the clock at `now` (its :until for a wave
   claim, :seen plus `ttl-ms` otherwise), never below 0. A plain claim may
   end sooner when its agent leaves the swarm."
  [claim now ttl-ms]
  (let [end (if (wave? claim) (:until claim) (+ (or (:seen claim) 0) ttl-ms))]
    (max 0 (quot (- end now) 1000))))

(defn claim-view
  "What status, who and stack-status show of one claim: note, since, seen,
   whether it is a wave claim (with its :until) and expires-in-seconds."
  [claim now ttl-ms]
  (cond-> (assoc (select-keys claim [:note :since :seen])
                 :wave? (wave? claim)
                 :expires-in-seconds (expires-in-seconds claim now ttl-ms))
    (wave? claim) (assoc :until (:until claim))))

(defn claims-view
  "{agent claim-view} of `claims`, sorted by agent (the owner)."
  [claims now ttl-ms]
  (into (sorted-map) (map (fn [[k c]] [k (claim-view c now ttl-ms)])) claims))

(defn wave-until
  "Result<until-or-nil>: the absolute :until of a claim asked at `now` with
   `ttl-minutes` (nil when none was asked: a plain claim). Refused when it is
   not a positive integer (invalid-param), above `max-minutes`
   (claim-ttl-over-max, naming the bound: never clamped), or when the claim
   has no non-blank `note` (missing-param): a wave claim outlives its agent,
   so it must say what it holds the section for."
  [now ttl-minutes max-minutes note]
  (cond
    (nil? ttl-minutes) (r/ok nil)
    (not (and (int? ttl-minutes) (pos? ttl-minutes)))
    (r/err :compose/invalid-param {:param "ttl_minutes" :value ttl-minutes
                                   :hint "a whole number of minutes, at least 1"})
    (> ttl-minutes max-minutes)
    (r/err :compose/claim-ttl-over-max
           {:param "ttl_minutes" :value ttl-minutes :max max-minutes
            :hint (str "a wave claim lasts at most " max-minutes
                       " minutes (:compose/claim-max-ttl-minutes); claim again before it ends to move it")})
    (str/blank? note)
    (r/err :compose/missing-param {:param "note"
                                   :hint "a wave claim (ttl_minutes) needs a note saying what it holds the section for"})
    :else (r/ok (+ now (* 60000 ttl-minutes)))))

;; ---------------------------------------------------------------------------
;; usage

(defn fresh-peers
  "{port #{peer}} of `now` absent from `before`: connections opened since the
   last look. A long-lived connection (a pool, an editor left attached)
   counts once, when first seen."
  [before now]
  (into {}
        (keep (fn [[p peers]]
                (let [new (into #{} (remove (get before p #{})) peers)]
                  (when (seq new) [p new]))))
        now))

(defn peer-host
  "The address of `peer` (\"ip:port\", or \"[ip6]:port\"), nil when it has no port."
  [peer]
  (let [s (str peer)
        i (str/last-index-of s ":")]
    (when (and i (pos? i))
      (let [h (subs s 0 i)]
        (if (and (str/starts-with? h "[") (str/ends-with? h "]"))
          (subs h 1 (dec (count h)))
          h)))))

(defn without-hosts
  "{port #{peer}} of `peers` with the peers whose address is one of `hosts`
   left out, and the ports left with none dropped. The connections a
   project's own containers hold to its published ports are not its use."
  [peers hosts]
  (let [hosts (set hosts)]
    (if (empty? hosts)
      (into {} (filter (comp seq val)) peers)
      (into {}
            (keep (fn [[p ps]]
                    (let [left (into #{} (remove #(contains? hosts (peer-host %))) ps)]
                      (when (seq left) [p left]))))
            peers))))

(defn published-peers?
  "True when `fresh` ({port #{peer}}) holds a peer on a port `ports` ({port
   source}) marks :published: only then is it worth asking who the project's
   own containers are."
  [fresh ports]
  (boolean (some (fn [[p s]] (and (= :published s) (seq (get fresh p)))) ports)))

(defn section-fresh
  "The fresh peers ({port #{peer}}) of a section watching `ports` ({port
   source}): those on its ports, with the peers its own containers hold
   (addresses `own`) left out on its :published ones, where a container's
   pool to a sibling's port would otherwise look like use."
  [fresh ports own]
  (let [mine (into {} (filter (comp seq val)) (select-keys fresh (keys ports)))
        published (into [] (keep (fn [[p s]] (when (= :published s) p))) ports)]
    (merge (apply dissoc mine published)
           (without-hosts (select-keys mine published) own))))

(defn busy-services
  "{service percent} of `load` ({service percent}) above `threshold`, among
   the services of `footprint` (all of `load` when nil). A reading that is
   no number is left out."
  [load threshold footprint]
  (into (sorted-map)
        (filter (fn [[s pct]]
                  (and (number? pct) (> pct threshold)
                       (or (nil? footprint) (contains? (set footprint) s)))))
        load))

(defn usage
  "{:in-use? bool :by [reason] :sources [source]} of an active section from
   what one pass saw: `claims` still holding, `agents` working in its
   project's directories, `fresh` ({port #{peer}}) connections opened to
   its ports since the last pass, each port's source (:nrepl, :published)
   given by `sources` ({port source}), and `busy` ({service percent}) services
   found working on their own (see `busy-services`). A reason is {:claim
   agent}, {:agent id}, {:port p :peers n :source s} or {:service s :cpu
   percent :source :cpu}; :sources names, in that order and once each, what
   the reasons came from: :claim, :agent, a port's source (:port when
   `sources` does not name it), :cpu."
  [{:keys [claims agents fresh sources busy]}]
  (let [ports (for [[p peers] (sort-by key fresh) :when (seq peers)]
                (cond-> {:port p :peers (count peers)}
                  (get sources p) (assoc :source (get sources p))))
        cpu (for [[s pct] (sort-by key busy)]
              {:service s :cpu pct :source :cpu})
        by (vec (concat (map (fn [a] (cond-> {:claim a}
                                       (wave? (get claims a)) (assoc :wave? true :until (:until (get claims a)))))
                             (sort (keys claims)))
                        (map (fn [a] {:agent (:agent/id a)}) agents)
                        ports
                        cpu))]
    {:in-use? (boolean (seq by))
     :by by
     :sources (vec (distinct (concat (when (seq claims) [:claim])
                                     (when (seq agents) [:agent])
                                     (map #(or (:source %) :port) ports)
                                     (when (seq cpu) [:cpu]))))}))

;; ---------------------------------------------------------------------------
;; nREPL routes

(defn best-route
  "The route of `routes` whose :dir holds `path` most closely (the deepest
   directory), nil when none does."
  [routes path]
  (->> routes
       (filter #(under? (:dir %) path))
       (sort-by (comp - count clean-dir :dir))
       first))

(defn route-advice
  "One line telling an agent how to drive the module of `route`."
  [{:keys [dir nrepl-port nrepl-ready? running? project] :as route}]
  (cond
    (nil? route) nil
    (and running? nrepl-ready?)
    (str "drive " dir " through the shared nREPL on port " nrepl-port
         " (clojure_eval port=" nrepl-port "); do not start another JVM for it")
    running?
    (str "the program for " dir " runs but its nREPL on port " nrepl-port
         " does not answer yet; wait for it instead of starting another JVM")
    :else
    (str "no shared nREPL runs for " dir "; start it with compose claim (or up) on a "
         project " target instead of starting a private JVM")))

(defn notices
  "[{:agent id :section id :routes [route]}] for the agents of `working`
   ({section-id [agent]}) not yet told about that section (`told`, a set of
   [agent-id section-id]), when the section has a ready route to tell."
  [working routes-by-section told]
  (vec (for [[sid agents] (sort-by key working)
             :let [ready (filterv :nrepl-ready? (get routes-by-section sid))]
             :when (seq ready)
             a agents
             :when (not (contains? told [(:agent/id a) sid]))]
         {:agent (:agent/id a) :section sid :routes ready})))
