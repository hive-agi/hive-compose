(ns hive-compose.promote.swarm
  "Pure: who works on what a section runs, which shared nREPL drives which
   module, and whether a section is in use.

   Agents are {:agent/id :agent/status :agent/cwd}. A claim is what an agent
   declared itself, kept on the active entry under :claims
   {agent-id {:since ms :seen ms :note s :swarm? bool}}. A route is one
   program with an nREPL: {:dir :program/id :project :nrepl-port ..}."
  (:require [clojure.string :as str]))

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

(defn claim
  "`claims` with `agent` claiming at `now`. A renewed claim keeps its :since,
   and its note unless a new one is given."
  [claims agent now {:keys [note swarm?]}]
  (update claims agent
          (fn [c] (cond-> (assoc (or c {:since now}) :seen now)
                    (some? note) (assoc :note note)
                    (some? swarm?) (assoc :swarm? (boolean swarm?))))))

(defn expired-claims
  "{agent reason} of the claims in `claims` that no longer hold at `now`:
   :gone when the agent was in the swarm when it claimed and the swarm
   (`live`, a set of ids; nil when no swarm answers) no longer lists it,
   :stale when not renewed for `ttl-ms`."
  [claims live now ttl-ms]
  (into (sorted-map)
        (keep (fn [[agent {:keys [seen swarm?]}]]
                (cond
                  (and swarm? live (not (contains? live agent))) [agent :gone]
                  (>= (- now (or seen 0)) ttl-ms) [agent :stale]
                  :else nil)))
        claims))

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

(defn usage
  "{:in-use? bool :by [reason] :sources [source]} of an active section from
   what one pass saw: `claims` still holding, `agents` working in its
   project's directories and `fresh` ({port #{peer}}) connections opened to
   its ports since the last pass, each port's source (:nrepl, :published)
   given by `sources` ({port source}). A reason is {:claim agent}, {:agent id}
   or {:port p :peers n :source s}; :sources names, in that order and once
   each, what the reasons came from: :claim, :agent, or a port's source
   (:port when `sources` does not name it)."
  [{:keys [claims agents fresh sources]}]
  (let [ports (for [[p peers] (sort-by key fresh) :when (seq peers)]
                (cond-> {:port p :peers (count peers)}
                  (get sources p) (assoc :source (get sources p))))
        by (vec (concat (map (fn [a] {:claim a}) (sort (keys claims)))
                        (map (fn [a] {:agent (:agent/id a)}) agents)
                        ports))]
    {:in-use? (boolean (seq by))
     :by by
     :sources (vec (distinct (concat (when (seq claims) [:claim])
                                     (when (seq agents) [:agent])
                                     (map #(or (:source %) :port) ports))))}))

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
