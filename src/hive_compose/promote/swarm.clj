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

(defn usage
  "{:in-use? bool :by [reason]} of an active section from what one pass saw:
   `claims` still holding, `agents` working in its project's directories and
   `fresh` ({port #{peer}}) connections opened to its ports since the last
   pass. A reason is {:claim agent}, {:agent id} or {:port p :peers n}."
  [{:keys [claims agents fresh]}]
  (let [by (vec (concat (map (fn [a] {:claim a}) (sort (keys claims)))
                        (map (fn [a] {:agent (:agent/id a)}) agents)
                        (for [[p peers] (sort-by key fresh) :when (seq peers)]
                          {:port p :peers (count peers)})))]
    {:in-use? (boolean (seq by)) :by by}))

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
