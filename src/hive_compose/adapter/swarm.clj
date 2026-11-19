(ns hive-compose.adapter.swarm
  "ISwarm over the hive host: the live agents come from the host's swarm
   registry, notices go out as hivemind messages. A notice addressed to one
   agent (:to) is sent directed, through the host's routing-verdict shout,
   so it lands in that agent's context only; with no directed channel it
   falls back to the plain shout. Everything is soft-resolved, so the
   published addon never requires hive-mcp; with no host, `-agents` answers
   :host/absent and `-notify!` an error, and the addon runs on claims and
   connections alone.

   A delivered notice answers {:channel :direct|:broadcast ..}: :direct with
   the :to it reached (and the host's :context-id, :to-unresolved when the
   host knows no such reader), :broadcast when it went out as a shout,
   :fallback? true when it was meant for one agent but no directed channel
   resolved."
  (:require [hive-addon.host :as addon-host]
            [hive-compose.port :as port]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def sender
  "The agent id notices are shouted as."
  "hive-compose")

(defn agent-row
  "{:agent/id :agent/status :agent/cwd} of a host swarm row, nil without an id."
  [row]
  (when-let [id (:slave/id row)]
    (cond-> {:agent/id (str id)}
      (:slave/status row) (assoc :agent/status (keyword (name (:slave/status row))))
      (:slave/cwd row) (assoc :agent/cwd (str (:slave/cwd row))))))

(defn- answer
  "Result of calling host fn `f` with `args`: a host Result stays one, a
   thrown host failure becomes :compose/swarm-failed."
  [f & args]
  (try
    (let [v (apply f args)]
      (if (and (map? v) (contains? v :error)) v (r/ok v)))
    (catch Throwable t
      (r/err :compose/swarm-failed {:message (ex-message t)}))))

(defn- unresolved?
  "True when `res` says the host fn behind it does not resolve."
  [res]
  (= :host/absent (:error res)))

(defn- broadcast!
  "Result of shouting `data` (no :to) as a :progress notice."
  [shout! event data fallback?]
  (r/bind (answer shout! sender :progress (assoc (dissoc data :to) :event (name event)))
          (fn [delivered?]
            (if (false? delivered?)
              (r/err :compose/notice-dropped {:event event :channel :broadcast})
              (r/ok (cond-> {:channel :broadcast}
                      fallback? (assoc :fallback? true)))))))

(defn- direct!
  "Result of sending `data` to agent `to` through the host's directed shout;
   the :host/absent error itself when that channel does not resolve."
  [send! event to data]
  (let [res (answer send! sender :progress (assoc data :event (name event) :to to))]
    (if (or (r/err? res) (unresolved? res))
      res
      (let [verdict (:ok res)]
        (if (and (map? verdict) (false? (:delivered verdict)))
          (r/err :compose/notice-dropped {:event event :channel :direct :to to
                                          :reason (:reason verdict)})
          (r/ok (cond-> {:channel :direct :to to}
                  (and (map? verdict) (:routing verdict)) (assoc :routing (:routing verdict))
                  (and (map? verdict) (:context-id verdict)) (assoc :context-id (:context-id verdict))
                  (and (map? verdict) (:to-unresolved verdict)) (assoc :to-unresolved true))))))))

(defrecord HostSwarm [list-agents shout! send!]
  port/ISwarm
  (-agents [_]
    (r/map-ok (answer list-agents) #(vec (keep agent-row %))))
  (-notify! [_ event data]
    (if-let [to (some-> (:to data) str not-empty)]
      (let [res (direct! send! event to data)]
        (if (unresolved? res)
          (broadcast! shout! event data true)
          res))
      (broadcast! shout! event data false))))

(defn make-swarm
  "HostSwarm. `opts`: :list-agents (0-arity, host rows with :slave/id
   :slave/status :slave/cwd; default the host swarm registry's live agents),
   :shout! ((agent-id event-type data) -> bool; default the host hivemind
   shout) and :send! ((agent-id event-type data) -> routing verdict, data
   carrying :to; default the host's shout-with-verdict!, the directed
   channel). A :send! answering :host/absent sends directed notices as
   shouts instead."
  ([] (make-swarm {}))
  ([{:keys [list-agents shout! send!]}]
   (->HostSwarm (or list-agents (addon-host/soft 'hive-mcp.swarm.datascript.queries/get-all-slaves))
                (or shout! (addon-host/soft 'hive-mcp.hivemind.messaging/shout!))
                (or send! (addon-host/soft 'hive-mcp.hivemind.messaging/shout-with-verdict!)))))
