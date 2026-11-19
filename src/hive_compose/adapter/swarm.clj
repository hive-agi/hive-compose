(ns hive-compose.adapter.swarm
  "ISwarm over the hive host: the live agents come from the host's swarm
   registry, notices go out as hivemind shouts. Both are soft-resolved, so the
   published addon never requires hive-mcp; with no host, `-agents` answers
   :host/absent and `-notify!` an error, and the addon runs on claims and
   connections alone."
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

(defrecord HostSwarm [list-agents shout!]
  port/ISwarm
  (-agents [_]
    (r/map-ok (answer list-agents) #(vec (keep agent-row %))))
  (-notify! [_ event data]
    (r/bind (answer shout! sender :progress
                    (assoc data :event (name event)))
            (fn [delivered?]
              (if (false? delivered?)
                (r/err :compose/notice-dropped {:event event})
                (r/ok true))))))

(defn make-swarm
  "HostSwarm. `opts`: :list-agents (0-arity, host rows with :slave/id
   :slave/status :slave/cwd; default the host swarm registry's live agents)
   and :shout! ((agent-id event-type data); default the host hivemind)."
  ([] (make-swarm {}))
  ([{:keys [list-agents shout!]}]
   (->HostSwarm (or list-agents (addon-host/soft 'hive-mcp.swarm.datascript.queries/get-all-slaves))
                (or shout! (addon-host/soft 'hive-mcp.hivemind.messaging/shout!)))))
