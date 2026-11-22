(ns hive-compose.pipeline.faults
  "Fault injection over a stack run (hc-12): create the proxies of a faults
   plan in the run's toxiproxy, then add, remove, reset and list toxics on
   them through IToxiproxyClient.

   The run's toxiproxy is recorded by `install!` in the ctx's `:faults`
   atom, {run {:url :proxies [name]}}: in memory only, the stack ops
   install again when they re-adopt a run. The hc-6 stack ops call it after
   bringing toxiproxy up (`promote.faults/project-spec`) and connecting it
   (`promote.faults/joins`), with the host URL of its published API port.
   `:toxiproxy` in the ctx, (fn [url] IToxiproxyClient), builds the client;
   it defaults to the HTTP adapter."
  (:require [hive-compose.adapter.toxiproxy :as http]
            [hive-compose.port :as port]
            [hive-compose.promote.faults :as faults]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def ops #{"add" "remove" "reset" "list"})

(defn- client [ctx url]
  ((or (:toxiproxy ctx) http/make-client) url))

(defn install!
  "Result<{:run :url :proxies [Proxy]}>: create every proxy of faults plan
   `p` in the toxiproxy at `url` and record the run's toxiproxy. A plan that
   interposes nothing is a no-op."
  [ctx p url]
  (if-not (faults/active? p)
    (r/ok {:run (:faults/run p) :proxies []})
    (let [c (client ctx url)
          run (:faults/run p)]
      (r/let-ok [created (reduce (fn [acc pr]
                                   (let [res (port/-create-proxy! c (faults/proxy-config pr))]
                                     (if (r/err? res)
                                       (reduced (assoc res :proxy (:proxy/name pr)))
                                       (r/map-ok acc #(conj % (:ok res))))))
                                 (r/ok [])
                                 (:faults/proxies p))]
        (swap! (:faults ctx) assoc run
               {:url url :proxies (mapv :proxy/name (:faults/proxies p))})
        (r/ok {:run run :url url :proxies created})))))

(defn forget!
  "Drop the record of `run`'s toxiproxy (its stack went down)."
  [ctx run]
  (swap! (:faults ctx) dissoc run)
  (r/ok {:run run}))

(defn teardown!
  "Result<{:run :steps [step]}>: undo plan `p` at stack-down, `dir` holding
   the compose fragment it came up from. Runs each of
   `promote.faults/teardown` through `exec-step!` ((fn [step] Result), the
   hc-6 stack ops map :down-project to `-down-project!` and :network-rm to
   `-network-rm!`), stopping at the first error, then forgets the run. A
   plan that interposes nothing runs no step."
  [ctx p dir exec-step!]
  (r/let-ok [done (reduce (fn [acc step]
                            (let [res (exec-step! step)]
                              (if (r/err? res)
                                (reduced (assoc res :step (:step step)))
                                (r/map-ok acc #(conj % (:step step))))))
                          (r/ok [])
                          (faults/teardown p dir))]
    (forget! ctx (:faults/run p))
    (r/ok {:run (:faults/run p) :steps done})))

(defn- target
  "Result<{:client :proxies}> of the run named in `req`: its recorded
   toxiproxy, or the explicit `:url`."
  [ctx {:keys [run url]}]
  (let [rec (get @(:faults ctx) run)]
    (cond
      url (r/ok {:client (client ctx url) :proxies (:proxies rec)})
      (nil? run) (r/err :compose/invalid-param {:param "run" :hint "faults needs run=<run id>"})
      (nil? rec) (r/err :compose/faults-no-toxiproxy
                        {:run run :known (vec (sort (keys @(:faults ctx))))
                         :hint "the run interposes nothing (no need :via :toxiproxy) or is not up"})
      :else (r/ok {:client (client ctx (:url rec)) :proxies (:proxies rec)}))))

(defn- proxy-param
  "Result<name> of `proxy` checked against the run's proxies when known."
  [proxy proxies]
  (cond
    (nil? proxy) (r/err :compose/invalid-param {:param "proxy" :known (vec proxies)})
    (and (seq proxies) (not (some #{proxy} proxies)))
    (r/err :compose/faults-unknown-proxy {:proxy proxy :known (vec proxies)})
    :else (r/ok proxy)))

(defn faults!
  "Result of one faults request {:run :url :op add|remove|reset|list
   :proxy :toxic {:type :attributes :stream :toxicity :name}}:
   add answers the toxic, remove {:removed?}, reset {:reset true}, list
   {name Proxy}."
  [ctx {:keys [op proxy toxic] :as req}]
  (let [op (or op "list")]
    (if-not (ops op)
      (r/err :compose/invalid-param {:param "op" :value op :known (vec (sort ops))})
      (r/let-ok [{:keys [client proxies]} (target ctx req)]
        (case op
          "list" (port/-list client)
          "reset" (port/-reset! client)
          "add" (r/let-ok [pname (proxy-param proxy proxies)
                           body (faults/toxic (or toxic {}))]
                  (port/-add-toxic! client pname body))
          "remove" (r/let-ok [pname (proxy-param proxy proxies)]
                     (let [tname (faults/toxic-name toxic)]
                       (if tname
                         (port/-remove-toxic! client pname tname)
                         (r/err :compose/invalid-param {:param "toxic"
                                                        :hint "remove needs toxic {name} or {type, stream}"})))))))))
