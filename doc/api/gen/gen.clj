(ns gen
  "Scratch generator (hc-14 draft): JSON Schema components from the malli
   value objects in hive-compose.schema, via malli.json-schema, plus the MCP
   tool's own inputSchema and command-help read from the live vars.

   Run against a checkout (read-only; nothing is written into it):
     clojure -Sdeps '{:paths [\".\"] :deps {io.github.hive-agi/hive-compose {:local/root \"<checkout>\"}}}' \\
             -M -m gen <out-prefix> [extra schema var names..]"
  (:require [malli.json-schema :as mjs]
            [hive-compose.schema]
            [clojure.data.json :as json]))

(def names '[ProfileId IdleAction SecretRef EnvValue Env Program Running Activity
             Profile Claim Usage Active ActiveMap Project Settings Release
             ;; stack value objects (hc-2, merged)
             CapabilityId Version VersionRange RunId ConsumerId Resources Provide Need
             Provides Needs Stack Instance Resolution Wiring ReportFormat Suite])

(defn- write [file x]
  ;; malli 0.20 leaves :re patterns as java.util.regex.Pattern: render as source.
  (spit file (json/write-str x :escape-slash false
                             :value-fn (fn [_ v] (cond (instance? java.util.regex.Pattern v) (str v)
                                                       (fn? v) "<fn>"
                                                       :else v)))))

(defn -main [prefix & extra]
  (let [want (into names (map symbol) extra)
        out (into (sorted-map)
                  (for [n want
                        :let [v (ns-resolve 'hive-compose.schema n)]
                        :when v]
                    [(str n) (mjs/transform @v)]))]
    (write (str prefix "components.generated.json") out)
    (let [tool ((requiring-resolve 'hive-compose.addon/tool)
                ((requiring-resolve 'hive-compose.addon/make-addon)))]
      (write (str prefix "tool.generated.json")
             {:inputSchema (:inputSchema tool)
              :description (:description tool)
              :commands @(requiring-resolve 'hive-compose.addon/commands)
              :command-help @(requiring-resolve 'hive-compose.addon/command-help)
              ;; the params each command reads: what the API enforces and Req_<command> declares
              :command-params (into (sorted-map)
                                    (map (fn [[c ps]] [c (vec (sort ps))]))
                                    @(requiring-resolve 'hive-compose.addon/command-params))
              ;; every category the source answers: the ErrorCategory enum
              :categories (vec ((requiring-resolve 'hive-compose.api/source-categories) "../../../src"))}))
    ;; How the MCP tool's data.json write-str renders keywords, sets and
    ;; namespaced keys: the wire shape every response actually has.
    (write (str prefix "datajson-probe.json")
           {:error :compose/unknown-profile :profile/id "x" :action :stop
            :set #{:nrepl} :sources [:claim] :program/id "p"})
    (println "ok" prefix (count out) (vec (remove #(ns-resolve 'hive-compose.schema %) want)))))
