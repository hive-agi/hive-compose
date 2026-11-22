(ns hive-compose.openapi-contract-test
  "resources/hive_compose/openapi.yaml against the MCP tool it mirrors (card
   hc-14): one path per command and none extra apart from x-status planned
   ones and the two GET reads; request schemas accept the tool's params and
   refuse what the tool would; every category the API maps is in the spec."
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [hive-compose.addon :as addon]
            [hive-compose.api :as api])
  (:import (com.networknt.schema InputFormat SchemaRegistry SpecificationVersion)
           (tools.jackson.dataformat.yaml YAMLMapper)))

;; SPDX-License-Identifier: MIT

(def spec
  (delay (json/read-str (str (.readTree (YAMLMapper.) ^String (slurp (io/resource "hive_compose/openapi.yaml")))))))

(defn- ops
  "{segment {method op}} of every /v1 path."
  []
  (into {} (for [[path m] (get @spec "paths")]
             [(subs path 4) m])))

(deftest every-command-has-a-path-and-every-path-a-command
  (let [by-seg (ops)
        live (set (for [[seg m] by-seg
                        :let [post (get m "post")]
                        :when (and post (not= "planned" (get post "x-status")))]
                    seg))
        planned (set (for [[seg m] by-seg :when (= "planned" (get-in m ["post" "x-status"]))] seg))
        gets (set (for [[seg m] by-seg :when (get m "get")] seg))]
    (is (= (set addon/commands) live))
    (is (= (set (keys api/planned)) planned) "the API answers 501 for exactly the planned paths")
    (is (= #{"health" "openapi.yaml"} gets))
    (testing "the hive_call op of a command is the command; of a planned path, api/planned's"
      (doseq [c addon/commands]
        (is (= c (get-in by-seg [c "post" "x-hive-call-op"])) c))
      (doseq [[seg op] api/planned]
        (is (= op (get-in by-seg [seg "post" "x-hive-call-op"])) seg)))
    (testing "the command enum of the MCP tool is the same list"
      (is (= addon/commands (get-in (addon/tool (addon/make-addon)) [:inputSchema :properties "command" :enum]))))))

(deftest every-mapped-category-is-in-the-spec
  (let [enum (set (get-in @spec ["components" "schemas" "ErrorCategory" "enum"]))]
    (doseq [c (keys api/status-by-category)]
      (is (contains? enum (api/category-str c)) (str c)))
    (is (every? #(re-find #"^[a-z-]+/[a-z-]+$" %) enum) "every category is qualified")
    (testing "the enum is exactly the categories the source can answer (api/source-categories)"
      (let [in-src (api/source-categories "src")]
        (is (seq in-src))
        (is (= #{} (apply disj in-src enum)) "missing from ErrorCategory")
        (is (= #{} (apply disj enum in-src)) "in ErrorCategory but answered by no source")))))

(deftest request-params-are-the-tools-params
  (let [tool-params (set (keys (get-in (addon/tool (addon/make-addon)) [:inputSchema :properties])))]
    (doseq [c addon/commands
            p (keys (get-in @spec ["components" "schemas" (str "Req_" c) "properties"]))]
      (is (contains? (conj tool-params "agent_id") p) (str c " " p))))
  (testing "Req_<command> declares exactly addon/command-params, the set the API enforces"
    (is (= (set addon/commands) (set (keys addon/command-params))))
    (doseq [[c ps] addon/command-params]
      (is (= ps (set (keys (get-in @spec ["components" "schemas" (str "Req_" c) "properties"])))) c)
      (is (or (empty? ps) (false? (get-in @spec ["components" "schemas" (str "Req_" c) "additionalProperties"]))) c))))

(def ^:private registry (delay (SchemaRegistry/withDefaultDialect SpecificationVersion/DRAFT_2020_12)))

(defn- problems
  "Validation messages of `params` against the request schema of `command`."
  [command params]
  (let [doc {"$schema" "https://json-schema.org/draft/2020-12/schema"
             "$ref" (str "#/components/schemas/Req_" command)
             "components" (get @spec "components")}
        s (.getSchema ^SchemaRegistry @registry ^String (json/write-str doc) InputFormat/JSON)]
    (mapv str (.validate s ^String (json/write-str params) InputFormat/JSON))))

(deftest request-schemas-validate-sample-params
  (doseq [[c params] {"up" {"target" "dc/web" "no_deps" true "wait" false "allow_shared" true
                            "env" {"HOST" "1.2.3.4" "TOKEN" {"pass" "dev/token"}}}
                      "switch" {"profile" "api" "allow_orphans" true}
                      "restart" {"target" "dc/web" "recreate" true}
                      "down" {"profile" "api"} "stop" {"target" "dc/web"}
                      "touch" {"project" "dc"} "ps" {"target" "dc/web"}
                      "logs" {"target" "dc/web" "tail" 50}
                      "claim" {"target" "dc/web" "note" "hc-14" "agent" "ling-1"}
                      "release" {"target" "dc/web" "stop" true "agent_id" "ling-1"}
                      "nrepl" {"path" "/w/dc/src/a.clj" "project" "dc"}
                      "status" {} "targets" {} "help" {} "who" {} "reap" {}}]
    (is (= [] (problems c params)) c))
  (testing "what the tool would refuse is refused"
    (is (seq (problems "up" {"target" "dc/web" "env" {"TOKEN" {"pass" 1}}})) "a pass ref is a string")
    (is (seq (problems "up" {"target" "dc/web" "env" {"TOKEN" {"pass" "x" "extra" "y"}}})) "a ref is closed")
    (is (seq (problems "logs" {"target" "dc/web" "tail" "many"})))
    (is (seq (problems "ps" {"target" "dc/web" "bogus" 1})) "unknown params are named")))
