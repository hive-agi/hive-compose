(ns hive-compose.api
  "The control API's request/response layer (card hc-14). Pure apart from the
   dispatch fn it is handed: (method, path, body) -> {:status :headers :body}.

   POST /v1/{command} with a JSON object body = the `compose` MCP tool's params
   minus `command`, run through the SAME dispatch the tool runs
   (`hive-compose.addon/dispatch`). The answer is the hive-cabi/v1 envelope,
   {ok:true,value} | {ok:false,error,..rest of the Result error map}, with the
   error category QUALIFIED (\"compose/held-by-another-section\"); the MCP tool
   text keeps today's bare name. GET /v1/health and GET /v1/openapi.yaml (the
   spec in resources/hive_compose/openapi.yaml) are the two reads.

   HTTP status per category (`status-by-category`), 200 for ok. The body is
   always the envelope, so a client may ignore the code, as hive_call does."
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def defaults
  "`:compose/api` config, off by default. :port 0 binds an ephemeral port
   (`status` answers the bound one); :host must be a loopback address."
  {:enabled? false :host "127.0.0.1" :port 0 :threads 4})

(defn config
  "The API config of addon config `cfg`: `defaults` under `:compose/api`."
  [cfg]
  (merge defaults (when (map? (:compose/api cfg)) (:compose/api cfg))))

(def planned
  "Path segment -> hive_call op of the paths the spec marks x-status planned
   (plan 20261122140443-09c5c225). Answered 501 until they are implemented."
  {"test" "test"})

(def status-by-category
  "HTTP status of a Result error category. Every category the source answers
   with an r/err literal is mapped, which api-test checks against src/; any
   other answers 500. 400 the request or the stack it names is invalid, 404 a
   named thing is not there, 409 it conflicts with what runs, 502 a backend
   (docker, toxiproxy, the trace store) failed, 503 the addon or a backend is
   not ready or not configured, 500 a fault of the addon itself."
  {:compose/missing-param 400
   :compose/invalid-param 400
   :compose/claim-ttl-over-max 400
   :compose/invalid-run 400
   :compose/invalid-stack 400
   :compose/invalid-version 400
   :compose/invalid-range 400
   :compose/invalid-consumer 400
   :compose/invalid-extra-envs 400
   :compose/invalid-wiring 400
   :compose/invalid-resolution 400
   :compose/inconsistent-resolution 400
   :compose/unknown-consumer 400
   :compose/unknown-instance 400
   :compose/unknown-provider 400
   :compose/unknown-via 400
   :compose/dependency-cycle 400
   :compose/name-collision 400
   :compose/consumer-name-collision 400
   :compose/alias-clash 400
   :compose/alias-shadows-service 400
   :compose/env-collision 400
   :compose/otlp-attributes-ref 400
   :compose/otlp-pair-broken 400
   :compose/faults-alias-clash 400
   :compose/faults-port-clash 400
   :compose/faults-missing-port 400
   :compose/faults-invalid-toxic 400
   :compose/faults-unknown-toxic 400
   :trace/bad-id 400
   :compose/invalid-run-id 400
   :compose/invalid-trace-id 400
   :compose/unknown-names 400
   :compose/empty-matrix 400
   :compose/invalid-data-lifecycle 400
   :compose/invalid-data-step 400
   :compose/invalid-data-state 400
   :compose/invalid-phase 400
   :compose/unknown-data-service 400
   :compose/shared-reset-without-tenancy 400
   :compose/volume-removal-refused 400
   ;; a suite's test report (JUnit / TAP) the runner left: 502 like any
   ;; other backend whose output cannot be read or is not there
   :report/malformed 502
   :report/unreadable 502
   :report/none 502
   :compose/unknown-command 404
   :compose/unknown-stack 404
   :compose/unknown-run 404
   :compose/unknown-test 404
   :compose/no-runs 404
   :compose/no-results 404
   :compose/faults-no-toxiproxy 404
   :compose/faults-unknown-proxy 404
   :trace/not-found 404
   :compose/unknown-route 404
   :compose/unknown-profile 404
   :compose/unknown-target 404
   :compose/unknown-project 404
   :compose/no-projects 404
   :compose/forbidden-origin 403
   :compose/method-not-allowed 405
   :compose/payload-too-large 413
   :compose/unsupported-media-type 415
   :compose/not-active 409
   :compose/not-claimed 409
   :compose/orphans-hold-resources 409
   :compose/held-by-another-section 409
   :compose/holders-disagree 409
   :compose/not-a-holder 400
   :compose/run-exists 409
   :compose/over-budget 409
   :compose/toxiproxy-proxy-conflict 409
   :compose/not-implemented 501
   :compose/command-failed 502
   :compose/secret-unresolved 502
   :secrets/unknown-backend 502
   :compose/no-container 502
   :compose/instance-unhealthy 502
   :compose/stack-down-incomplete 502
   :compose/toxiproxy-failed 502
   :compose/runner-failed 502
   :compose/unparseable 502
   :compose/toxiproxy-unparseable 502
   :compose/data-step-failed 502
   :compose/toxiproxy-unreachable 502
   :trace/unreachable 502
   :trace/http-error 502
   :trace/bad-json 502
   :compose/not-initialized 503
   :compose/invalid-config 503
   :compose/profiles-unreadable 503
   :compose/no-history 503
   :compose/no-compose-file 503
   :compose/program-needs-command 503
   :compose/stacks-unsupported 503
   :compose/data-exec-unsupported 503
   :compose/data-runner-unsupported 503
   :compose/no-trace-query 503
   :compose/instance-not-ready 503
   :trace/not-ready 503
   :compose/dispatch-threw 500
   :compose/internal 500
   :compose/invalid-result 500
   :compose/release-threw 500
   :compose/note-failed 500
   :compose/notice-failed 500
   :compose/notice-dropped 500
   :compose/swarm-failed 500
   :compose/orphan-check-failed 500
   :compose/ownership-check-failed 500
   :compose/proc-unreadable 500
   :compose/meminfo-unreadable 500
   :compose/api-bind-failed 500
   :compose/state-unreadable 500
   :compose/state-unwritable 500
   :compose/lock-unreadable 500
   :compose/lock-unwritable 500
   :compose/result-unreadable 500
   :compose/result-unwritable 500
   :compose/result-run-mismatch 500
   :compose/bundle-unwritable 500
   :compose/history-unreadable 500
   :compose/history-unwritable 500
   :compose/log-unwritable 500
   :compose/program-unreadable 500
   :compose/program-stop-failed 500
   :compose/override-unwritable 500
   :compose/otlp-env-failed 500})

(def ^:private category-forms
  "Where a Result category is written in source: the first arg of r/err,
   r/try-effect* and r/try-effect-throwable*, the category arg of read-edn
   (boundary/files) and of parsed (adapter/cli), and a keyword assoc'd under
   :error. One pattern per spelling, one capture group each."
  [#"\((?:r/err|r/try-effect\*|r/try-effect-throwable\*)\s+:([a-z-]+/[a-z-]+)"
   #"\(read-edn\s+\S+\s+:([a-z-]+/[a-z-]+)"
   #":error\s+:([a-z-]+/[a-z-]+)"
   #"\(parsed\s+\(run[^\n]*\n?\s*:([a-z-]+/[a-z-]+)"])

(defn source-categories
  "Sorted set of every category (ns/name strings) the .clj files under `root`
   can answer, by `category-forms`. Build and test time only: the spec's
   ErrorCategory enum (doc/api/gen) and the drift tests over
   `status-by-category` are this one scan, so they cannot disagree."
  [root]
  (into (sorted-set)
        (for [f (file-seq (io/file root))
              :when (re-find #"\.clj$" (str f))
              :let [text (slurp f)]
              re category-forms
              [_ c] (re-seq re text)]
          c)))

(defn http-status
  "200 for an ok Result, else the status of its category (500 when unmapped)."
  [res]
  (if (r/ok? res) 200 (get status-by-category (:error res) 500)))

(defn category-str
  "A category keyword as the API writes it: ns/name, or name when bare."
  [k]
  (if (qualified-keyword? k) (str (namespace k) "/" (name k)) (name k)))

(defn qualify-errors
  "`x` with every keyword under an :error key, at any depth, written as its
   qualified string, so the wire keeps the category's namespace. Every other
   key and value is left to the encoder (namespaced keys stay bare, as on the
   MCP wire)."
  [x]
  (letfn [(error-of [m]
            ;; a sorted map with string keys (status :claims) throws on a keyword lookup
            (try (get m :error) (catch ClassCastException _ nil)))]
    (walk/postwalk (fn [f] (if (and (map? f) (keyword? (error-of f))) (assoc f :error (category-str (:error f))) f)) x)))

(defn envelope
  "The hive-cabi/v1 envelope of Result `res`."
  [res]
  (if (r/ok? res)
    {:ok true :value (qualify-errors (:ok res))}
    (assoc (qualify-errors res) :ok false)))

(defn encode
  "JSON text of `x`, slashes unescaped (compose/x, not compose\\/x)."
  [x]
  (json/write-str x :escape-slash false))

(defn respond
  "{:status :headers :body} answering Result `res`."
  ([res] (respond res {}))
  ([res headers]
   {:status (http-status res)
    :headers (merge {"Content-Type" "application/json; charset=utf-8"} headers)
    :body (encode (envelope res))}))

(defn parse-params
  "Result<params>: the request body as the tool's params (string keys; a
   `command` key is dropped, the path names the command). A blank body is {}.
   Anything but a JSON object is an invalid-param."
  [^String body]
  (if (str/blank? body)
    (r/ok {})
    (let [v (try (json/read-str body) (catch Exception _ ::unreadable))]
      (if (map? v)
        (r/ok (dissoc v "command"))
        (r/err :compose/invalid-param {:param "body"
                                       :hint "the body is a JSON object of the command's params"})))))

(defn known-op
  "Result<command>: `command` when `commands` (deps) holds it, else
   not-implemented for a planned op and unknown-command for anything else.
   Checked before the body, so /v1/bogus is 404 whatever it is sent."
  [{:keys [commands]} command]
  (cond
    (contains? planned command)
    (r/err :compose/not-implemented {:op (get planned command)
                                     :hint "planned: x-status planned in GET /v1/openapi.yaml"})

    (not (some #{command} commands))
    (r/err :compose/unknown-command {:command command :known (vec commands)
                                     :hint "POST /v1/help lists what each does"})

    :else (r/ok command)))

(defn call
  "Result of `command` with `params` through `dispatch` ((command params) ->
   Result, the MCP tool's contract), after `known-op`, so a planned or unknown
   op does not depend on the addon's lifecycle. A throw is answered, never
   propagated."
  [{:keys [dispatch] :as deps} command params]
  (r/bind (known-op deps command)
          (fn [_]
            (try (dispatch command params)
                 (catch Throwable t
                   ;; class and message only: an ex-info's data could carry env values
                   (r/err :compose/dispatch-threw {:command command :class (.getName (class t))
                                                   :message (ex-message t)}))))))

(defn- type-ok?
  "Whether JSON value `v` fits JSON Schema type `t` as the tool reads it
   (booleans also as \"true\"/\"false\", see addon/flag)."
  [t v]
  (case t
    "string" (string? v)
    "integer" (int? v)
    "boolean" (or (boolean? v) (contains? #{"true" "false"} (some-> v str str/lower-case)))
    "object" (map? v)
    true))

(defn check-params
  "Result<params>: `params` when every key is one `command` reads (`known`,
   addon/command-params) and every one the tool declares (`declared`, the
   tool's inputSchema properties, string names to {:type ..}) has that JSON
   type, else an invalid-param naming the first that is not. An unknown key
   is refused rather than dropped: a typo (runn for run) on stack-down would
   otherwise take down every run of the stack. A nil `known` (no table)
   checks types only."
  ([declared params] (check-params declared nil params))
  ([declared known params]
   (let [ks (sort (keys params))]
     (if-let [k (when known (first (remove known ks)))]
       (r/err :compose/invalid-param {:param k :known (vec (sort known))
                                      :hint (str "not a param of this command")})
       (if-let [[k t] (first (for [k ks
                                   :let [v (get params k) t (get-in declared [k :type])]
                                   :when (and t (some? v) (not (type-ok? t v)))]
                               [k t]))]
         (r/err :compose/invalid-param {:param k :hint (str "expected " t)})
         (r/ok params))))))

(defn- host-part
  "The host of a Host header or an Origin URL, lower-cased, port dropped;
   \"\" when there is none."
  [s]
  (when s
    (let [s (str/lower-case (str/replace s #"^[a-z][a-z0-9+.-]*://" ""))
          s (or (first (str/split s #"/")) "")]
      (if (str/starts-with? s "[")
        (subs s 0 (inc (or (str/index-of s "]") (dec (count s)))))
        (or (first (str/split s #":")) "")))))

(defn- loopback-name?
  "Whether host `h` (from `host-part`) names this machine with no DNS lookup:
   localhost, any 127/8 literal, or a bracketed IPv6 loopback literal ([::1],
   [0:0:0:0:0:0:0:1]); every address `adapter.http/start!` may bind."
  [h]
  (boolean
   (when h
     (or (= "localhost" h)
         (re-matches #"127(\.\d{1,3}){3}" h)
         (when-let [[_ v6] (re-matches #"\[([0-9a-f:]+)\]" h)]
           ;; an IPv6 literal: parsed, never resolved
           (try (.isLoopbackAddress (java.net.InetAddress/getByName v6)) (catch Exception _ false)))))))

(defn browser-guard
  "nil when request headers `hs` (lower-cased names) may reach the API, else
   the refusal. Loopback binding does not stop a web page: it can POST a
   `text/plain` body without a preflight, and DNS rebinding lets it read the
   answer. So the Host must be a loopback name, an Origin, when sent, must be
   one too, and a POST must be `application/json` (which forces a CORS
   preflight this server never grants)."
  [method hs]
  (cond
    (not (loopback-name? (host-part (get hs "host"))))
    (r/err :compose/forbidden-origin {:hint "Host must be localhost, a 127/8 address or [::1]"})

    (and (contains? hs "origin") (not (loopback-name? (host-part (get hs "origin")))))
    (r/err :compose/forbidden-origin {:hint "requests from a web page are refused"})

    (and (= "POST" method)
         (not (str/starts-with? (str/lower-case (str (get hs "content-type"))) "application/json")))
    (r/err :compose/unsupported-media-type {:hint "send Content-Type: application/json"})))

(defn spec-text
  "The OpenAPI document served at GET /v1/openapi.yaml, nil when missing."
  []
  (some-> (io/resource "hive_compose/openapi.yaml") slurp))

(defn- route*
  [deps {:keys [method path body] hs :headers}]
  (let [seg (some->> path (re-matches #"/v1/(.+)") second)
        refused (browser-guard method hs)]
    (cond
      refused
      (respond refused)

      (nil? body)
      (respond (r/err :compose/payload-too-large {:hint "the body is limited to 1 MiB"}))

      (and (= "GET" method) (= "/v1/health" path))
      (respond (r/ok ((:health deps))))

      (and (= "GET" method) (= "/v1/openapi.yaml" path))
      (if-let [s ((:spec deps))]
        {:status 200 :headers {"Content-Type" "application/yaml; charset=utf-8"} :body s}
        (respond (r/err :compose/unknown-route {:path path :hint "the spec resource is missing"})))

      (nil? seg)
      (respond (r/err :compose/unknown-route {:path path :hint "POST /v1/{command}, GET /v1/health, GET /v1/openapi.yaml"}))

      (not= "POST" method)
      (respond (r/err :compose/method-not-allowed {:method method :allow "POST"}) {"Allow" "POST"})

      :else
      (respond (r/let-ok [_ (known-op deps seg)
                          params (parse-params body)
                          params (check-params (:declared deps) (some-> (:params deps) (get seg)) params)]
                 (call deps seg params))))))

(defn route
  "{:status :headers :body} for one request {:method :path :headers :body}
   (:headers lower-cased names; :body the request text, nil when it was too
   large). `deps`: :dispatch and :commands (see `call`), :declared (the tool's
   inputSchema properties) and :params (addon/command-params; absent: no
   unknown-param check), see `check-params`, :health (0-arity, the addon
   health map) and :spec (0-arity, the spec text). Never throws: a throw is
   answered as the envelope of compose/internal."
  [deps req]
  (try (route* deps req)
       (catch Throwable t
         (respond (r/err :compose/internal {:class (.getName (class t)) :message (ex-message t)})))))
