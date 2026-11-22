(ns hive-compose.adapter.tempo
  "ITraceQuery over Grafana Tempo's HTTP API (grafana/otel-lgtm serves it on
   :3200): GET /api/search?q=<TraceQL>&start&end&limit, GET
   /api/traces/<id> and GET /ready. The JDK's java.net.http client, no other
   dependency; the answers are read by `hive-compose.promote.trace`.

   Every failure is a Result, never a throw: :trace/unreachable (connect
   failed, timed out), :trace/http-error (a non-2xx status, with :status and
   the start of the :body), :trace/not-found (404 on a trace),
   :trace/bad-json (an answer that does not parse or has a shape it cannot
   read), :trace/bad-id (a trace id that is not hex), :trace/not-ready (a
   non-2xx /ready).

   The request timeout bounds the wait for the response headers; Tempo's
   answers are small JSON, read whole."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [hive-compose.port :as port]
            [hive-compose.promote.trace :as trace]
            [hive-dsl.result :as r])
  (:import (java.net URI URLEncoder)
           (java.net.http HttpClient HttpClient$Redirect HttpRequest HttpResponse$BodyHandlers)
           (java.nio.charset StandardCharsets)
           (java.time Duration)))

;; SPDX-License-Identifier: MIT

(def defaults
  "Connection settings when none are given."
  {:base-url "http://localhost:3200"
   :connect-timeout-ms 2000
   :request-timeout-ms 10000
   :limit 20})

(defn- encode [s]
  (URLEncoder/encode (str s) StandardCharsets/UTF_8))

(defn query-string
  "`params` ({name value}) as a URL query string, names sorted."
  [params]
  (str/join "&" (for [[k v] (sort-by key params)] (str (encode k) "=" (encode v)))))

(defn url
  "`base-url` joined with `path` and `params`, one slash between them."
  [base-url path params]
  (str (str/replace (str base-url) #"/+$" "") path
       (when (seq params) (str "?" (query-string params)))))

(defn- snippet [s]
  (let [s (str s)] (if (> (count s) 500) (subs s 0 500) s)))

(defn- fetch
  "Result<{:status :body}> of GET `u` with `client`, a transport failure an
   :trace/unreachable error."
  [^HttpClient client request-timeout-ms u]
  (try
    (let [req (-> (HttpRequest/newBuilder (URI/create u))
                  (.timeout (Duration/ofMillis request-timeout-ms))
                  (.header "Accept" "application/json")
                  (.GET)
                  (.build))
          resp (.send client req (HttpResponse$BodyHandlers/ofString StandardCharsets/UTF_8))]
      (r/ok {:status (.statusCode resp) :body (.body resp)}))
    (catch InterruptedException e
      (.interrupt (Thread/currentThread))
      (r/err :trace/unreachable {:url u :message (str "interrupted: " (ex-message e))}))
    (catch Exception e
      (r/err :trace/unreachable {:url u :message (or (ex-message e) (str (type e)))}))))

(defn- json-body
  "Result<parsed JSON> of a 2xx answer; a non-2xx one an :trace/http-error."
  [u {:keys [status body]}]
  (if (<= 200 status 299)
    (try (r/ok (json/read-str body))
         (catch Exception e
           (r/err :trace/bad-json {:url u :message (ex-message e) :body (snippet body)})))
    (r/err :trace/http-error {:url u :status status :body (snippet body)})))

(defn- read-answer
  "Result of `read` (0-arity, reads a parsed answer into data): a shape it
   cannot read is :trace/bad-json, never a throw."
  [u read]
  (try (r/ok (read))
       (catch Exception e
         (r/err :trace/bad-json {:url u :message (or (ex-message e) (str (type e)))}))))

(defrecord TempoQuery [client base-url request-timeout-ms limit]
  port/ITraceQuery
  (-find-traces [_ query]
    (let [u (url base-url "/api/search"
                 (trace/search-params (update query :limit #(or % limit))))]
      (r/let-ok [resp (fetch client request-timeout-ms u)
                 body (json-body u resp)]
        (read-answer u #(trace/search-result body)))))
  (-trace [_ trace-id]
    (if-not (trace/hex-id? trace-id)
      (r/err :trace/bad-id {:trace-id trace-id})
      (let [id (trace/normalize-id trace-id 32)
            u (url base-url (str "/api/traces/" id) {})]
        (r/let-ok [resp (fetch client request-timeout-ms u)
                   body (if (= 404 (:status resp))
                          (r/err :trace/not-found {:trace-id id :url u})
                          (json-body u resp))]
          (read-answer u #(trace/trace-result id body))))))
  (-health [_]
    (let [u (url base-url "/ready" {})]
      (r/let-ok [{:keys [status body]} (fetch client request-timeout-ms u)]
        (if (<= 200 status 299)
          (r/ok {:ready? true})
          (r/err :trace/not-ready {:url u :status status :body (snippet body)}))))))

(defn make-query
  "TempoQuery. `opts` (merged over `defaults`): :base-url, :connect-timeout-ms,
   :request-timeout-ms, :limit (the search limit when a query names none)."
  ([] (make-query {}))
  ([opts]
   (let [{:keys [base-url connect-timeout-ms request-timeout-ms limit]} (merge defaults opts)
         client (-> (HttpClient/newBuilder)
                    (.connectTimeout (Duration/ofMillis connect-timeout-ms))
                    (.followRedirects HttpClient$Redirect/NORMAL)
                    (.build))]
     (->TempoQuery client base-url request-timeout-ms limit))))
