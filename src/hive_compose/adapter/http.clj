(ns hive-compose.adapter.http
  "Boundary: the control API served by the JDK's com.sun.net.httpserver (no
   extra dependency). Bound to a loopback address only. The handler is
   `(fn [method path body] -> {:status :headers :body})`, body nil when the
   request body is over `max-body-bytes` (see `hive-compose.api/route`)."
  (:require [hive-dsl.result :as r])
  (:import (com.sun.net.httpserver HttpExchange HttpHandler HttpServer)
           (java.net InetAddress InetSocketAddress)
           (java.nio.charset StandardCharsets)
           (java.util.concurrent ExecutorService Executors ThreadFactory)))

;; SPDX-License-Identifier: MIT

(def max-body-bytes (* 1024 1024))

(defn- read-body
  "The request body as text, nil when over `max-body-bytes`."
  [^HttpExchange ex]
  (with-open [in (.getRequestBody ex)]
    (let [^bytes bs (.readNBytes in (int (inc max-body-bytes)))]
      (when (<= (alength bs) max-body-bytes)
        (String. bs StandardCharsets/UTF_8)))))

(defn- send! [^HttpExchange ex {:keys [status headers body]}]
  (let [^bytes bs (.getBytes (str body) StandardCharsets/UTF_8)
        h (.getResponseHeaders ex)]
    (doseq [[k v] headers] (.set h (str k) (str v)))
    (if (or (zero? (alength bs)) (= "HEAD" (.getRequestMethod ex)))
      (.sendResponseHeaders ex (int status) -1)
      (do (.sendResponseHeaders ex (int status) (alength bs))
          (with-open [out (.getResponseBody ex)] (.write out bs))))))

(defn- http-handler [handle]
  (reify HttpHandler
    (handle [_ ex]
      (try
        (send! ex (try (handle (.getRequestMethod ex) (.getPath (.getRequestURI ex)) (read-body ex))
                       (catch Throwable t
                         {:status 500 :headers {"Content-Type" "text/plain; charset=utf-8"}
                          :body (str "internal error: " (.getName (class t)))})))
        (catch Throwable _ nil)
        (finally (.close ex))))))

(defn- pool ^ExecutorService [n]
  (Executors/newFixedThreadPool
   (int (max 1 n))
   (reify ThreadFactory
     (newThread [_ runnable]
       (doto (Thread. ^Runnable runnable "hive-compose-api")
         (.setDaemon true))))))

(defn start!
  "Result<{:server :executor :host :port :url}>: `handle` served on `host`:`port`
   (0 = an ephemeral port; :port answers the bound one) by `threads` daemon
   threads. A host that is not a loopback address is refused."
  [{:keys [host port threads]} handle]
  (try
    (let [addr (InetAddress/getByName (str host))]
      (if-not (.isLoopbackAddress addr)
        (r/err :compose/invalid-config {:param ":compose/api :host" :host (str host)
                                        :hint "the control API binds a loopback address only (127.0.0.1)"})
        (let [server (HttpServer/create (InetSocketAddress. addr (int (or port 0))) 0)
              ex (pool (or threads 4))]
          (.createContext server "/" (http-handler handle))
          (.setExecutor server ex)
          (.start server)
          (let [bound (.getPort (.getAddress server))
                h (.getHostAddress addr)]
            (r/ok {:server server :executor ex :host h :port bound
                   :url (str "http://" h ":" bound "/v1/")})))))
    (catch java.io.IOException e
      (r/err :compose/api-bind-failed {:host (str host) :port port :message (ex-message e)}))))

(defn stop!
  "Stop a server `start!` answered. Idempotent; nil."
  [{:keys [^HttpServer server ^ExecutorService executor]}]
  (when server (.stop server 0))
  (when executor (.shutdownNow executor))
  nil)
