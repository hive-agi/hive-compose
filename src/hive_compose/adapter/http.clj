(ns hive-compose.adapter.http
  "Boundary: the control API served by the JDK's com.sun.net.httpserver (no
   extra dependency). Bound to a loopback address only. The handler is
   `(fn [req] -> {:status :headers :body})`, req being {:method :path :headers
   :body}: :headers lower-cased names to their first value, :body the request
   text or nil when it is over `max-body-bytes` (see `hive-compose.api/route`)."
  (:require [hive-dsl.result :as r])
  (:import (com.sun.net.httpserver HttpExchange HttpHandler HttpServer)
           (java.net InetAddress InetSocketAddress)
           (java.nio.charset StandardCharsets)
           (java.util.concurrent ExecutorService Executors ThreadFactory)
           (java.util.concurrent.atomic AtomicLong)))

;; SPDX-License-Identifier: MIT

(def max-body-bytes (* 1024 1024))

(defn- read-body
  "The request body as text, nil when over `max-body-bytes`."
  [^HttpExchange ex]
  (with-open [in (.getRequestBody ex)]
    (let [^bytes bs (.readNBytes in (int (inc max-body-bytes)))]
      (when (<= (alength bs) max-body-bytes)
        (String. bs StandardCharsets/UTF_8)))))

(defn- headers
  "{lower-case name first-value} of the request headers."
  [^HttpExchange ex]
  (into {} (for [[k vs] (.getRequestHeaders ex) :when (seq vs)]
             [(.toLowerCase (str k)) (str (first vs))])))

(defn- send! [^HttpExchange ex {:keys [status headers body]}]
  (let [^bytes bs (.getBytes (str body) StandardCharsets/UTF_8)
        h (.getResponseHeaders ex)]
    (doseq [[k v] headers] (.set h (str k) (str v)))
    (if (or (zero? (alength bs)) (= "HEAD" (.getRequestMethod ex)))
      (.sendResponseHeaders ex (int status) -1)
      (do (.sendResponseHeaders ex (int status) (alength bs))
          (with-open [out (.getResponseBody ex)] (.write out bs))))))

(def ^:private last-resort
  "What a handler that throws answers (`hive-compose.api/route` never does)."
  {:status 500 :headers {"Content-Type" "application/json; charset=utf-8"}
   :body "{\"ok\":false,\"error\":\"compose/internal\"}"})

(defn- http-handler [handle]
  (reify HttpHandler
    (handle [_ ex]
      (try
        (send! ex (try (handle {:method (.getRequestMethod ex)
                                :path (.getPath (.getRequestURI ex))
                                :headers (headers ex)
                                :body (read-body ex)})
                       (catch Throwable _ last-resort)))
        (catch Throwable _ nil)
        (finally (.close ex))))))

(defn- pool ^ExecutorService [n]
  (let [seq-no (AtomicLong.)]
    (Executors/newFixedThreadPool
     (int n)
     (reify ThreadFactory
       (newThread [_ runnable]
         (doto (Thread. ^Runnable runnable (str "hive-compose-api-" (.incrementAndGet seq-no)))
           (.setDaemon true)))))))

(defn start!
  "Result<{:server :executor :host :port :url}>: `handle` served on `host`:`port`
   (0 = an ephemeral port; :port answers the bound one) by `threads` daemon
   threads. A host that is not a loopback address, a port outside 0..65535 or
   a thread count below 1 is an invalid-config; a failed bind api-bind-failed.
   Never throws."
  [{:keys [host port threads]} handle]
  (cond
    (not (and (int? port) (<= 0 port 65535)))
    (r/err :compose/invalid-config {:param ":compose/api :port" :hint "an integer 0..65535 (0 = ephemeral)"})

    (not (and (int? threads) (pos? threads)))
    (r/err :compose/invalid-config {:param ":compose/api :threads" :hint "a positive integer"})

    :else
    (try
      (let [addr (InetAddress/getByName (str host))]
        (if-not (.isLoopbackAddress addr)
          (r/err :compose/invalid-config {:param ":compose/api :host" :host (str host)
                                          :hint "the control API binds a loopback address only (127.0.0.1)"})
          (let [server (HttpServer/create (InetSocketAddress. addr (int port)) 0)
                ex (pool threads)]
            (.createContext server "/" (http-handler handle))
            (.setExecutor server ex)
            (.start server)
            (let [bound (.getPort (.getAddress server))
                  h (.getHostAddress addr)
                  url-host (if (re-find #":" h) (str "[" h "]") h)]
              (r/ok {:server server :executor ex :host h :port bound
                     :url (str "http://" url-host ":" bound "/v1/")})))))
      (catch Exception e
        (r/err :compose/api-bind-failed {:host (str host) :port port :message (ex-message e)})))))

(defn stop!
  "Stop a server `start!` answered. Idempotent; nil."
  [{:keys [^HttpServer server ^ExecutorService executor]}]
  (when server (.stop server 0))
  (when executor (.shutdownNow executor))
  nil)
