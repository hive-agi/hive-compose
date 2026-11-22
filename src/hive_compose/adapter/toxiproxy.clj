(ns hive-compose.adapter.toxiproxy
  "IToxiproxyClient over Toxiproxy's REST API (port 8474) with
   java.net.http. Requests are built as pure values {:method :path :body};
   the transport is an injected `send` (request -> Result<{:status :body}>),
   so the mapping of statuses to Results is tested without a server.

   API: GET /proxies, POST /proxies, GET /proxies/{p},
   POST /proxies/{p}/toxics, DELETE /proxies/{p}/toxics/{t}, POST /reset."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [hive-compose.port :as port]
            [hive-dsl.result :as r])
  (:import (java.net URI URLEncoder)
           (java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers
                          HttpResponse$BodyHandlers)
           (java.nio.charset StandardCharsets)
           (java.time Duration)))

;; SPDX-License-Identifier: MIT

(defn- enc [s] (str/replace (URLEncoder/encode (str s) StandardCharsets/UTF_8) "+" "%20"))

;; ---------------------------------------------------------------------------
;; Requests (pure)

(def list-request {:method "GET" :path "/proxies"})

(defn proxy-request [name] {:method "GET" :path (str "/proxies/" (enc name))})

(defn create-request [proxy] {:method "POST" :path "/proxies" :body proxy})

(defn add-toxic-request [proxy-name toxic]
  {:method "POST" :path (str "/proxies/" (enc proxy-name) "/toxics") :body toxic})

(defn remove-toxic-request [proxy-name toxic-name]
  {:method "DELETE" :path (str "/proxies/" (enc proxy-name) "/toxics/" (enc toxic-name))})

(def reset-request {:method "POST" :path "/reset"})

;; ---------------------------------------------------------------------------
;; Responses

(defn- parse [body]
  (if (str/blank? body) nil (json/read-str body)))

(defn- failure [req {:keys [status body]}]
  (r/err :compose/toxiproxy-failed {:method (:method req) :path (:path req)
                                    :status status
                                    :message (let [b (try (parse body) (catch Exception _ body))]
                                               (if (map? b) (get b "error" body) body))}))

(defn- call
  "Result<parsed body> of `req`, `ok` the statuses that succeed."
  [send req ok]
  (r/bind (send req)
          (fn [{:keys [status body] :as resp}]
            (if (contains? ok status)
              (r/try-effect* :compose/toxiproxy-unparseable (parse body))
              (failure req resp)))))

(defn- same-proxy? [a b]
  (and (= (get a "listen") (get b "listen")) (= (get a "upstream") (get b "upstream"))))

(defrecord HttpToxiproxy [send]
  port/IToxiproxyClient
  (-create-proxy! [_ proxy]
    (let [res (send (create-request proxy))]
      (r/bind res
              (fn [{:keys [status body] :as resp}]
                (cond
                  (#{200 201} status) (r/try-effect* :compose/toxiproxy-unparseable (parse body))
                  (= 409 status)
                  (r/bind (call send (proxy-request (get proxy "name")) #{200})
                          (fn [existing]
                            ;; toxiproxy reports listen as the resolved address,
                            ;; so compare upstream, and listen only by port
                            (if (and (= (get existing "upstream") (get proxy "upstream"))
                                     (= (last (str/split (str (get existing "listen")) #":"))
                                        (last (str/split (str (get proxy "listen")) #":"))))
                              (r/ok existing)
                              (r/err :compose/toxiproxy-proxy-conflict
                                     {:name (get proxy "name") :existing (select-keys existing ["listen" "upstream"])
                                      :wanted (select-keys proxy ["listen" "upstream"])}))))
                  :else (failure (create-request proxy) resp))))))
  (-add-toxic! [_ proxy-name toxic]
    (let [req (add-toxic-request proxy-name toxic)]
      (r/bind (send req)
              (fn [{:keys [status body] :as resp}]
                (cond
                  (#{200 201} status) (r/try-effect* :compose/toxiproxy-unparseable (parse body))
                  (= 409 status)
                  (r/bind (call send (remove-toxic-request proxy-name (get toxic "name")) #{200 204})
                          (fn [_] (call send req #{200 201})))
                  :else (failure req resp))))))
  (-remove-toxic! [_ proxy-name toxic-name]
    (let [req (remove-toxic-request proxy-name toxic-name)]
      (r/bind (send req)
              (fn [{:keys [status] :as resp}]
                (cond
                  (#{200 204} status) (r/ok {:removed? true})
                  (= 404 status) (r/ok {:removed? false})
                  :else (failure req resp))))))
  (-reset! [_]
    (r/map-ok (call send reset-request #{200 204}) (fn [_] {:reset true})))
  (-list [_]
    (call send list-request #{200})))

;; ---------------------------------------------------------------------------
;; Transport

(defn http-send
  "A `send` over java.net.http against `base-url` (\"http://127.0.0.1:8474\").
   A connection failure is :compose/toxiproxy-unreachable, never a throw."
  [base-url {:keys [timeout-ms] :or {timeout-ms 10000}}]
  (let [client (-> (HttpClient/newBuilder)
                   (.connectTimeout (Duration/ofMillis timeout-ms))
                   (.build))
        base (str/replace (str base-url) #"/+$" "")]
    (fn [{:keys [method path body]}]
      (try
        (let [pub (if (some? body)
                    (HttpRequest$BodyPublishers/ofString (json/write-str body))
                    (HttpRequest$BodyPublishers/noBody))
              req (-> (HttpRequest/newBuilder (URI/create (str base path)))
                      (.timeout (Duration/ofMillis timeout-ms))
                      (.header "Content-Type" "application/json")
                      (.method method pub)
                      (.build))
              resp (.send ^HttpClient client req (HttpResponse$BodyHandlers/ofString))]
          (r/ok {:status (.statusCode resp) :body (.body resp)}))
        (catch InterruptedException e
          (.interrupt (Thread/currentThread))
          (r/err :compose/toxiproxy-unreachable {:url (str base path) :message (ex-message e)}))
        (catch Exception e
          (r/err :compose/toxiproxy-unreachable {:url (str base path)
                                                 :message (or (ex-message e) (str (class e)))}))))))

(defn make-client
  "HttpToxiproxy at `base-url`. `opts`: :send (replaces the transport),
   :timeout-ms."
  ([base-url] (make-client base-url {}))
  ([base-url opts]
   (->HttpToxiproxy (or (:send opts) (http-send base-url opts)))))
