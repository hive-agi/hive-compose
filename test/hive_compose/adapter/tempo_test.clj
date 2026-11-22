(ns hive-compose.adapter.tempo-test
  "TempoQuery against a local com.sun.net.httpserver.HttpServer serving the
   answers recorded from Tempo 2.8.2 (test/resources/tempo/). No docker."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [hive-compose.adapter.tempo :as tempo]
            [hive-compose.port :as port]
            [hive-compose.promote.trace :as trace]
            [hive-compose.stub-trace :as stub-trace]
            [hive-dsl.result :as r])
  (:import (com.sun.net.httpserver HttpExchange HttpHandler HttpServer)
           (java.net InetSocketAddress ServerSocket URI URLDecoder)
           (java.nio.charset StandardCharsets)))

;; SPDX-License-Identifier: MIT

(def checkout-id "00f067aa0ba902b7a1b2c3d4e5f60718")
(def cart-id "4bf92f3577b34da6a3ce929d0e0e4736")

(defn- resource [name] (slurp (io/resource (str "tempo/" name))))

(defn- query-params [^URI uri]
  (into {} (for [kv (some-> (.getRawQuery uri) (str/split #"&"))
                 :let [[k v] (str/split kv #"=" 2)]]
             [(URLDecoder/decode k StandardCharsets/UTF_8)
              (URLDecoder/decode (or v "") StandardCharsets/UTF_8)])))

(defn- fake-tempo
  "The answer [status body] of a fake Tempo to `path` with `params`, as the
   recorded one answered: search by the q it was recorded for, traces by id."
  [path params]
  (cond
    (= "/ready" path) [200 "ready"]
    (= "/api/search" path)
    (case (get params "q")
      "{ resource.hive.run.id = \"run-42\" && span.test.id = \"checkout \\\"pays\\\"\" }" [200 (resource "search-checkout.json")]
      "{ resource.hive.run.id = \"run-42\" }" [200 (resource "search-run.json")]
      "{ span.test.id = \"garbage\" }" [200 "{\"traces\": ["]
      "{ span.test.id = \"shape\" }" [200 "{\"traces\": 5}"]
      [200 (resource "search-empty.json")])
    (= (str "/api/traces/" checkout-id) path) [200 (resource "trace-checkout.json")]
    (= (str "/api/traces/" cart-id) path) [200 (resource "trace-cart.json")]
    (str/starts-with? path "/api/traces/") [404 ""]
    :else [404 "404 page not found"]))

(def ^:dynamic *base* nil)
(def ^:dynamic *seen* nil)

(defn- serve
  "Start an HttpServer on a free loopback port answering with `answer`
   ([path params] -> [status body]); requests are conj'd onto `seen`."
  [answer seen]
  (let [server (HttpServer/create (InetSocketAddress. "127.0.0.1" 0) 0)]
    (.createContext server "/"
                    (reify HttpHandler
                      (handle [_ ex]
                        (let [^HttpExchange ex ex
                              uri (.getRequestURI ex)
                              params (query-params uri)
                              _ (swap! seen conj {:path (.getPath uri) :params params
                                                  :accept (.getFirst (.getRequestHeaders ex) "Accept")})
                              [status body] (answer (.getPath uri) params)
                              bs (.getBytes ^String body StandardCharsets/UTF_8)]
                          (.sendResponseHeaders ex status (if (zero? (alength bs)) -1 (alength bs)))
                          (with-open [out (.getResponseBody ex)] (.write out bs))))))
    (.start server)
    server))

(use-fixtures :each
  (fn [t]
    (let [seen (atom [])
          server (serve fake-tempo seen)]
      (try
        (binding [*base* (str "http://127.0.0.1:" (.getPort (.getAddress server)) "/")
                  *seen* seen]
          (t))
        (finally (.stop server 0))))))

(defn- q [] (tempo/make-query {:base-url *base* :request-timeout-ms 3000}))

(deftest finds-a-tests-traces-by-traceql
  (let [res (port/-find-traces (q) {:attrs {"test.id" "checkout \"pays\"" "hive.run.id" "run-42"}
                                    :start-ms 1795367900000 :end-ms 1795367999500})]
    (is (r/ok? res))
    (is (= [{:trace/id checkout-id :trace/root-service "suite-runner" :trace/root-name "test checkout pays"
             :trace/start-ms 1795367923560 :trace/duration-ms 900}]
           (:ok res)))
    (is (= {:path "/api/search"
            :params {"q" "{ resource.hive.run.id = \"run-42\" && span.test.id = \"checkout \\\"pays\\\"\" }"
                     "start" "1795367900" "end" "1795368000" "limit" "20"}
            :accept "application/json"}
           (last @*seen*))
        "the window in seconds, the default limit")))

(deftest a-run-finds-every-trace-and-none-is-empty
  (is (= [checkout-id cart-id]
         (mapv :trace/id (:ok (port/-find-traces (q) {:attrs {"hive.run.id" "run-42"} :limit 5})))))
  (is (= "5" (get-in (last @*seen*) [:params "limit"])))
  (is (= (r/ok []) (port/-find-traces (q) {:attrs {"test.id" "nope"}}))))

(deftest reads-a-trace-and-explains-it
  (let [res (port/-trace (q) checkout-id)
        f (trace/failing-span (:ok res))]
    (is (r/ok? res))
    (is (= 5 (count (:trace/spans (:ok res)))))
    (is (= "INSERT orders" (:span/name f)))
    (is (= "duplicate key value violates unique constraint" (:span/status-message f))))
  (testing "a short or upper-case id is normalised before the request"
    (is (r/ok? (port/-trace (q) "F067AA0BA902B7A1B2C3D4E5F60718")))
    (is (= (str "/api/traces/" checkout-id) (:path (last @*seen*))))))

(deftest failures-are-results-never-throws
  (testing "a trace Tempo does not hold"
    (is (= :trace/not-found (:error (port/-trace (q) "ffffffffffffffffffffffffffffffff")))))
  (testing "an id that is not hex never reaches the wire"
    (let [n (count @*seen*)]
      (is (= :trace/bad-id (:error (port/-trace (q) "../ready"))))
      (is (= n (count @*seen*)))))
  (testing "a value that would end an unescaped literal reaches Tempo escaped"
    (is (= (r/ok []) (port/-find-traces (q) {:attrs {"span.test.id" "x }"}})))
    (is (= "{ span.test.id = \"x }\" }" (get-in (last @*seen*) [:params "q"]))))
  (testing "a non-2xx answer (the refusal Tempo gave the unescaped query)"
    (let [server (serve (fn [_ _] [400 (resource "search-bad-query.txt")]) (atom []))]
      (try
        (let [res (port/-find-traces (tempo/make-query {:base-url (str "http://127.0.0.1:" (.getPort (.getAddress server)))})
                                     {:attrs {"test.id" "t"}})]
          (is (= :trace/http-error (:error res)))
          (is (= 400 (:status res)))
          (is (str/includes? (:body res) "literal not terminated")))
        (finally (.stop server 0)))))
  (testing "an answer that is not JSON"
    (is (= :trace/bad-json (:error (port/-find-traces (q) {:attrs {"test.id" "garbage"}})))))
  (testing "JSON of a shape the reader cannot take"
    (is (= :trace/bad-json (:error (port/-find-traces (q) {:attrs {"test.id" "shape"}})))))
  (testing "nobody listening"
    (let [port (with-open [s (ServerSocket. 0)] (.getLocalPort s))
          dead (tempo/make-query {:base-url (str "http://127.0.0.1:" port) :connect-timeout-ms 500})]
      (is (= :trace/unreachable (:error (port/-find-traces dead {:attrs {}}))))
      (is (= :trace/unreachable (:error (port/-trace dead checkout-id))))
      (is (= :trace/unreachable (:error (port/-health dead))))))
  (testing "a server that answers too slowly"
    (let [server (serve (fn [_ _] (Thread/sleep 1500) [200 "ready"]) (atom []))]
      (try
        (is (= :trace/unreachable
               (:error (port/-health (tempo/make-query {:base-url (str "http://127.0.0.1:" (.getPort (.getAddress server)))
                                                        :request-timeout-ms 200})))))
        (finally (.stop server 0))))))

(deftest health
  (is (= (r/ok {:ready? true}) (port/-health (q))))
  (let [server (serve (fn [_ _] [503 "Ingester not ready"]) (atom []))]
    (try
      (is (= :trace/not-ready
             (:error (port/-health (tempo/make-query {:base-url (str "http://127.0.0.1:" (.getPort (.getAddress server)))})))))
      (finally (.stop server 0)))))

(deftest url-joins-one-slash-and-encodes
  (is (= "http://h:3200/api/search?limit=1&q=%7B+span.test.id+%3D+%22a+%26%26+b%22+%7D"
         (tempo/url "http://h:3200//" "/api/search" {"q" "{ span.test.id = \"a && b\" }" "limit" "1"})))
  (is (= "http://h:3200/ready" (tempo/url "http://h:3200" "/ready" {}))))

(deftest the-stub-answers-as-tempo-does
  (let [real (:ok (port/-trace (q) checkout-id))
        s (stub-trace/query [real])]
    (is (= (:ok (port/-trace (q) checkout-id)) (:ok (port/-trace s checkout-id))))
    (is (= [checkout-id] (mapv :trace/id (:ok (port/-find-traces s {:attrs {"test.id" "checkout \"pays\""
                                                                            "resource.hive.run.id" "run-42"}})))))
    (is (= {:trace/root-service "suite-runner" :trace/root-name "test checkout pays"
            :trace/start-ms 1795367923560 :trace/duration-ms 900}
           (dissoc (first (:ok (port/-find-traces s {:attrs {"hive.run.id" "run-42"}}))) :trace/id)))
    (is (= [] (:ok (port/-find-traces s {:attrs {"test.id" "other"}}))))
    (is (= [] (:ok (port/-find-traces s {:attrs {} :start-ms 1795367923561}))) "outside the window")
    (is (= :trace/not-found (:error (port/-trace s cart-id))))
    (stub-trace/fail! s :find-traces)
    (is (= :stub/fault (:error (port/-find-traces s {:attrs {}}))))
    (is (= #{:find-traces :trace} (set (map first (stub-trace/calls s)))))))
