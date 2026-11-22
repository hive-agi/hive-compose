(ns hive-compose.api-test
  "The control API's pure request/response layer (card hc-14): envelope,
   qualified categories, HTTP status, routing, the browser guard and the
   param type check. A dispatch fn stands in for the addon."
  (:require [clojure.data.json :as json]
            [clojure.test :refer [deftest is testing]]
            [hive-compose.api :as api]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- deps [dispatch]
  {:dispatch dispatch :commands ["help" "status" "up" "logs"]
   :declared {"tail" {:type "integer"} "env" {:type "object"} "wait" {:type "boolean"} "target" {:type "string"}}
   :health (constantly {:status :ok}) :spec (constantly "openapi: 3.1.0\n")})

(def ^:private local {"host" "127.0.0.1:7781" "content-type" "application/json"})

(defn- req
  ([method path body] (req method path body local))
  ([method path body headers] {:method method :path path :body body :headers headers}))

(defn- body [res] (json/read-str (:body res)))

(deftest the-envelope-qualifies-every-error-category
  (is (= {:ok true :value {:a 1}} (api/envelope (r/ok {:a 1}))))
  (is (= {:ok false :error "compose/held-by-another-section" :profile "p"}
         (api/envelope (r/err :compose/held-by-another-section {:profile "p"}))))
  (testing "a nested :error keyword (ownership-check, a program report) is qualified too"
    (is (= {:ok true :value {:ownership-check {:error "stub/fault"}}}
           (api/envelope (r/ok {:ownership-check {:error :stub/fault}})))))
  (testing "a sorted map with string keys (status :claims) passes through"
    (is (= {:ok true :value {:claims {"ling" {:since 0}}}}
           (api/envelope (r/ok {:claims (sorted-map "ling" {:since 0})})))))
  (testing "other keywords are left to the encoder, as on the MCP wire"
    (is (= "{\"ok\":true,\"value\":{\"id\":\"a\",\"action\":\"stop\"}}"
           (api/encode (api/envelope (r/ok {:profile/id "a" :action :stop})))))))

(deftest http-status-follows-the-category
  (doseq [[cat code] {:compose/missing-param 400 :compose/invalid-param 400
                      :compose/unknown-command 404 :compose/unknown-profile 404
                      :compose/forbidden-origin 403 :compose/unsupported-media-type 415
                      :compose/held-by-another-section 409 :compose/orphans-hold-resources 409
                      :compose/not-active 409 :compose/command-failed 502
                      :compose/not-initialized 503 :compose/invalid-config 503}]
    (is (= code (api/http-status (r/err cat {}))) (str cat)))
  (is (= 200 (api/http-status (r/ok nil))))
  (is (= 500 (api/http-status (r/err :compose/release-threw {})))))

(deftest post-runs-the-dispatch-with-the-body-as-params
  (let [seen (atom nil)
        d (deps (fn [c p] (reset! seen [c p]) (r/ok {:ran c})))
        res (api/route d (req "POST" "/v1/up" "{\"target\":\"api\",\"command\":\"ignored\",\"wait\":true}"))]
    (is (= 200 (:status res)))
    (is (= {"ok" true "value" {"ran" "up"}} (body res)))
    (is (= ["up" {"target" "api" "wait" true}] @seen) "command comes from the path, never the body"))
  (testing "a blank body is no params"
    (let [seen (atom nil)]
      (api/route (deps (fn [_ p] (reset! seen p) (r/ok {}))) (req "POST" "/v1/status" ""))
      (is (= {} @seen)))))

(deftest a-param-of-the-wrong-type-is-400-before-dispatch
  (let [d (deps (fn [_ _] (throw (ex-info "must not run" {}))))]
    (doseq [b ["{\"tail\":\"many\"}" "{\"env\":\"x\"}" "{\"wait\":3}" "{\"target\":1}"]]
      (let [res (api/route d (req "POST" "/v1/logs" b))]
        (is (= 400 (:status res)) b)
        (is (= "compose/invalid-param" (get (body res) "error")) b))))
  (let [d (deps (fn [_ p] (r/ok p)))]
    (is (= 200 (:status (api/route d (req "POST" "/v1/up" "{\"wait\":\"true\",\"tail\":5,\"extra\":[1]}"))))
        "\"true\" is a boolean as on MCP; an undeclared param passes through")))

(deftest refusals-and-unknowns-never-reach-dispatch
  (let [d (deps (fn [_ _] (throw (ex-info "must not run" {}))))]
    (is (= 404 (:status (api/route d (req "POST" "/v1/bogus" "{}")))))
    (is (= "compose/unknown-command" (get (body (api/route d (req "POST" "/v1/bogus" "{}"))) "error")))
    (is (= 501 (:status (api/route d (req "POST" "/v1/stack/up" "{}")))))
    (is (= "stack.up" (get (body (api/route d (req "POST" "/v1/stack/up" "{}"))) "op")))
    (is (= 400 (:status (api/route d (req "POST" "/v1/up" "[1,2]")))))
    (is (= 400 (:status (api/route d (req "POST" "/v1/up" "{not json")))))
    (is (= 405 (:status (api/route d (req "GET" "/v1/up" "")))))
    (is (= 404 (:status (api/route d (req "POST" "/elsewhere" "")))))
    (is (= 413 (:status (api/route d (req "POST" "/v1/up" nil)))))))

(deftest browser-traffic-is-refused
  (let [ran (atom 0)
        d (deps (fn [_ _] (swap! ran inc) (r/ok {})))]
    (testing "a cross-site simple request (text/plain, no preflight) is refused"
      (is (= 415 (:status (api/route d (req "POST" "/v1/up" "{}" {"host" "127.0.0.1:1" "content-type" "text/plain"})))))
      (is (= 415 (:status (api/route d (req "POST" "/v1/up" "{}" {"host" "127.0.0.1:1"}))))))
    (testing "a page's Origin is refused, a loopback one is not"
      (is (= 403 (:status (api/route d (req "POST" "/v1/up" "{}" (assoc local "origin" "https://evil.example"))))))
      (is (= 403 (:status (api/route d (req "GET" "/v1/health" "" (assoc local "origin" "null"))))))
      (is (= 200 (:status (api/route d (req "POST" "/v1/status" "{}" (assoc local "origin" "http://localhost:3000")))))))
    (testing "DNS rebinding: a Host that is not a loopback name is refused, reads included"
      (is (= 403 (:status (api/route d (req "GET" "/v1/health" "" {"host" "evil.example:7781"})))))
      (is (= 403 (:status (api/route d (req "POST" "/v1/status" "{}" {"content-type" "application/json"}))))))
    (testing "loopback names pass"
      (doseq [h ["localhost" "localhost:9" "127.0.0.1" "127.0.0.2:9" "[::1]:9" "[0:0:0:0:0:0:0:1]:9"]]
        (is (= 200 (:status (api/route d (req "GET" "/v1/health" "" {"host" h})))) h)))
    (testing "malformed or look-alike hosts fail closed with 403, never a 500"
      (doseq [h ["/" "" "127.0.0.1.evil.example" "localhost.evil.example" "[::2]" "[fe80::1]"]]
        (is (= 403 (:status (api/route d (req "GET" "/v1/health" "" {"host" h})))) h))
      (is (= 403 (:status (api/route d (req "GET" "/v1/health" "" (assoc local "origin" "http:///")))))))
    (is (= 1 @ran) "only the loopback-origin POST reached dispatch")))

(deftest a-throw-is-answered-without-its-data
  (let [res (api/route (deps (fn [_ _] (throw (ex-info "boom" {:env {"TOKEN" "s3cret"}})))) (req "POST" "/v1/status" "{}"))]
    (is (= 500 (:status res)))
    (is (= "compose/dispatch-threw" (get (body res) "error")))
    (is (= "boom" (get (body res) "message")))
    (is (not (re-find #"s3cret" (:body res))) "ex-data never reaches the wire"))
  (testing "a throw outside dispatch (health) is still the envelope"
    (let [res (api/route (assoc (deps (fn [_ _] (r/ok {}))) :health #(throw (RuntimeException. "engine gone")))
                         (req "GET" "/v1/health" ""))]
      (is (= 500 (:status res)))
      (is (= "compose/internal" (get (body res) "error"))))))

(deftest the-two-reads
  (let [d (deps (fn [_ _] (r/ok {})))]
    (is (= {"ok" true "value" {"status" "ok"}} (body (api/route d (req "GET" "/v1/health" "")))))
    (let [res (api/route d (req "GET" "/v1/openapi.yaml" ""))]
      (is (= 200 (:status res)))
      (is (re-find #"yaml" (get-in res [:headers "Content-Type"])))
      (is (= "openapi: 3.1.0\n" (:body res)))))
  (is (re-find #"(?m)^openapi: 3\.1\.0" (api/spec-text)) "the spec resource is on the classpath"))

(deftest config-is-additive-and-off-by-default
  (is (= {:enabled? false :host "127.0.0.1" :port 0 :threads 4} (api/config {})))
  (is (= 7781 (:port (api/config {:compose/api {:enabled? true :port 7781}}))))
  (is (= "127.0.0.1" (:host (api/config {:compose/api {:enabled? true}})))))
