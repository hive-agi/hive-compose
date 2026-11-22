(ns hive-compose.api-test
  "The control API's pure request/response layer (card hc-14): envelope,
   qualified categories, HTTP status, routing. A dispatch fn stands in for
   the addon."
  (:require [clojure.data.json :as json]
            [clojure.test :refer [deftest is testing]]
            [hive-compose.api :as api]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- deps [dispatch]
  {:dispatch dispatch :commands ["help" "status" "up"]
   :health (constantly {:status :ok}) :spec (constantly "openapi: 3.1.0\n")})

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
                      :compose/held-by-another-section 409 :compose/orphans-hold-resources 409
                      :compose/not-active 409 :compose/command-failed 502
                      :compose/not-initialized 503 :compose/invalid-config 503}]
    (is (= code (api/http-status (r/err cat {}))) (str cat)))
  (is (= 200 (api/http-status (r/ok nil))))
  (is (= 500 (api/http-status (r/err :compose/release-threw {})))))

(deftest post-runs-the-dispatch-with-the-body-as-params
  (let [seen (atom nil)
        d (deps (fn [c p] (reset! seen [c p]) (r/ok {:ran c})))
        res (api/route d "POST" "/v1/up" "{\"profile\":\"api\",\"command\":\"ignored\",\"wait\":true}")]
    (is (= 200 (:status res)))
    (is (= {"ok" true "value" {"ran" "up"}} (body res)))
    (is (= ["up" {"profile" "api" "wait" true}] @seen) "command comes from the path, never the body"))
  (testing "a blank body is no params"
    (let [seen (atom nil)]
      (api/route (deps (fn [c p] (reset! seen p) (r/ok {}))) "POST" "/v1/status" "")
      (is (= {} @seen)))))

(deftest refusals-and-unknowns-never-reach-dispatch
  (let [d (deps (fn [_ _] (throw (ex-info "must not run" {}))))]
    (is (= 404 (:status (api/route d "POST" "/v1/bogus" "{}"))))
    (is (= "compose/unknown-command" (get (body (api/route d "POST" "/v1/bogus" "{}")) "error")))
    (is (= 501 (:status (api/route d "POST" "/v1/stack/up" "{}"))))
    (is (= "stack.up" (get (body (api/route d "POST" "/v1/stack/up" "{}")) "op")))
    (is (= 400 (:status (api/route d "POST" "/v1/up" "[1,2]"))))
    (is (= 400 (:status (api/route d "POST" "/v1/up" "{not json"))))
    (is (= 405 (:status (api/route d "GET" "/v1/up" ""))))
    (is (= 404 (:status (api/route d "POST" "/elsewhere" ""))))
    (is (= 413 (:status (api/route d "POST" "/v1/up" nil))))))

(deftest a-throwing-dispatch-is-answered
  (let [res (api/route (deps (fn [_ _] (throw (RuntimeException. "boom")))) "POST" "/v1/status" "{}")]
    (is (= 500 (:status res)))
    (is (= "compose/dispatch-threw" (get (body res) "error")))))

(deftest the-two-reads
  (let [d (deps (fn [_ _] (r/ok {})))]
    (is (= {"ok" true "value" {"status" "ok"}} (body (api/route d "GET" "/v1/health" ""))))
    (let [res (api/route d "GET" "/v1/openapi.yaml" "")]
      (is (= 200 (:status res)))
      (is (re-find #"yaml" (get-in res [:headers "Content-Type"])))
      (is (= "openapi: 3.1.0\n" (:body res)))))
  (is (re-find #"(?m)^openapi: 3\.1\.0" (api/spec-text)) "the spec resource is on the classpath"))

(deftest config-is-additive-and-off-by-default
  (is (= {:enabled? false :host "127.0.0.1" :port 0 :threads 4} (api/config {})))
  (is (= 7781 (:port (api/config {:compose/api {:enabled? true :port 7781}}))))
  (is (= "127.0.0.1" (:host (api/config {:compose/api {:enabled? true}})))))
