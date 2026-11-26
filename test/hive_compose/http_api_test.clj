(ns hive-compose.http-api-test
  "The control API end to end (card hc-14): the addon serves it on an
   ephemeral loopback port when :compose/api enables it, over the stub
   engine, and stops it on shutdown."
  (:require [clojure.data.json :as json]
            [clojure.test :refer [deftest is testing]]
            [hive-addon.protocol :as proto]
            [hive-compose.adapter.http :as http]
            [hive-compose.addon :as addon]
            [hive-compose.stub :as stub]
            [hive-dsl.result :as r])
  (:import (java.net URI)
           (java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers HttpResponse$BodyHandlers)))

;; SPDX-License-Identifier: MIT

(def model
  {"envoy" {:deps ["auth-service"]} "auth-service" {} "escala" {:deps ["envoy"]} "inventory" {}})

(defn- holder
  "A section dc/envoy claimed by ling-gu05, as the state file keeps it."
  []
  {:profile/id "dc/envoy" :project "dc" :services ["auth-service" "envoy"]
   :profile {:profile/id "dc/envoy" :profile/dir "/w/dc" :profile/project "dc" :profile/files ["a.yml"]
             :profile/env {"GW" "172.25.0.9"} :profile/services ["envoy"]}
   :started-at 0 :last-touch 0 :claims {"ling-gu05" {:since 0 :seen 0}}})

(defn- with-addon
  "Run (f addon port) on an initialized addon serving the API, then shut down."
  [api-cfg f]
  (let [state-file (str (System/getProperty "java.io.tmpdir") "/hc-api-" (System/nanoTime) ".edn")
        _ (spit state-file (pr-str {:active {"dc/envoy" (holder)} :current "dc/envoy"}))
        e (stub/engine {} {"dc" #{"auth-service" "envoy"}} {"dc" model})
        a (addon/make-addon {:compose/engine e :compose/process-table nil :compose/runner nil
                             :compose/state-file state-file :compose/config-source (constantly {})
                             :compose/profiles-file "/nonexistent/profiles.edn"
                             :compose/projects [{:project/id "dc" :project/dir "/w/dc" :project/files ["a.yml"]
                                                 :project/env {"GW" "172.25.0.9"}}]
                             :compose/swarm? false :compose/notify? false :compose/adopt? false
                             :compose/initial-delay-ms 3600000
                             :compose/api api-cfg})]
    (try
      (is (:success? (proto/initialize! a {})))
      (f a (get-in @(:state a) [:api :port]))
      (finally
        (proto/shutdown! a)
        (.delete (java.io.File. state-file))))))

(def ^:private client (delay (HttpClient/newHttpClient)))

(defn- request
  ([port method path body] (request port method path body {"Content-Type" "application/json"}))
  ([port method path body headers]
   (let [b (HttpRequest/newBuilder (URI. (str "http://127.0.0.1:" port path)))
         _ (doseq [[k v] headers] (.header b k v))
         rq (.build (if (= "GET" method)
                      (.GET b)
                      (.POST b (HttpRequest$BodyPublishers/ofString (or body "")))))
         rs (.send ^HttpClient @client rq (HttpResponse$BodyHandlers/ofString))]
     {:status (.statusCode rs)
      :type (first (.allValues (.headers rs) "content-type"))
      :text (.body rs)})))

(defn- post [port cmd body]
  (let [res (request port "POST" (str "/v1/" cmd) (json/write-str body))]
    (assoc res :body (json/read-str (:text res)))))

(deftest round-trip-on-an-ephemeral-port
  (with-addon {:enabled? true :port 0}
    (fn [_a port]
      (is (pos-int? port))
      (testing "status answers the envelope and the bound port"
        (let [{:keys [status body]} (post port "status" {})]
          (is (= 200 status))
          (is (true? (get body "ok")))
          (is (= port (get-in body ["value" "api" "port"])))
          (is (= "127.0.0.1" (get-in body ["value" "api" "host"])))))
      (testing "targets"
        (let [{:keys [status body]} (post port "targets" {})]
          (is (= 200 status))
          (is (= "dc" (get-in body ["value" 0 "project"])))))
      (testing "an unknown command is 404 with the qualified category"
        (let [{:keys [status body]} (post port "bogus" {})]
          (is (= 404 status))
          (is (= {"ok" false "error" "compose/unknown-command"} (select-keys body ["ok" "error"])))))
      (testing "a refusal is 409 naming the holder"
        (let [{:keys [status body]} (post port "up" {"target" "dc/escala" "env" {"GW" "192.168.2.106"}})]
          (is (= 409 status))
          (is (= "compose/held-by-another-section" (get body "error")))
          (is (= "dc/envoy" (get-in body ["holders" 0 "section"])))))
      (testing "the same input shares and runs"
        (is (= 200 (:status (post port "up" {"target" "dc/inventory"})))))
      (testing "a missing param is 400, an unknown target 404, an unknown test param 400"
        (is (= 400 (:status (post port "ps" {}))))
        (is (= 404 (:status (post port "ps" {"target" "nope/x"}))))
        (is (= 400 (:status (post port "test" {"suitez" "x"})))))
      (testing "an unknown param is 400"
        (is (= 400 (:status (post port "ps" {"target" "dc/envoy" "tial" 5})))))
      (testing "a param of the wrong type is 400, not a throw"
        (is (= 400 (:status (post port "logs" {"target" "dc/envoy" "tail" "many"})))))
      (testing "browser traffic is refused over the wire too"
        (is (= 415 (:status (request port "POST" "/v1/status" "{}" {"Content-Type" "text/plain"}))))
        (is (= 403 (:status (request port "POST" "/v1/status" "{}"
                                     {"Content-Type" "application/json" "Origin" "https://evil.example"})))))
      (testing "the two reads"
        (let [h (request port "GET" "/v1/health" nil)]
          (is (= 200 (:status h)))
          (is (= "ok" (get-in (json/read-str (:text h)) ["value" "status"]))))
        (let [s (request port "GET" "/v1/openapi.yaml" nil)]
          (is (= 200 (:status s)))
          (is (re-find #"yaml" (:type s)))
          (is (re-find #"(?m)^openapi: 3\.1\.0" (:text s)))))))
  (testing "MCP text keeps the bare category"
    (with-addon {:enabled? false}
      (fn [a port]
        (is (nil? port) "off: nothing is bound")
        (let [res ((:handler (addon/tool a)) {"command" "bogus"})]
          (is (:isError res))
          (is (= "unknown-command" (get (json/read-str (get-in res [:content 0 :text])) "error"))))))))

(deftest a-bad-api-config-never-fails-the-addon
  (doseq [bad [{:port 70000} {:port "7781"} {:threads 0} {:threads "4"}]]
    (is (= :compose/invalid-config (:error (http/start! (merge {:host "127.0.0.1" :port 0 :threads 1} bad)
                                                        (fn [_] {:status 200 :body ""}))))
        (pr-str bad)))
  (with-addon {:enabled? true :port 70000}
    (fn [a port]
      (is (nil? port))
      (is (= :compose/invalid-config (get-in @(:state a) [:api :error])))
      (is (= :active (:lifecycle @(:state a))) "the addon runs; only the API is down"))))

(deftest shutdown-stops-the-server
  (let [seen (atom nil)]
    (with-addon {:enabled? true}
      (fn [_ port] (reset! seen port)))
    (is (thrown? java.net.ConnectException (request @seen "POST" "/v1/status" "{}")))))

(deftest a-non-loopback-host-is-refused
  (is (= :compose/invalid-config
         (:error (http/start! {:host "8.8.8.8" :port 0 :threads 1} (fn [_] {:status 200 :body ""})))))
  (testing "the addon still starts and status says why the API is not up"
    (with-addon {:enabled? true :host "8.8.8.8"}
      (fn [a port]
        (is (nil? port))
        (is (= :compose/invalid-config (get-in @(:state a) [:api :error])))
        (is (r/ok? (addon/dispatch a "status" {})))))))
