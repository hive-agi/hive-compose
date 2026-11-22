(ns hive-compose.pipeline.faults-test
  (:require [clojure.data.json :as json]
            [clojure.test :refer [deftest is testing]]
            [hive-addon.protocol :as proto]
            [hive-compose.addon :as addon]
            [hive-compose.adapter.toxiproxy :as http]
            [hive-compose.stub]
            [hive-compose.pipeline.faults :as sut]
            [hive-compose.port :as port]
            [hive-compose.promote.faults :as faults]
            [hive-compose.stub-toxiproxy :as stub]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def settings
  {:compose/projects
   {"db" {:project/id "db" :project/dir "/w/db" :project/provides {"pg" {:service "pg" :port 5432}}}
    "a" {:project/id "a" :project/dir "/w/a" :project/needs {"pg" {:via :toxiproxy}}}}})

(def resolution
  {:resolution/instances {"db/pg" {:instance/id "db/pg" :instance/capability "pg" :instance/provider "db"
                                   :instance/service "pg" :instance/share? true
                                   :instance/consumers ["a"] :instance/aliases ["pg"]}}
   :resolution/bindings {"a" {"pg" "db/pg"}}})

(defn- ctx [client]
  {:faults (atom {}) :toxiproxy (fn [_url] client)})

(deftest install-then-drive-toxics
  (let [c (stub/client)
        x (ctx c)
        p (:ok (faults/plan settings resolution "run1"))]
    (is (= ["a.pg"] (mapv #(get % "name") (:proxies (:ok (sut/install! x p "http://127.0.0.1:1"))))))
    (is (r/ok? (sut/install! x p "http://127.0.0.1:1")) "install is idempotent")
    (testing "add, list, remove, reset"
      (is (= "latency_downstream"
             (get (:ok (sut/faults! x {:run "run1" :op "add" :proxy "a.pg"
                                       :toxic {:type "latency" :attributes {"latency" 300}}}))
                  "name")))
      (is (= ["latency_downstream"] (keys (stub/toxics c "a.pg"))))
      (is (= 1 (count (get-in (:ok (sut/faults! x {:run "run1"})) ["a.pg" "toxics"]))))
      (is (= {:removed? true} (:ok (sut/faults! x {:run "run1" :op "remove" :proxy "a.pg"
                                                    :toxic {:type "latency"}}))))
      (sut/faults! x {:run "run1" :op "add" :proxy "a.pg" :toxic {:type "reset_peer" :attributes {"timeout" 0}}})
      (is (= {:reset true} (:ok (sut/faults! x {:run "run1" :op "reset"}))))
      (is (empty? (stub/toxics c "a.pg"))))
    (testing "refusals"
      (is (= :compose/faults-unknown-proxy
             (:error (sut/faults! x {:run "run1" :op "add" :proxy "b.pg" :toxic {:type "latency"}}))))
      (is (= :compose/faults-no-toxiproxy (:error (sut/faults! x {:run "r9"}))))
      (is (= :compose/invalid-param (:error (sut/faults! x {:run "run1" :op "explode"}))))
      (is (= :compose/invalid-param (:error (sut/faults! x {:run "run1" :op "remove" :proxy "a.pg"}))))
      (is (= :compose/faults-unknown-toxic
             (:error (sut/faults! x {:run "run1" :op "add" :proxy "a.pg" :toxic {:type "boom"}})))))
    (testing "an unreachable toxiproxy is an error, never a throw"
      (stub/fail! c :list)
      (is (= :compose/toxiproxy-unreachable (:error (sut/faults! x {:run "run1"})))))
    (sut/forget! x "run1")
    (is (= :compose/faults-no-toxiproxy (:error (sut/faults! x {:run "run1"}))))))

(deftest teardown-removes-toxiproxy-then-the-faults-network
  (let [c (stub/client)
        x (ctx c)
        p (:ok (faults/plan settings resolution "run1"))
        none (:ok (faults/plan (assoc-in settings [:compose/projects "a" :project/needs] {}) resolution "run1"))
        steps (atom [])]
    (sut/install! x p "http://x")
    (is (= {:run "run1" :steps [:down-project :network-rm]}
           (:ok (sut/teardown! x p "/tmp/tp" (fn [s] (swap! steps conj s) (r/ok {}))))))
    (is (= [{:step :down-project :project "hive-toxiproxy-faults-run1" :all? true}
            {:step :network-rm :network "hive-run1-faults"}]
           (mapv #(cond-> (select-keys % [:step :network])
                    (:spec %) (merge (select-keys (:spec %) [:project :all?])))
                 @steps)))
    (is (= :compose/faults-no-toxiproxy (:error (sut/faults! x {:run "run1"}))) "the run is forgotten")
    (testing "a failing step stops the teardown and names it"
      (sut/install! x p "http://x")
      (is (= :network-rm (:step (sut/teardown! x p "/tmp/tp" (fn [s] (if (= :network-rm (:step s))
                                                               (r/err :stub/fault)
                                                               (r/ok {}))))))))
    (testing "nothing interposed, nothing to undo"
      (is (= [] (:steps (:ok (sut/teardown! x none "/tmp/tp" (fn [_] (throw (ex-info "must not run" {})))))))))))

(deftest install-of-nothing-is-a-no-op
  (let [c (stub/client)
        p (:ok (faults/plan (assoc-in settings [:compose/projects "a" :project/needs] {"pg" {}})
                            resolution "run1"))]
    (is (= [] (:proxies (:ok (sut/install! (ctx c) p "http://x")))))
    (is (= [] (stub/calls c)))))

(deftest the-compose-faults-command
  (let [dir (str (java.nio.file.Files/createTempDirectory "hc12" (make-array java.nio.file.attribute.FileAttribute 0)))
        c (stub/client)
        a (addon/make-addon {:compose/profiles [] :compose/profiles-file (str dir "/absent.edn")
                             :compose/state-file (str dir "/state.edn")
                             :compose/engine (hive-compose.stub/engine {})
                             :compose/toxiproxy (fn [_] c)
                             :compose/initial-delay-ms 3600000})
        call (fn [params]
               (let [{:keys [content isError]} ((:handler (first (proto/tools a))) params)]
                 {:error? (boolean isError) :body (json/read-str (:text (first content)))}))]
    (proto/initialize! a {})
    (try
      (is (:error? (call {"command" "faults" "run" "run1"})) "no toxiproxy recorded for the run")
      (sut/install! (:ctx @(:state a)) (:ok (faults/plan settings resolution "run1")) "http://x")
      (is (= "latency_upstream"
             (get (:body (call {"command" "faults" "run" "run1" "op" "add" "proxy" "a.pg"
                                "toxic" {"type" "latency" "stream" "upstream" "attributes" {"latency" 100}}}))
                  "name")))
      (is (contains? (get-in (:body (call {"command" "faults" "run" "run1"})) ["a.pg" "toxics"]) 0))
      (is (:error? (call {"command" "faults" "run" "run1" "op" "add" "proxy" "a.pg" "toxic" "latency"})))
      (finally (proto/shutdown! a)))))

;; ---------------------------------------------------------------------------
;; The HTTP adapter over a scripted transport

(defn- scripted
  "A `send` answering each request with the next of `responses`, recording requests."
  [seen responses]
  (let [left (atom responses)]
    (fn [req]
      (swap! seen conj req)
      (let [resp (first @left)]
        (swap! left rest)
        resp))))

(deftest http-adapter-maps-the-rest-api
  (testing "create, then a 409 of the same proxy answers the existing one"
    (let [seen (atom [])
          c (http/make-client "http://h:8474"
                              {:send (scripted seen [(r/ok {:status 201 :body "{\"name\":\"a.pg\"}"})
                                                     (r/ok {:status 409 :body "{\"error\":\"exists\"}"})
                                                     (r/ok {:status 200 :body (json/write-str {"name" "a.pg" "listen" "172.1.0.2:5432" "upstream" "u:5432"})})])})
          proxy {"name" "a.pg" "listen" "toxiproxy-a:5432" "upstream" "u:5432" "enabled" true}]
      (is (= {"name" "a.pg"} (:ok (port/-create-proxy! c proxy))))
      (is (= "u:5432" (get (:ok (port/-create-proxy! c proxy)) "upstream")))
      (is (= [{:method "POST" :path "/proxies" :body proxy}
              {:method "POST" :path "/proxies" :body proxy}
              {:method "GET" :path "/proxies/a.pg"}]
             @seen))))
  (testing "a 409 of a different proxy is a conflict"
    (let [c (http/make-client "x" {:send (scripted (atom []) [(r/ok {:status 409 :body ""})
                                                              (r/ok {:status 200 :body "{\"listen\":\"0.0.0.0:1\",\"upstream\":\"other:1\"}"})])})]
      (is (= :compose/toxiproxy-proxy-conflict
             (:error (port/-create-proxy! c {"name" "p" "listen" "l:1" "upstream" "u:1"}))))))
  (testing "toxics, remove of an absent one, reset, failures"
    (let [seen (atom [])
          c (http/make-client "x" {:send (scripted seen [(r/ok {:status 200 :body "{\"name\":\"t\"}"})
                                                         (r/ok {:status 404 :body "{\"error\":\"toxic not found\"}"})
                                                         (r/ok {:status 204 :body ""})
                                                         (r/ok {:status 500 :body "{\"error\":\"boom\"}"})
                                                         (r/err :compose/toxiproxy-unreachable {})])})]
      (is (= {"name" "t"} (:ok (port/-add-toxic! c "a b" {"name" "t"}))))
      (is (= "/proxies/a%20b/toxics" (:path (first @seen))))
      (is (= {:removed? false} (:ok (port/-remove-toxic! c "a" "t"))))
      (is (= {:reset true} (:ok (port/-reset! c))))
      (let [e (port/-list c)]
        (is (= :compose/toxiproxy-failed (:error e)))
        (is (= "boom" (:message e))))
      (is (= :compose/toxiproxy-unreachable (:error (port/-list c))))))
  (testing "the real transport answers an unreachable server as an error"
    (is (= :compose/toxiproxy-unreachable
           (:error (port/-list (http/make-client "http://127.0.0.1:1" {:timeout-ms 500})))))))
