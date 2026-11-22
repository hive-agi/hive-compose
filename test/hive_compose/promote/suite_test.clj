(ns hive-compose.promote.suite-test
  "The pure suite x project matrix (hc-8): cells, the union stack, runner
   specs, cell results and the summary."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.adapter.suite-runner :as suite-runner]
            [hive-compose.promote.otel :as otel]
            [hive-compose.promote.suite :as suite]
            [hive-compose.schema :as schema]
            [hive-dsl.result :as r]
            [malli.core :as m]))

;; SPDX-License-Identifier: MIT

(def projects
  {"shop" {:project/id "shop" :project/dir "/w/shop"
           :project/needs {"kafka" {:version "^3"} "postgres" {:share? false}}}
   "billing" {:project/id "billing" :project/dir "/w/billing"
              :project/needs {"kafka" {:version "^3"}}}
   "web" {:project/id "web" :project/dir "/w/web"}})

(def suites
  {"smoke" {:suite/id "smoke" :suite/image "ghcr.io/acme/smoke:1"
            :suite/needs {"otel" {}}
            :suite/env {"LOG_LEVEL" "debug" "HIVE_RUN_ID" "spoofed"}
            :suite/report-format :junit}
   "api" {:suite/id "api" :suite/command ["npm" "test"] :suite/dir "/w/tests/api"
          :suite/projects ["shop" "billing"]
          :suite/reports-dir "out/reports"
          :suite/timeout-ms 60000}
   "load" {:suite/id "load" :suite/command "k6 run load.js"
           :suite/needs {"kafka" {:share? false}}
           :suite/projects ["web"]}})

(deftest suites-fixture-conforms
  (doseq [[id s] suites] (is (nil? (schema/explain schema/Suite s)) id))
  (doseq [[id p] projects] (is (nil? (schema/explain schema/Project p)) id)))

;; ---------------------------------------------------------------------------
;; (a) expand-matrix

(deftest parse-names-forms
  (is (nil? (suite/parse-names nil)))
  (is (nil? (suite/parse-names "")))
  (is (nil? (suite/parse-names " , ")))
  (is (= ["a" "b"] (suite/parse-names "b, a,,b")))
  (is (= ["a" "b"] (suite/parse-names [:b 'a "a"])))
  (is (= ["x"] (suite/parse-names :x))))

(deftest every-suite-on-its-projects
  (is (= [{:suite/id "api" :project "billing"} {:suite/id "api" :project "shop"}
          {:suite/id "load" :project "web"}
          {:suite/id "smoke" :project "billing"} {:suite/id "smoke" :project "shop"}
          {:suite/id "smoke" :project "web"}]
         (:ok (suite/expand-matrix suites projects {})))
      "a suite without :suite/projects runs against every project"))

(deftest filters-narrow-the-matrix
  (is (= [{:suite/id "api" :project "shop"} {:suite/id "smoke" :project "shop"}]
         (:ok (suite/expand-matrix suites projects {:suites "smoke,api" :projects "shop"}))))
  (is (= [{:suite/id "load" :project "web"}]
         (:ok (suite/expand-matrix suites projects {:suites ["load"]}))))
  (testing ":suite/projects limits the cells even when a filter names more"
    (is (= [{:suite/id "api" :project "billing"}]
           (:ok (suite/expand-matrix suites projects {:suites "api" :projects "billing,web"}))))))

(deftest unknown-names-are-named
  (let [res (suite/expand-matrix suites projects {:suites "smoke,nope,zzz" :projects "shop,ghost"})]
    (is (= :compose/unknown-names (:error res)))
    (is (= ["nope" "zzz"] (:suites res)))
    (is (= ["ghost"] (:projects res)))
    (is (str/includes? (:message res) "nope"))
    (is (str/includes? (:message res) "ghost")))
  (testing "a suite naming a project that does not exist"
    (let [res (suite/expand-matrix (assoc-in suites ["api" :suite/projects] ["shop" "gone"]) projects {})]
      (is (= :compose/unknown-names (:error res)))
      (is (= {"api" ["gone"]} (:suite-projects res)))
      (is (str/includes? (:message res) "api -> gone")))))

(deftest an-empty-matrix-is-an-error
  (let [res (suite/expand-matrix suites projects {:suites "load" :projects "shop"})]
    (is (= :compose/empty-matrix (:error res)))
    (is (str/includes? (:message res) "shop")))
  (is (= :compose/empty-matrix (:error (suite/expand-matrix {} projects {})))))

;; ---------------------------------------------------------------------------
;; (b) union-needs

(deftest union-of-projects-and-suites
  (let [cells (:ok (suite/expand-matrix suites projects {}))]
    (is (= {"billing" {"kafka" {:version "^3"}}
            "shop" {"kafka" {:version "^3"} "postgres" {:share? false}}
            "web" {}
            "suite:smoke" {"otel" {}}
            "suite:load" {"kafka" {:share? false}}}
           (suite/union-needs suites projects cells)))
    (testing "only what the selected cells need"
      (is (= {"shop" {"kafka" {:version "^3"} "postgres" {:share? false}}}
             (suite/union-needs suites projects [{:suite/id "api" :project "shop"}]))))
    (is (= {:stack/id "test-r01" :stack/consumers ["billing" "shop" "web"]
            :stack/description "suite matrix of run r01"}
           (suite/matrix-stack "r01" cells)))
    (is (nil? (schema/explain schema/Stack (suite/matrix-stack "r01" cells))))
    (is (every? #(m/validate schema/ConsumerId %) (keys (suite/union-needs suites projects cells))))))

(def gen-name (gen/elements ["a" "b" "c" "d" "e"]))
(def gen-cap (gen/elements ["kafka" "postgres" "redis" "otel" "s3"]))
(def gen-need (gen/let [v (gen/elements ["*" "^3" "~3.7" ">=3.5 <4"]) s gen/boolean]
                {:version v :share? s}))
(def gen-needs (gen/map gen-cap gen-need {:max-elements 3}))

(def gen-world
  "Random projects and suites, and the cells of their whole matrix."
  (gen/let [pids (gen/fmap #(vec (sort %)) (gen/set gen-name {:min-elements 1}))
            pneeds (gen/vector gen-needs (count pids))
            sids (gen/fmap #(vec (sort %)) (gen/set gen-name {:min-elements 1}))
            sdefs (gen/vector (gen/tuple gen-needs
                                         (gen/one-of [(gen/return nil)
                                                      (gen/fmap #(vec (distinct %))
                                                                (gen/vector (gen/elements pids) 1 3))]))
                              (count sids))]
    (let [projects (into {} (map (fn [id n] [id (cond-> {:project/id id :project/dir (str "/w/" id)}
                                                  (seq n) (assoc :project/needs n))])
                                 pids pneeds))
          suites (into {} (map (fn [id [n ps]] [id (cond-> {:suite/id id :suite/command "t"}
                                                     (seq n) (assoc :suite/needs n)
                                                     ps (assoc :suite/projects ps))])
                               sids sdefs))]
      {:projects projects :suites suites
       :cells (:ok (suite/expand-matrix suites projects {}))})))

(defspec union-is-order-independent-and-covers-every-cell 200
  (prop/for-all [{:keys [projects suites cells]} gen-world
                 seed gen/nat]
    (let [shuffled (vec (sort-by #(hash [seed %]) cells))
          u (suite/union-needs suites projects cells)]
      (and (= u (suite/union-needs suites projects shuffled))
           ;; duplicate cells add nothing
           (= u (suite/union-needs suites projects (concat cells shuffled)))
           (every? (fn [{sid :suite/id p :project}]
                     (and (= (get-in projects [p :project/needs] {}) (get u p))
                          (or (empty? (get-in suites [sid :suite/needs]))
                              (= (get-in suites [sid :suite/needs]) (get u (suite/suite-consumer sid))))))
                   cells)
           ;; minimal: no consumer beyond the cells' projects and suites
           (= (set (keys u))
              (into (set (map :project cells))
                    (comp (filter #(seq (get-in suites [(:suite/id %) :suite/needs])))
                          (map #(suite/suite-consumer (:suite/id %))))
                    cells))))))

(defspec matrix-cells-respect-suite-projects-and-filters 200
  (prop/for-all [{:keys [projects suites]} gen-world
                 pf (gen/vector gen-name 0 3)
                 sf (gen/vector gen-name 0 3)]
    (let [res (suite/expand-matrix suites projects {:suites sf :projects pf})
          known? (fn [m xs] (every? #(contains? m %) xs))]
      (if (and (known? suites sf) (known? projects pf))
        (or (= :compose/empty-matrix (:error res))
            (every? (fn [{sid :suite/id p :project}]
                      (and (contains? projects p)
                           (or (empty? sf) (some #{sid} sf))
                           (or (empty? pf) (some #{p} pf))
                           (if-let [own (get-in suites [sid :suite/projects])] (some #{p} own) true)))
                    (:ok res)))
        (and (= :compose/unknown-names (:error res))
             (= (set (concat (:suites res) (:projects res)))
                (set (concat (remove #(contains? suites %) sf)
                             (remove #(contains? projects %) pf)))))))))

;; ---------------------------------------------------------------------------
;; (c) runner-spec

(defn counter-ids
  "A deterministic id source: each call answers the next counter, as hex."
  []
  (let [n (atom 0)]
    (fn [bytes] (format (str "%0" (* 2 bytes) "x") (swap! n inc)))))

(defn fake-otlp-env
  "otlp-env as data: what hive-compose.promote.otel/otlp-env answers."
  [{:keys [run-id service test-id side]}]
  {"OTEL_EXPORTER_OTLP_ENDPOINT" (if (= side :host) "http://127.0.0.1:4317" "http://otel:4317")
   "OTEL_EXPORTER_OTLP_PROTOCOL" "grpc"
   "OTEL_SERVICE_NAME" service
   "OTEL_RESOURCE_ATTRIBUTES" (str "hive.run.id=" run-id ",service.name=" service ",hive.test.id=" test-id)})

(defn ctx [& {:as over}]
  (merge {:suites suites :run "r01" :state-dir "/var/hc/" :id-source (counter-ids)
          :otlp-env fake-otlp-env}
         over))

(deftest container-runner
  (let [spec (:ok (suite/runner-spec (ctx) {:suite/id "smoke" :project "billing"}))]
    (is (nil? (m/explain suite/RunnerSpec spec)))
    (is (= :container (:runner spec)))
    (is (= "ghcr.io/acme/smoke:1" (:image spec)))
    (is (= "hive-r01-billing" (:network spec)) "the view network of the project under test")
    (is (= (schema/view-network "r01" "billing") (:network spec)))
    (is (= [{:host "/var/hc/runs/r01/smoke/billing/reports" :container "/reports"}] (:mounts spec)))
    (is (= "/var/hc/runs/r01/smoke/billing/reports" (:reports-dir spec)))
    (is (= {"hive.run" "r01" "hive.consumer" "suite:smoke"} (:labels spec)))
    (is (= ["hive-r01-suite-smoke"] (:extra-networks spec)) "the suite's own instances answer on its view network")
    (is (= :junit (:report-format spec)))
    (let [env (:env spec)]
      (is (= "r01" (env "HIVE_RUN_ID")) "a suite cannot override the correlation vars")
      (is (= "smoke" (env "HIVE_SUITE_ID")))
      (is (= "smoke/billing/" (env "HIVE_TEST_ID_PREFIX")))
      (is (= "/reports" (env "HIVE_REPORTS_DIR")))
      (is (= "test.id=smoke%2Fbilling" (env "BAGGAGE")))
      (is (= (str "00-" (format "%032x" 1) "-" (format "%016x" 2) "-01") (env "TRACEPARENT")))
      (is (= (format "%032x" 1) (:trace-id spec)))
      (is (= "debug" (env "LOG_LEVEL")) ":suite/env layered in")
      (is (= "http://otel:4317" (env "OTEL_EXPORTER_OTLP_ENDPOINT")))
      (is (= "smoke/billing" (get (otel/parse-resource-attributes (env "OTEL_RESOURCE_ATTRIBUTES")) "hive.test.id"))))))

(deftest command-runner
  (let [spec (:ok (suite/runner-spec (ctx) {:suite/id "api" :project "shop"}))]
    (is (nil? (m/explain suite/RunnerSpec spec)))
    (is (= :command (:runner spec)))
    (is (= ["npm" "test"] (:command spec)))
    (is (= "/w/tests/api" (:dir spec)))
    (is (= 60000 (:timeout-ms spec)))
    (is (= "/var/hc/runs/r01/api/shop/reports" (:reports-dir spec)) "every cell has its own reports dir")
    (is (= "/var/hc/runs/r01/api/shop/reports" (get-in spec [:env "HIVE_REPORTS_DIR"])))
    (is (= "/w/tests/api/out/reports" (:collect-from spec)) "the declared dir, relative to :suite/dir, is collected from")
    (is (= "http://127.0.0.1:4317" (get-in spec [:env "OTEL_EXPORTER_OTLP_ENDPOINT"])) "a host command sees the host side")
    (is (not (contains? spec :network)))
    (is (= :auto (:report-format spec))))
  (testing "a command without its own reports dir reports under the state dir"
    (let [spec (:ok (suite/runner-spec (ctx) {:suite/id "load" :project "web"}))]
      (is (= "/var/hc/runs/r01/load/web/reports" (:reports-dir spec)))
      (is (not (contains? spec :dir))))))

(deftest otlp-is-optional-and-layered
  (testing "no otlp-env: no OTEL vars"
    (let [env (:env (:ok (suite/runner-spec (ctx :otlp-env nil) {:suite/id "load" :project "web"})))]
      (is (not-any? #(str/starts-with? % "OTEL_") (keys env)))))
  (testing "wiring env under otlp under suite env under correlation"
    (let [env (:env (:ok (suite/runner-spec (ctx :wiring-env {"KAFKA_BROKERS" "kafka:9092" "LOG_LEVEL" "info"
                                                               "TRACEPARENT" "forged"})
                                            {:suite/id "smoke" :project "shop"})))]
      (is (= "kafka:9092" (env "KAFKA_BROKERS")))
      (is (= "debug" (env "LOG_LEVEL")))
      (is (not= "forged" (env "TRACEPARENT")))))
  (testing "a suite's resource attributes are merged, ours win per key"
    (let [s (assoc-in suites ["smoke" :suite/env "OTEL_RESOURCE_ATTRIBUTES"] "team=qa,hive.run.id=evil")
          env (:env (:ok (suite/runner-spec (ctx :suites s) {:suite/id "smoke" :project "shop"})))
          attrs (otel/parse-resource-attributes (env "OTEL_RESOURCE_ATTRIBUTES"))]
      (is (= "qa" (attrs "team")))
      (is (= "r01" (attrs "hive.run.id")))
      (is (= "smoke/shop" (attrs "hive.test.id")))))
  (testing "a suite naming only the OTLP endpoint breaks the pair: refused, as wiring refuses it"
    (let [s (assoc-in suites ["smoke" :suite/env] {"OTEL_EXPORTER_OTLP_ENDPOINT" "http://collector:4318"})]
      (is (= :compose/otlp-pair-broken (:error (suite/runner-spec (ctx :suites s) {:suite/id "smoke" :project "shop"}))))))
  (testing "a suite naming the whole pair owns it"
    (let [s (assoc-in suites ["smoke" :suite/env] {"OTEL_EXPORTER_OTLP_ENDPOINT" "http://collector:4318"
                                                     "OTEL_EXPORTER_OTLP_PROTOCOL" "http/protobuf"})
          env (:env (:ok (suite/runner-spec (ctx :suites s) {:suite/id "smoke" :project "shop"})))]
      (is (= ["http://collector:4318" "http/protobuf"]
             [(env "OTEL_EXPORTER_OTLP_ENDPOINT") (env "OTEL_EXPORTER_OTLP_PROTOCOL")]))))
  (testing "wiring attributes survive; without otlp the run id is still forced"
    (let [s (assoc-in suites ["load" :suite/env] {"OTEL_RESOURCE_ATTRIBUTES" "hive.run.id=evil,service.name=k6"})
          env (:env (:ok (suite/runner-spec (ctx :suites s :otlp-env nil
                                                 :wiring-env {"OTEL_RESOURCE_ATTRIBUTES" "team=core"})
                                            {:suite/id "load" :project "web"})))
          attrs (otel/parse-resource-attributes (env "OTEL_RESOURCE_ATTRIBUTES"))]
      (is (= {"team" "core" "hive.run.id" "r01" "service.name" "k6" "hive.test.id" "load/web"} attrs))))
  (testing "a suite may name its own service.name; hive.* stays ours"
    (let [s (assoc-in suites ["smoke" :suite/env "OTEL_RESOURCE_ATTRIBUTES"] "service.name=mine,hive.test.id=x")
          attrs (otel/parse-resource-attributes
                 (get-in (:ok (suite/runner-spec (ctx :suites s) {:suite/id "smoke" :project "shop"}))
                         [:env "OTEL_RESOURCE_ATTRIBUTES"]))]
      (is (= "mine" (attrs "service.name")))
      (is (= "smoke/shop" (attrs "hive.test.id")))))
  (testing "an otlp-env that throws is a Result error"
    (is (= :compose/otlp-env-failed
           (:error (suite/runner-spec (ctx :otlp-env (fn [_] (throw (ex-info "no :service" {}))))
                                      {:suite/id "load" :project "web"}))))))

(deftest with-the-real-otlp-env
  (let [spec (:ok (suite/runner-spec (ctx :otlp-env otel/otlp-env) {:suite/id "smoke" :project "billing.api"}))
        env (:env spec)
        attrs (otel/parse-resource-attributes (env "OTEL_RESOURCE_ATTRIBUTES"))]
    (is (nil? (m/explain suite/RunnerSpec spec)))
    (is (= (otel/endpoint :container :grpc) (env "OTEL_EXPORTER_OTLP_ENDPOINT")))
    (is (= "suite-smoke" (env "OTEL_SERVICE_NAME")))
    (is (= {"hive.run.id" "r01" "service.name" "suite-smoke" "hive.test.id" "smoke/billing.api"} attrs))
    (is (= "test.id=smoke%2Fbilling.api" (env "BAGGAGE")))
    (is (= "hive-r01-billing-api" (:network spec))))
  (is (= (otel/endpoint :host :grpc)
         (get-in (:ok (suite/runner-spec (ctx :otlp-env otel/otlp-env) {:suite/id "api" :project "shop"}))
                 [:env "OTEL_EXPORTER_OTLP_ENDPOINT"]))))

(deftest runner-spec-refusals
  (is (= :compose/unknown-names (:error (suite/runner-spec (ctx) {:suite/id "nope" :project "web"}))))
  (is (= :compose/invalid-run-id (:error (suite/runner-spec (ctx :run "ci-1") {:suite/id "load" :project "web"}))))
  (is (= :compose/invalid-param (:error (suite/runner-spec (ctx :state-dir nil) {:suite/id "load" :project "web"}))))
  (is (= :compose/invalid-param (:error (suite/runner-spec (ctx :id-source nil) {:suite/id "load" :project "web"}))))
  (testing "a bad id source is refused, never turned into a bad traceparent"
    (doseq [src [(constantly "xyz") (fn [n] (apply str (repeat (* 2 n) "0")))
                 (fn [n] (apply str (repeat (* 2 n) "A"))) (fn [_] (throw (ex-info "entropy gone" {})))]]
      (is (= :compose/invalid-trace-id
             (:error (suite/runner-spec (ctx :id-source src) {:suite/id "load" :project "web"})))))))

(deftest path-traversal-is-refused
  (doseq [bad [".." "." "a/../b" "/abs" "a:b" "a b" "" "-x" "billing/api"]]
    (is (= :compose/invalid-param (:error (suite/runner-spec (ctx) {:suite/id "smoke" :project bad})))
        (str "project " (pr-str bad)))
    (is (= :compose/invalid-param (:error (suite/runner-spec (ctx :suites (assoc suites bad (get suites "smoke")))
                                                             {:suite/id bad :project "shop"})))
        (str "suite " (pr-str bad))))
  (testing "a command's reports dir never leaves :suite/dir"
    (doseq [bad ["/tmp/x" "../x" "out/../../x"]]
      (is (= :compose/invalid-param
             (:error (suite/runner-spec (ctx :suites (assoc-in suites ["api" :suite/reports-dir] bad))
                                        {:suite/id "api" :project "shop"})))
          bad))
    (is (= :compose/invalid-param
           (:error (suite/runner-spec (ctx :suites (update suites "api" dissoc :suite/dir))
                                      {:suite/id "api" :project "shop"})))
        "a relative reports dir with no :suite/dir to be relative to")))

(def gen-unsafe-part
  (gen/one-of [(gen/elements [".." "." "..." "a/../b" "/abs" "a:b" "" "/" "a/b" "c:\\x"])
               (gen/fmap (fn [[a b]] (str a "/" b)) (gen/tuple gen/string-alphanumeric gen/string-alphanumeric))
               (gen/fmap #(str % ":") gen/string-alphanumeric)
               (gen/fmap #(str "../" %) gen/string-alphanumeric)]))

(defspec unsafe-ids-never-become-paths 200
  (prop/for-all [bad gen-unsafe-part]
    (and (= :compose/invalid-param (:error (suite/runner-spec (ctx) {:suite/id "smoke" :project bad})))
         (= :compose/invalid-param (:error (suite/runner-spec (ctx :suites {bad (get suites "smoke")})
                                                              {:suite/id bad :project "shop"}))))))

(deftest resources-reach-the-spec-and-the-container-argv
  (let [capped (assoc-in suites ["smoke" :suite/resources] {:mem "256m" :cpus 0.5})
        spec (:ok (suite/runner-spec (ctx :suites capped) {:suite/id "smoke" :project "shop"}))
        cs (#'suite-runner/caps spec)
        argv (suite-runner/container-argv spec cs)]
    (is (= {:mem (* 256 1024 1024) :cpus 0.5} cs) "the adapter reads the spec's :resources")
    (is (nil? (m/explain suite/RunnerSpec spec)))
    (is (= {:mem "256m" :cpus 0.5} (:resources spec)))
    (is (some #{"--memory"} argv) (pr-str argv))
    (is (some #{"--cpus"} argv) (pr-str argv)))
  (testing ":compose/default-resources is the fallback, a suite's own caps win, :unbounded passes through"
    (is (= {:mem "1g"} (:resources (:ok (suite/runner-spec (ctx :default-resources {:mem "1g"})
                                                            {:suite/id "api" :project "shop"})))))
    (is (= :unbounded (:resources (:ok (suite/runner-spec (ctx :suites (assoc-in suites ["api" :suite/resources] :unbounded)
                                                               :default-resources {:mem "1g"})
                                                          {:suite/id "api" :project "shop"})))))
    (is (not (contains? (:ok (suite/runner-spec (ctx) {:suite/id "api" :project "shop"})) :resources)))))

(deftest slug-alike-cells-get-distinct-container-names
  (is (not= (suite/container-name "r01" "smoke" "a.b") (suite/container-name "r01" "smoke" "a-b")))
  (is (str/starts-with? (suite/container-name "r01" "smoke" "a.b") "hive-r01-smoke-a-b-")))

(deftest test-id-is-forced-without-otlp
  (let [s (assoc-in suites ["load" :suite/env] {"OTEL_RESOURCE_ATTRIBUTES" "hive.test.id=x"})
        attrs (otel/parse-resource-attributes
               (get-in (:ok (suite/runner-spec (ctx :suites s :otlp-env nil) {:suite/id "load" :project "web"}))
                       [:env "OTEL_RESOURCE_ATTRIBUTES"]))]
    (is (= "load/web" (attrs "hive.test.id")))))

(def traceparent-re #"^00-[0-9a-f]{32}-[0-9a-f]{16}-01\z")

(def gen-hex-source
  "A pure id source fed by generated bytes: never all-zero."
  (gen/fmap (fn [bs]
              (let [n (atom 0)]
                (fn [k]
                  (let [i (swap! n inc)]
                    (apply str (map #(format "%02x" (bit-and (+ % i) 0xff))
                                    (take k (cycle (cons 1 bs)))))))))
            (gen/vector (gen/choose 0 255) 1 24)))

(def gen-id-part
  (gen/fmap (fn [[c cs]] (apply str c cs))
            (gen/tuple (gen/elements (seq "abcXYZ019"))
                       (gen/vector (gen/elements (seq "abcXYZ019_.-")) 0 10))))

(defspec runner-env-is-always-correlated 200
  (prop/for-all [sid gen-id-part
                 project (gen/one-of [gen-id-part (gen/elements ["billing.api" "a_b" "x-y.z" "9"])])
                 run (gen/fmap #(apply str %) (gen/vector (gen/elements (seq "abc0123456789")) 3 12))
                 src gen-hex-source
                 image? gen/boolean
                 suite-env (gen/map (gen/elements ["HIVE_RUN_ID" "BAGGAGE" "TRACEPARENT" "FOO"]) gen/string-alphanumeric)]
    (let [s (cond-> {:suite/id sid :suite/env suite-env}
              image? (assoc :suite/image "img")
              (not image?) (assoc :suite/command "t"))
          cell {:suite/id sid :project project}
          res (suite/runner-spec {:suites {sid s} :run run :state-dir "/st" :id-source src} cell)
          spec (:ok res)
          env (:env spec)]
      (and (r/ok? res)
           (m/validate suite/RunnerSpec spec)
           (= run (env "HIVE_RUN_ID"))
           (= sid (env "HIVE_SUITE_ID"))
           (re-matches traceparent-re (env "TRACEPARENT"))
           (= (str sid "/" project) (otel/percent-decode (subs (env "BAGGAGE") (count "test.id="))))
           (re-matches #"test\.id=[A-Za-z0-9%._~-]*" (env "BAGGAGE"))
           (= (str "/st/runs/" run "/" sid "/" project "/reports") (:reports-dir spec))
           (if image?
             (= (schema/view-network run project) (:network spec))
             (not (contains? spec :network)))))))

;; ---------------------------------------------------------------------------
;; (d) cell-result

(defn report
  "A Report as hc-8's promote.report builds it, with `statuses` as its cases."
  [statuses]
  (let [cases (vec (map-indexed (fn [i st] {:case/id (str "smoke/t." i) :case/name (str "t." i)
                                            :case/classname "t" :case/status st :case/time-s 0.1
                                            :case/message nil :case/output nil})
                                statuses))]
    {:report/format :junit
     :report/cases cases
     :report/totals (merge {:passed 0 :failed 0 :error 0 :skipped 0} (frequencies statuses))}))

(def cell {:suite/id "smoke" :project "shop"})

(deftest cell-results
  (testing "a green report and exit 0"
    (let [res (suite/cell-result "r01" cell {:exit 0 :report (r/ok (report [:passed :skipped]))
                                             :started-at 1000 :finished-at 3500})]
      (is (= :passed (:status res)))
      (is (= {:run/id "r01" :suite/id "smoke" :project "shop" :exit 0}
             (select-keys res [:run/id :suite/id :project :exit])))
      (is (= 2500 (:duration-ms res)))
      (is (= 2 (count (get-in res [:report :report/cases]))))))
  (testing "failing cases fail the cell even at exit 0"
    (is (= :failed (:status (suite/cell-result "r01" cell {:exit 0 :report (r/ok (report [:passed :failed]))})))))
  (testing "a failed case fails the cell even when the totals were not recounted"
    (is (= :failed (:status (suite/cell-result "r01" cell {:exit 0 :report (r/ok (assoc (report [:failed])
                                                                                         :report/totals {:passed 1 :failed 0 :error 0 :skipped 0}))})))))
  (testing "a bail-out fails the cell"
    (is (= :failed (:status (suite/cell-result "r01" cell {:exit 0 :report (r/ok (assoc (report [:passed]) :report/bail-out "db down"))})))))
  (testing "a non-zero exit fails a green report"
    (is (= :failed (:status (suite/cell-result "r01" cell {:exit 2 :report (r/ok (report [:passed]))})))))
  (testing "no report: the exit code is the verdict"
    (let [none (r/err :report/none {:dir "/x"})]
      (is (= :passed (:status (suite/cell-result "r01" cell {:exit 0 :report none}))))
      (let [res (suite/cell-result "r01" cell {:exit 137 :report none})]
        (is (= :error (:status res)))
        (is (= {:error :report/none :dir "/x"} (:report-error res)))
        (is (not (contains? res :report))))
      (is (= :error (:status (suite/cell-result "r01" cell {:exit nil :report none}))) "killed, no exit")))
  (testing "a malformed report counts as none"
    (is (= :error (:status (suite/cell-result "r01" cell {:exit 1 :report (r/err :report/malformed {:reason :tap/no-tests})})))))
  (testing "the fields hc-13 record-run! reads"
    (let [res (suite/cell-result "r01" cell {:exit 1 :report (r/ok (report [:failed]))})]
      (is (m/validate schema/RunId (:run/id res)))
      (is (string? (:suite/id res)))
      (is (string? (:project res)))
      (is (every? #(and (string? (:case/id %)) (keyword? (:case/status %)))
                  (get-in res [:report :report/cases]))))))

(deftest an-oom-finding-reaches-the-cell-result
  (let [f {:oom/service "smoke/shop" :oom/source :container :oom/cap 268435456 :oom/exit 137
           :oom/line "smoke/shop OOM-killed at 256m"}
        out (suite/runner-outcome {:exit 137 :started-at 10 :finished-at 30 :timed-out? false :oom f}
                                  (r/err :report/none {:dir "/x"}))
        res (suite/cell-result "r01" cell out)]
    (is (= f (:oom res)))
    (is (= :error (:status res)))
    (is (= 20 (:duration-ms res)))
    (is (= "smoke/shop OOM-killed at 256m" (get-in (suite/matrix-summary [res]) [:failures 0 :oom/line]))))
  (testing "no finding, no :oom; a timed-out runner has no exit"
    (let [res (suite/cell-result "r01" cell (suite/runner-outcome {:exit 1 :timed-out? true} (r/err :report/none {})))]
      (is (not (contains? res :oom)))
      (is (nil? (:exit res)))
      (is (not (contains? (first (:failures (suite/matrix-summary [res]))) :oom/line))))))

(def gen-status (gen/elements [:passed :failed :error :skipped]))
(def gen-exit (gen/one-of [(gen/return 0) (gen/return nil) (gen/choose -1 255)]))
(def gen-report-res
  (gen/one-of [(gen/fmap #(r/ok (report %)) (gen/vector gen-status 0 6))
               (gen/return (r/err :report/none {:dir "/x"}))
               (gen/return (r/err :report/malformed {:reason :x}))]))

(defspec status-follows-exit-and-report 300
  (prop/for-all [exit gen-exit rep gen-report-res]
    (let [{:keys [status]} (suite/cell-result "r01" cell {:exit exit :report rep})
          ok? (r/ok? rep)
          t (get-in rep [:ok :report/totals])
          failing? (and ok? (pos? (+ (:failed t) (:error t))))
          dirty? (not= 0 exit)]
      (cond
        (and (not ok?) dirty?) (= :error status)
        (or failing? dirty?) (= :failed status)
        :else (= :passed status)))))

;; ---------------------------------------------------------------------------
;; (e) matrix-summary

(deftest summary-folds-cells
  (let [results [(suite/cell-result "r01" {:suite/id "smoke" :project "shop"}
                                    {:exit 0 :report (r/ok (report [:passed :passed])) :started-at 0 :finished-at 10})
                 (suite/cell-result "r01" {:suite/id "smoke" :project "web"}
                                    {:exit 1 :report (r/ok (report [:passed :failed :skipped])) :started-at 0 :finished-at 5})
                 (suite/cell-result "r01" {:suite/id "api" :project "shop"}
                                    {:exit 137 :report (r/err :report/none {})})]
        s (suite/matrix-summary results)]
    (is (= :error (:status s)))
    (is (= 1 (:exit s)))
    (is (= {:cells 3 :passed 1 :failed 1 :error 1} (select-keys s [:cells :passed :failed :error])))
    (is (= {:passed 3 :failed 1 :error 0 :skipped 1} (:cases s)))
    (is (= {"api" {:error 1} "smoke" {:failed 1 :passed 1}} (:by-suite s)))
    (is (= {"shop" {:error 1 :passed 1} "web" {:failed 1}} (:by-project s)))
    (is (= [{:suite/id "api" :project "shop" :status :error :exit 137 :cases []}
            {:suite/id "smoke" :project "web" :status :failed :exit 1 :cases ["smoke/t.1"]}]
           (:failures s)))
    (is (= 15 (:duration-ms s))))
  (is (= {:status :passed :exit 0 :cells 0 :passed 0 :failed 0 :error 0}
         (select-keys (suite/matrix-summary []) [:status :exit :cells :passed :failed :error]))))

(def gen-results
  (gen/let [n (gen/choose 0 8)
            outs (gen/vector (gen/tuple gen-exit gen-report-res (gen/choose 0 100)) n)]
    (vec (map-indexed (fn [i [exit rep d]]
                        (suite/cell-result "r01" {:suite/id (str "s" (mod i 3)) :project (str "p" i)}
                                           {:exit exit :report rep :started-at 0 :finished-at d}))
                      outs))))

(defspec summary-is-order-independent-and-consistent 200
  (prop/for-all [results gen-results seed gen/nat]
    (let [s (suite/matrix-summary results)
          shuffled (sort-by #(hash [seed (:project %)]) results)]
      (and (= s (suite/matrix-summary shuffled))
           (= (:cells s) (+ (:passed s) (:failed s) (:error s)) (count results))
           (= (= 0 (:exit s)) (every? #(= :passed (:status %)) results))
           (= (count (:failures s)) (+ (:failed s) (:error s)))))))
