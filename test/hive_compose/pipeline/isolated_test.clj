(ns hive-compose.pipeline.isolated-test
  "Isolated runs of sections (FUN-D) on the stub engine."
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.adapter.cli :as cli]
            [hive-compose.addon]
            [hive-compose.promote.config]
            [hive-compose.pipeline.isolated :as isolated]
            [hive-compose.pipeline.ops :as ops]
            [hive-compose.port :as port]
            [hive-compose.promote.isolated :as iso]
            [hive-compose.stub :as stub]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def model
  {"services" {"postgres" {"container_name" "dc-postgres" "image" "postgres:16"
                           "ports" [{"target" 5432 "published" "5434" "protocol" "tcp"}]
                           "volumes" [{"type" "volume" "source" "pgdata" "target" "/d"}]}
               "web" {"build" {"context" "."} "depends_on" {"postgres" {}}
                      "ports" [{"target" 8080 "published" "18090" "protocol" "tcp"}]
                      "volumes" [{"type" "volume" "source" "go_cache" "target" "/c"}]}
               "worker" {"image" "busybox"}}
   "volumes" {"pgdata" {"name" "dc_pgdata"} "go_cache" {"name" "dc_go_cache"} "ledger" {"name" "ledger-book"}
              "ext" {"name" "ext" "external" true}}})

(defn- tmp-dir []
  (let [d (io/file (System/getProperty "java.io.tmpdir") (str "hcf-iso-" (System/nanoTime)))]
    (.mkdirs d)
    (spit (io/file d "compose.yaml") "services: {}\n")
    (str d)))

(defn- world []
  (let [dir (tmp-dir)
        e (stub/engine {} {"dc" #{"postgres" "web"}}
                       {"dc" {"postgres" {} "web" {:deps ["postgres"]} "worker" {}}})
        clock (atom 1000)]
    (stub/model! e "dc" model)
    (stub/images! e {"dc-web" "sha256:aaa"})
    (stub/volumes! e {"dc_pgdata" {:labels {"com.docker.compose.project" "dc" "com.docker.compose.volume" "pgdata"}}
                      "ext" {:labels {}}})
    {:e e :dir dir :clock clock
     :ctx {:engine e
           :settings {:compose/projects {"dc" {:project/id "dc" :project/dir dir :project/name "dc"}}
                      :compose/profiles {"flow-payment" {:profile/id "flow-payment" :profile/dir dir :profile/project "dc"
                                                          :profile/services ["web"]
                                                          :profile/compose-profiles ["e2e-test"]
                                                          :profile/env {"PAY_SRC" "/w/pay" "TOKEN" {:pass "x/y"}}
                                                          :profile/run-shared-volumes ["go_cache"]}
                                         "flow-sync" {:profile/id "flow-sync" :profile/dir dir :profile/project "dc"
                                                      :profile/services ["worker" "postgres"]}}
                      :compose/default-ttl-minutes 60
                      :compose/default-idle-action :stop
                      :compose/state-file (str dir "/state.edn")}
           :state (atom {:active {} :current nil})
           :now #(deref clock)
           :rand-hex (constantly "abc123")}}))

(defn- preset [ctx id] (:ok (ops/lookup ctx id)))

(deftest one-run-several-sections-one-project
  (let [{:keys [ctx e]} (world)
        a (isolated/up! ctx (preset ctx "flow-payment") {:ports {:offset 1000}})
        rid (get-in a [:ok :run])
        b (isolated/up! ctx (preset ctx "flow-sync") {:run rid})]
    (is (r/ok? a) (pr-str a))
    (is (r/ok? b) (pr-str b))
    (is (= "dcabc123" rid))
    (is (= #{"flow-payment@dcabc123" "flow-sync@dcabc123"} (set (keys (:active @(:state ctx))))))
    (is (every? #(= "dc-dcabc123" (:project %)) (vals (:active @(:state ctx)))))
    (testing "the shared project is untouched"
      (is (= #{"postgres" "web"} (stub/running e "dc"))))
    (testing "native profiles ride on every compose call of the run's section (G2)"
      (is (= ["e2e-test"] (get-in @(:state ctx) [:active "flow-payment@dcabc123" :profile :profile/compose-profiles]))))
    (testing "run env (G6)"
      (let [env (get-in @(:state ctx) [:active "flow-sync@dcabc123" :profile :profile/env])]
        (is (= "dcabc123" (get env "HIVE_RUN")))
        (is (= "dc-dcabc123" (get env "HIVE_RUN_PROJECT")))
        (is (= "1000" (get env "HIVE_PORT_OFFSET")))
        (is (= "6434" (get env "HIVE_PORT_POSTGRES_5432")))
        (is (= "19090" (get env "HIVE_PORT_WEB_8080")))))
    (testing "the override: names reset, ports offset, image pinned by id, cache volume borrowed"
      (let [text (slurp (get-in @(:state ctx) [:active "flow-payment@dcabc123" :profile :profile/override]))]
        (is (str/includes? text "\"container_name\": !reset null"))
        (is (str/includes? text "\"published\": \"6434\""))
        (is (str/includes? text "\"image\": \"sha256:aaa\""))
        (is (str/includes? text "\"name\": \"dc_go_cache\""))
        (is (str/includes? text "\"name\": \"dc-dcabc123_ledger_dcabc123\"") "an explicit name never leaks into a run")
        (is (not (str/includes? text "pgdata")) "a compose-derived name is fresh under the run's -p")))
    (testing "a second run keeps its own port policy"
      (is (= :compose/invalid-param (:error (isolated/up! ctx (preset ctx "flow-sync") {:run rid :ports :strip})))))))

(deftest image-pinning
  (let [{:keys [ctx e]} (world)
        rid (get-in (isolated/up! ctx (preset ctx "flow-payment") {}) [:ok :run])]
    (is (= {"web" "sha256:aaa"} (get-in @(:state ctx) [:runs rid :images])))
    (stub/images! e {"dc-web" "sha256:bbb"})
    (let [x (isolated/up! ctx (preset ctx "flow-sync") {:run rid})]
      (is (= :compose/could-not-run (:error x)))
      (is (= {:reason :image-changed :service "web" :was "sha256:aaa" :now "sha256:bbb"}
             (select-keys x [:reason :service :was :now]))))
    (testing "a missing image is could-not-run, never a build"
      (let [{:keys [ctx]} (world)]
        (swap! (:world (:engine ctx)) assoc :images {})
        (is (= :image-missing (:reason (isolated/up! ctx (preset ctx "flow-payment") {}))))))))

(deftest reserved-env-and-shared-volume-rules
  (let [{:keys [ctx]} (world)]
    (is (= :compose/invalid-param
           (:error (isolated/up! ctx (update (preset ctx "flow-sync") :profile/env assoc "HIVE_PORT_X_1" "1") {}))))
    (is (= :compose/invalid-param
           (:error (isolated/up! ctx (preset ctx "flow-payment") {:shared-volumes ["pgdata"]})))
        "a call may only narrow the preset's list")
    (is (r/ok? (isolated/up! ctx (preset ctx "flow-payment") {:shared-volumes []})))))

(deftest could-not-run-categories
  (let [{:keys [ctx e]} (world)]
    (swap! (:world e) assoc :up-stderr "Error response from daemon: Bind for 0.0.0.0:6434 failed: port is already allocated")
    (let [x (isolated/up! ctx (preset ctx "flow-sync") {:ports {:offset 1000}})]
      (is (= :compose/could-not-run (:error x)))
      (is (= :port-in-use (:reason x)))
      (is (= 6434 (:port x))))
    (swap! (:world e) assoc :version "2.20.1" :up-stderr nil)
    (is (= :compose-too-old (:reason (isolated/up! ctx (preset ctx "flow-sync") {}))))))

(deftest dry-run-has-no-side-effect
  (let [{:keys [ctx e]} (world)
        x (isolated/plan ctx (preset ctx "flow-payment") {:ports {:offset 1000}})]
    (is (r/ok? x) (pr-str x))
    (is (= :ok (get-in x [:ok :admission :verdict])))
    (is (empty? (:active @(:state ctx))))
    (is (empty? (:runs @(:state ctx))))
    (is (not-any? #(= :up (first %)) (stub/calls e)))))

(deftest down-keeps-volumes-unless-asked-and-never-touches-the-shared-project
  (let [{:keys [ctx e]} (world)
        rid (get-in (isolated/up! ctx (preset ctx "flow-payment") {}) [:ok :run])]
    (is (contains? (stub/volumes e) "dc-dcabc123_pgdata"))
    (let [d (isolated/down-run! ctx rid false)]
      (is (r/ok? d) (pr-str d))
      (is (= ["dc-dcabc123_pgdata"] (get-in d [:ok :volumes-kept])))
      (is (contains? (stub/volumes e) "dc-dcabc123_pgdata"))
      (is (= [{:run rid :name "dc-dcabc123_pgdata"}] (:stale-run-volumes (isolated/status ctx)))))
    (testing "a later down run=<id> volumes=true of the ended run removes them (the real-docker gate order)"
      (let [d (isolated/down-run! ctx rid true)]
        (is (= ["dc-dcabc123_pgdata"] (get-in d [:ok :volumes :removed])) (pr-str d))
        (is (not (contains? (stub/volumes e) "dc-dcabc123_pgdata")))
        (is (contains? (stub/volumes e) "dc_pgdata"))))
    (testing "explicit removal takes only the run's own volumes"
      (let [{:keys [ctx e]} (world)
            rid (get-in (isolated/up! ctx (preset ctx "flow-payment") {}) [:ok :run])
            d (isolated/down-run! ctx rid true)]
        (is (= {:removed ["dc-dcabc123_pgdata"] :kept []} (get-in d [:ok :volumes])))
        (is (contains? (stub/volumes e) "dc_pgdata") "the shared project's volume stays")
        (is (contains? (stub/volumes e) "ext"))
        (is (every? (fn [[_ names]] (every? #(str/includes? % rid) names))
                    (filter #(= :remove-volumes (first %)) (stub/calls e))))))))

(deftest the-reaper-removes-an-unclaimed-run-and-spares-a-claimed-one
  (let [{:keys [ctx e clock]} (world)
        rid (get-in (isolated/up! ctx (preset ctx "flow-payment") {}) [:ok :run])]
    ;; a live claim (a wave claim included) keeps the section touched: the
    ;; usage pass (swarm/observe!) renews :last-touch while it is live
    (swap! (:state ctx) assoc-in [:active "flow-payment@dcabc123" :claims "ling-1"] {:since 1000 :seen 1000})
    (swap! clock + (* 120 60000))
    (swap! (:state ctx) update-in [:active "flow-payment@dcabc123"] assoc :last-touch @clock)
    (let [pass (ops/reap! ctx)]
      (is (empty? (isolated/after-reap! ctx (:reaped pass))))
      (is (contains? (stub/volumes e) "dc-dcabc123_pgdata") "a claimed run keeps its volumes"))
    (swap! clock + (* 120 60000))
    (let [pass (ops/reap! (assoc-in ctx [:settings :compose/claim-ttl-minutes] 30))
          ended (isolated/after-reap! ctx (:reaped pass))]
      (is (= [rid] (map :run ended)))
      (is (= ["dc-dcabc123_pgdata"] (get-in (first ended) [:volumes :removed])))
      (is (contains? (stub/volumes e) "dc_pgdata")))))

(deftest status-of-a-run
  (let [{:keys [ctx e]} (world)
        rid (get-in (isolated/up! ctx (preset ctx "flow-payment") {:ports {:offset 1000}}) [:ok :run])
        _ (stub/bindings! e "dc-dcabc123" {"postgres" {"5432" 6434} "web" {"8080" 19090}})
        row (first (:runs (isolated/status ctx)))
        srow (first (filter #(= "flow-payment@dcabc123" (:id %)) (:profiles (ops/status ctx))))]
    (is (= {"postgres" {"5432" 6434} "web" {"8080" 19090}} (:ports row)))
    (is (= ["dc-dcabc123_pgdata"] (get-in row [:volumes :created])))
    (is (= ["dc_go_cache"] (get-in row [:volumes :shared])))
    (is (str/starts-with? (:config-digest row) "sha256:"))
    (is (= {"PAY_SRC" "/w/pay"} (:env-src srow)) "only *_SRC values are shown")
    (is (some #{"TOKEN"} (:env-keys srow)))
    (is (= rid (:run srow)))))

;; ---------------------------------------------------------------------------
;; properties

(def gen-run (gen/fmap #(apply str %) (gen/vector (gen/elements "abcdef0123456789") 3 12)))

(def gen-volume
  (gen/let [proj (gen/elements ["dc" "dc-r1x" "other" nil])
            run-in-name? gen/boolean
            key (gen/elements ["pgdata" "fb" "cache"])]
    {:name (str (or proj "anon") "_" key (when run-in-name? "-RUN"))
     :labels (cond-> {} proj (assoc "com.docker.compose.project" proj "com.docker.compose.volume" key))}))

(defspec removable-is-a-subset-of-the-runs-own-volumes 300
  (prop/for-all [run gen-run
                 recorded? gen/boolean
                 vols (gen/vector gen-volume 0 8)
                 shared (gen/elements [[] ["cache"]])]
    (let [cp (iso/compose-project "dc" run)
          vols (map #(-> % (update :name str/replace "RUN" run)
                         (update :labels (fn [l] (if (= "dc-r1x" (get l "com.docker.compose.project"))
                                                   (assoc l "com.docker.compose.project" cp) l))))
                    vols)
          runs (if recorded? {run (assoc (iso/run-entry "dc" run :strip 1) :shared-volumes shared)} {})
          out (iso/removable runs run vols)]
      (and (set/subset? (set out) (set vols))
           (every? #(= cp (get-in % [:labels "com.docker.compose.project"])) out)
           (every? #(str/includes? (:name %) run) out)
           (not-any? #(= (str "dc_" %) (:name %)) shared)
           (or recorded? (empty? out))
           (empty? (iso/removable {"dc" {:run "dc" :base-project "dc" :compose-project "dc" :isolated? true}} "dc" vols))))))

(defspec config-digest-is-run-independent 100
  (prop/for-all [a gen-run b gen-run offset (gen/choose 0 2000)]
    (let [m (fn [run] (assoc-in model ["services" "postgres" "ports" 0 "published"] (str (+ 5434 offset (count run)))))
          cfg (fn [run] {"config" (assoc (m run) "name" (iso/compose-project "dc" run))
                         "override" {"volumes" {"pgdata" {"name" (str "dc_pgdata_" run)}}}})]
      (= (isolated/config-digest (cfg a) "dc" a) (isolated/config-digest (cfg b) "dc" b)))))

(deftest a-profile-with-no-override-keeps-its-argv
  (let [p {:profile/id "x" :profile/dir "/w/dc" :profile/files ["a.yml"] :profile/compose-profiles ["e2e"]}]
    (is (= ["docker" "compose" "--project-directory" "/w/dc" "-p" "dc" "-f" "a.yml" "--profile" "e2e"]
           (cli/base-argv p)))
    (is (= ["docker" "compose" "--project-directory" "/w/dc" "-p" "dc" "-f" "a.yml" "-f" "/s/o.yaml" "--profile" "e2e"]
           (cli/base-argv (assoc p :profile/override "/s/o.yaml"))))))

(deftest only-the-isolated-pipeline-can-remove-a-volume
  (let [callers (for [f (file-seq (io/file "src")) :when (str/ends-with? (str f) ".clj")
                      :when (str/includes? (slurp f) "-remove-volumes!")]
                  (str/replace (str f) #"^.*src/" ""))]
    (is (= #{"hive_compose/port.clj" "hive_compose/adapter/cli.clj" "hive_compose/pipeline/isolated.clj"}
           (set callers)))))

(deftest absolute-and-home-files-are-accepted
  (let [p (hive-compose.promote.config/normalize-profile "/home/u" {:profile/id "x" :profile/dir "~/w/dc"
                                                                     :profile/files ["compose.yml" "/gen/ports.yml" "~/o.yml"]})
        q (hive-compose.promote.config/normalize-project "/home/u" {:project/id "dc" :project/dir "/w/dc"
                                                                     :project/files ["~/a.yml" "b.yml"]})]
    (is (= ["compose.yml" "/gen/ports.yml" "/home/u/o.yml"] (:profile/files p)) "relative stays relative to the dir")
    (is (= ["/home/u/a.yml" "b.yml"] (:project/files q)))))

(deftest the-tool-drives-an-isolated-run
  (let [{:keys [ctx e]} (world)
        a (hive-compose.addon/make-addon)]
    (swap! (:state a) assoc :ctx ctx :lifecycle :active)
    (let [dry (hive-compose.addon/dispatch a "up" {"profile" "flow-sync" "isolated" true "dry_run" true
                                                   "ports" {"offset" 1000}})
          up (hive-compose.addon/dispatch a "up" {"profile" "flow-sync" "isolated" true "ports" {"offset" 1000}})
          rid (get-in up [:ok :run])
          st (hive-compose.addon/dispatch a "status" {})
          down (hive-compose.addon/dispatch a "down" {"run" rid})]
      (is (r/ok? dry) (pr-str dry))
      (is (= :ok (get-in dry [:ok :admission :verdict])))
      (is (r/ok? up) (pr-str up))
      (is (= [rid] (map :run (get-in st [:ok :runs]))))
      (is (r/ok? down) (pr-str down))
      (is (empty? (:active @(:state ctx))))
      (is (= #{"postgres" "web"} (stub/running e "dc")) "the shared project never moved"))))
