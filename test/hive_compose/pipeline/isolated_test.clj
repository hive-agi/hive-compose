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
            [hive-compose.pipeline.swarm]
            [hive-compose.schema]
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
    (stub/images! e {"dc-web" "sha256:aaa" "postgres:16" "sha256:pg" "busybox:latest" "sha256:bb"})
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
        (is (not (str/includes? text "pgdata_dcabc123")) "a compose-derived name is fresh under the run's -p: no rename")
        (is (str/includes? text "\"hive.compose.run\": \"dcabc123\"") "B1: the run's volumes carry its ownership labels")
        (is (str/includes? text "\"driver\": !reset null") "B3: a borrowed volume resets its driver")))
    (testing "a second run keeps its own port policy"
      (is (= :compose/invalid-param (:error (isolated/up! ctx (preset ctx "flow-sync") {:run rid :ports :strip})))))))

(deftest image-pinning
  (let [{:keys [ctx e]} (world)
        rid (get-in (isolated/up! ctx (preset ctx "flow-payment") {}) [:ok :run])]
    (is (= {"web" "sha256:aaa" "postgres" "sha256:pg"} (get-in @(:state ctx) [:runs rid :images]))
        "B2: every image of the section's closure (web + postgres), not worker (outside it)")
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

(def ok-roster {:swarm :ok :agents 2 :expired []})

(deftest the-reaper-marks-then-removes-after-the-grace-and-spares-a-claimed-run
  (let [{:keys [ctx e clock]} (world)
        rid (get-in (isolated/up! ctx (preset ctx "flow-payment") {}) [:ok :run])]
    ;; a live claim (a wave claim included) keeps the section touched: the
    ;; usage pass (swarm/observe!) renews :last-touch while it is live
    (swap! (:state ctx) assoc-in [:active "flow-payment@dcabc123" :claims "ling-1"] {:since 1000 :seen 1000})
    (swap! clock + (* 120 60000))
    (swap! (:state ctx) update-in [:active "flow-payment@dcabc123"] assoc :last-touch @clock)
    (let [pass (ops/reap! ctx)]
      (is (empty? (isolated/after-reap! ctx (:reaped pass) ok-roster)))
      (is (nil? (get-in @(:state ctx) [:runs rid :volumes-due-at])) "a claimed run is never marked"))
    (swap! (:state ctx) update-in [:active "flow-payment@dcabc123"] dissoc :claims)
    (swap! clock + (* 120 60000))
    (let [pass (ops/reap! ctx)
          ended (isolated/after-reap! ctx (:reaped pass) ok-roster)]
      (testing "step 1: the run ends, its volumes are only marked"
        (is (= [rid] (map :run ended)))
        (is (= (+ @clock (* 60 60000)) (:volumes-due-at (first ended))))
        (is (contains? (stub/volumes e) "dc-dcabc123_pgdata"))
        (is (= [{:run rid :name "dc-dcabc123_pgdata" :due-at (+ @clock (* 60 60000))}]
               (:stale-run-volumes (isolated/status ctx)))))
      (testing "the same pass sweeps nothing (not due yet)"
        (is (empty? (isolated/sweep-due! ctx)))
        (is (contains? (stub/volumes e) "dc-dcabc123_pgdata")))
      (testing "step 2: a later pass past the grace removes only the run's own volumes"
        (swap! clock + (* 61 60000))
        (is (= [rid] (map :run (isolated/sweep-due! ctx))))
        (is (not (contains? (stub/volumes e) "dc-dcabc123_pgdata")))
        (is (contains? (stub/volumes e) "dc_pgdata"))))))

(defn- reaped-run [ctx clock]
  (let [rid (get-in (isolated/up! ctx (preset ctx "flow-payment") {}) [:ok :run])]
    (swap! clock + (* 120 60000))
    [rid (:reaped (ops/reap! ctx))]))

(deftest a-doubtful-reap-never-marks
  (testing "an absent swarm"
    (let [{:keys [ctx e clock]} (world)
          [rid reaped] (reaped-run ctx clock)
          row (first (isolated/after-reap! ctx reaped {:swarm :absent :agents 0 :expired []}))]
      (is (= :doubtful-roster (:volumes-kept-why row)))
      (is (nil? (get-in @(:state ctx) [:runs rid :volumes-due-at])))
      (swap! clock + (* 24 60 60000))
      (is (empty? (isolated/sweep-due! ctx)) "a clock jump of a day removes nothing either")
      (is (contains? (stub/volumes e) "dc-dcabc123_pgdata"))))
  (testing "an empty roster"
    (let [{:keys [ctx clock]} (world)
          [rid reaped] (reaped-run ctx clock)]
      (isolated/after-reap! ctx reaped {:swarm :ok :agents 0 :expired []})
      (is (nil? (get-in @(:state ctx) [:runs rid :volumes-due-at])))))
  (testing "a claim that ended as :gone"
    (let [{:keys [ctx clock]} (world)
          rid (get-in (isolated/up! ctx (preset ctx "flow-payment") {}) [:ok :run])
          gone {:swarm :ok :agents 3 :expired [{:section (str "flow-payment@" rid) :agent "ling-9" :reason :gone}]}]
      (isolated/note-observed! ctx gone)
      (swap! clock + (* 120 60000))
      (let [row (first (isolated/after-reap! ctx (:reaped (ops/reap! ctx)) ok-roster))]
        (is (= :claim-gone (:volumes-kept-why row)))
        (is (nil? (get-in @(:state ctx) [:runs rid :volumes-due-at])))))))

(deftest up-claim-or-touch-cancels-a-pending-removal
  (let [{:keys [ctx e clock]} (world)
        [rid reaped] (reaped-run ctx clock)]
    (isolated/after-reap! ctx reaped ok-roster)
    (is (get-in @(:state ctx) [:runs rid :volumes-due-at]))
    (let [x (isolated/up! ctx (preset ctx "flow-payment") {:run rid})]
      (is (r/ok? x) (pr-str x)))
    (is (nil? (get-in @(:state ctx) [:runs rid :volumes-due-at])))
    (is (nil? (get-in @(:state ctx) [:runs rid :ended-at])))
    (swap! clock + (* 200 60000))
    (is (empty? (isolated/sweep-due! ctx)))
    (is (contains? (stub/volumes e) "dc-dcabc123_pgdata")))
  (let [{:keys [ctx clock]} (world)
        [rid reaped] (reaped-run ctx clock)]
    (isolated/after-reap! ctx reaped ok-roster)
    (isolated/cancel-due! ctx (str "flow-payment@" rid))
    (is (nil? (get-in @(:state ctx) [:runs rid :volumes-due-at])))))

(deftest b1-a-run-must-own-its-compose-project
  (let [{:keys [ctx e]} (world)]
    ;; a foreign project that happens to be named dc-web: a container and a volume
    (swap! (:world e) assoc-in [:containers "dc-web-pg-1"]
           {:project "dc-web" :service "pg" :labels {"com.docker.compose.project" "dc-web"} :state "running"})
    (stub/volumes! e {"dc-web_pgdata" {:labels {"com.docker.compose.project" "dc-web" "com.docker.compose.volume" "pgdata"}}})
    (let [x (isolated/up! ctx (preset ctx "flow-sync") {:run "web"})]
      (is (= :compose/could-not-run (:error x)))
      (is (= :project-exists (:reason x)))
      (is (= {:containers 1 :volumes 1 :networks 0} (:found x))))
    (is (= :project-exists (:reason (isolated/plan ctx (preset ctx "flow-sync") {:run "web"}))) "dry_run too")
    (is (not-any? #(= :up (first %)) (stub/calls e)) "nothing started")
    (testing "a hand-made record cannot aim the removal at the foreign volume"
      (swap! (:state ctx) assoc-in [:runs "web"] (assoc (iso/run-entry "dc" "web" :strip 1) :ended-at 2))
      (is (= :compose/run-still-active (:error (isolated/down-run! ctx "web" true)))
          "a container of the project exists")
      (swap! (:world e) update :containers dissoc "dc-web-pg-1")
      (let [d (isolated/down-run! ctx "web" true)]
        (is (= [] (get-in d [:ok :volumes :removed])) (pr-str d))
        (is (= ["dc-web_pgdata"] (get-in d [:ok :volumes :not-removable])))
        (is (contains? (stub/volumes e) "dc-web_pgdata"))))))

(deftest m2-another-agents-claim-blocks-volume-removal
  (let [{:keys [ctx e]} (world)
        rid (get-in (isolated/up! ctx (preset ctx "flow-payment") {}) [:ok :run])]
    (swap! (:state ctx) assoc-in [:active (str "flow-payment@" rid) :claims "ling-b"] {:since 1 :seen 1})
    (let [x (isolated/down-run! ctx rid true {:agent "ling-a"})]
      (is (= :compose/held-by-another-section (:error x)))
      (is (= [{:section (str "flow-payment@" rid) :agents ["ling-b"]}] (:holders x))))
    (is (contains? (stub/volumes e) "dc-dcabc123_pgdata"))
    (testing "the caller's own claim does not block; allow_shared overrides"
      (is (r/ok? (isolated/down-run! ctx rid true {:agent "ling-b"}))))))

(deftest m3-an-ended-record-cannot-remove-a-live-runs-volumes
  (let [{:keys [ctx e]} (world)
        rid (get-in (isolated/up! ctx (preset ctx "flow-payment") {}) [:ok :run])]
    (swap! (:state ctx) assoc-in [:runs rid :ended-at] 5)
    (let [x (isolated/down-run! ctx rid true)]
      (is (= :compose/run-still-active (:error x)) (pr-str x)))
    (is (contains? (stub/volumes e) "dc-dcabc123_pgdata"))))

(deftest m4-external-volumes-and-binds
  (let [{:keys [ctx e]} (world)]
    (stub/model! e "dc" (-> model
                            (assoc-in ["volumes" "ledger"] {"name" "ledger-prod" "external" true})
                            (update-in ["services" "worker" "volumes"] (fnil conj [])
                                       {"type" "volume" "source" "ledger" "target" "/l"})
                            (update-in ["services" "worker" "volumes"] conj
                                       {"type" "bind" "source" "/w/sync" "target" "/src" "read_only" true})))
    (let [x (isolated/up! ctx (preset ctx "flow-sync") {})]
      (is (= :compose/fresh-external-volume (:error x)))
      (is (= ["ledger"] (:volumes x))))
    (let [x (isolated/plan ctx (update (preset ctx "flow-sync") :profile/run-shared-volumes (fnil conj []) "ledger") {})]
      (is (r/ok? x) (pr-str x))
      (is (= ["ledger"] (get-in x [:ok :volumes :external])))
      (is (= [{:service "worker" :source "/w/sync" :target "/src" :rw? false}] (get-in x [:ok :binds]))))))

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

(def gen-base
  "Bases, some with dashes, so <base>-<run> collides with <base'>-<run'>."
  (gen/elements ["dc" "dc-x" "dc-xab" "sisf" "sisf-web" "a-b-c"]))

(defn- volume
  "A volume of compose project `proj` with key `k`, carrying the run
   ownership labels of [run base] when `owner` is given."
  [proj k owner]
  {:name (str proj "_" k)
   :labels (cond-> {"com.docker.compose.project" proj "com.docker.compose.volume" k}
             owner (assoc "hive.compose.run" (first owner) "hive.compose.base" (second owner)))})

(def gen-case
  "A run [base run], a world of volumes: the run's own (labelled), the same
   project unlabelled (it existed before the run), foreign projects whose
   name string-collides with <base>-<run> (base'-run' = base-run), prefix
   runs, the shared project's, a borrowed cache; and a record, possibly
   mutated."
  (gen/let [base gen-base
            run gen-run
            k (gen/elements ["pgdata" "fb" "cache"])
            pre (gen/elements ["ab" "abc"])
            mutate (gen/elements [nil :no-isolated :other-base :other-cp :conflict :missing])
            shared (gen/elements [[] ["cache"]])]
    (let [cp (iso/compose-project base run)
          ;; every split of cp at a dash other than the run's own one
          foreign (for [i (range (count cp))
                        :when (= \- (nth cp i))
                        :let [b (subs cp 0 i) r (subs cp (inc i))]
                        :when (not= [b r] [base run])]
                    [b r])
          vols (concat [(volume cp k [run base])
                        (volume cp "pre" nil)
                        (volume base k nil)
                        (volume (str cp "x") k [(str run "x") base])
                        (volume (iso/compose-project base (str pre run)) k [(str pre run) base])
                        {:name (str base "_cache") :labels {"com.docker.compose.project" base}}]
                       (for [[b r] foreign] (volume cp k [r b])))
          rec (assoc (iso/run-entry base run :strip 1) :shared-volumes shared)
          rec (case mutate
                :no-isolated (dissoc rec :isolated?)
                :other-base (assoc rec :base-project "zz")
                :other-cp (assoc rec :compose-project base)
                :conflict (assoc rec :conflict {:base-project "zz"})
                rec)]
      {:base base :run run :cp cp :vols (vec vols) :mutate mutate :shared shared
       :runs (if (= :missing mutate) {} {run rec})})))

(defspec removable-is-a-subset-of-the-runs-own-volumes 500
  (prop/for-all [{:keys [base run cp vols runs mutate shared]} gen-case]
    (let [out (iso/removable runs run vols)]
      (and (set/subset? (set out) (set vols))
           ;; only volumes that carry BOTH ownership labels of this run and its project label
           (every? #(and (= cp (get-in % [:labels "com.docker.compose.project"]))
                         (= run (get-in % [:labels "hive.compose.run"]))
                         (= base (get-in % [:labels "hive.compose.base"])))
                   out)
           ;; a foreign (base', run') sharing the project string is never taken
           (every? #(and (= run (get-in % [:labels "hive.compose.run"]))) out)
           ;; a borrowed cache volume is never removable
           (not-any? (set (map #(str base "_" %) shared)) (map :name out))
           ;; a mutated or missing record removes nothing
           (or (nil? mutate) (empty? out))
           ;; an unmutated record removes exactly the run's own labelled volume
           (or (some? mutate) (= [(str cp "_" (get-in (first vols) [:labels "com.docker.compose.volume"]))]
                                 (mapv :name out)))
           ;; never the shared project's volumes
           (empty? (iso/removable {base {:run base :base-project base :compose-project base :isolated? true}} base vols))))))

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

(deftest b1-an-image-outside-the-closure-may-be-missing
  (let [{:keys [ctx e]} (world)]
    ;; worker is not in flow-payment's closure (web + postgres); its image is absent
    (stub/model! e "dc" (assoc-in model ["services" "worker"] {"build" {"context" "."}}))
    (let [x (isolated/up! ctx (preset ctx "flow-payment") {})]
      (is (r/ok? x) (pr-str x))
      (is (not (contains? (get-in @(:state ctx) [:runs (get-in x [:ok :run]) :images]) "worker"))))))

(deftest b2-a-moved-tag-of-an-image-service-is-refused
  (let [{:keys [ctx e]} (world)
        rid (get-in (isolated/up! ctx (preset ctx "flow-payment") {}) [:ok :run])]
    (stub/images! e {"postgres:16" "sha256:pg2"})
    (let [x (isolated/up! ctx (preset ctx "flow-sync") {:run rid})]
      (is (= :image-changed (:reason x)) (pr-str x))
      (is (= "postgres" (:service x))))))

(deftest m1-a-new-run-that-could-not-come-up-is-ended
  (let [{:keys [ctx e]} (world)]
    (swap! (:world e) assoc :up-stderr "Error response from daemon: Bind for 0.0.0.0:5434 failed: port is already allocated")
    (let [x (isolated/up! ctx (preset ctx "flow-sync") {:ports :keep})
          rid (:run x)]
      (is (= :compose/could-not-run (:error x)))
      (is (get-in @(:state ctx) [:runs rid :ended-at]) "ended, not leaked")
      (is (get-in @(:state ctx) [:runs rid :volumes-due-at]) "volumes kept and marked as for any ended run"))))

(deftest m1-the-reaper-ends-an-orphaned-run
  (let [{:keys [ctx clock]} (world)]
    (swap! (:state ctx) assoc-in [:runs "dczzz111"] (iso/run-entry "dc" "dczzz111" :strip 1000))
    (is (empty? (isolated/end-orphans! ctx)) "younger than the TTL")
    (swap! clock + (* 61 60000))
    (is (= ["dczzz111"] (map :run (isolated/end-orphans! ctx))))
    (is (get-in @(:state ctx) [:runs "dczzz111" :ended-at]))))

(deftest m2-uncapped-services-are-assumed-not-free
  (let [{:keys [ctx]} (world)
        x (isolated/plan ctx (preset ctx "flow-payment") {})]
    (is (= 2 (get-in x [:ok :admission :assumed-services])) (pr-str x))
    (is (= 256 (get-in x [:ok :admission :need-mb])))))

(deftest m7-a-live-runs-ports-are-not-planned-twice
  (let [{:keys [ctx]} (world)]
    (is (r/ok? (isolated/up! ctx (preset ctx "flow-sync") {:ports {:offset 1000}})))
    (let [ctx2 (assoc ctx :rand-hex (constantly "def456"))
          x (isolated/up! ctx2 (preset ctx2 "flow-sync") {:ports {:offset 1000}})]
      (is (= :compose/invalid-param (:error x)))
      (is (= [6434] (map :host (:clashes x)))))))

(deftest m8-default-files-come-in-compose-pairs
  (let [{:keys [ctx dir]} (world)]
    (spit (io/file dir "compose.override.yaml") "services: {}\n")
    (let [x (isolated/plan ctx (dissoc (preset ctx "flow-sync") :profile/files) {})]
      (is (r/ok? x) (pr-str x))
      (is (= ["compose.yaml" "compose.override.yaml"] (get-in x [:ok :files]))))))

(deftest m3-a-configured-profile-id-may-not-hold-an-at
  (is (seq (hive-compose.schema/explain hive-compose.schema/Settings
                                        {:compose/profiles {"x@abc123" {:profile/id "x@abc123" :profile/dir "/w"}}}))))

(deftest a-wave-claim-keeps-a-run-past-its-ttl
  (let [{:keys [ctx e clock]} (world)
        rid (get-in (isolated/up! ctx (preset ctx "flow-payment") {}) [:ok :run])
        sid (str "flow-payment@" rid)
        p (:ok (ops/lookup ctx sid))
        c (hive-compose.pipeline.swarm/claim! ctx p "coordinator:x" {:ttl-minutes 480 :note "gate"})]
    (is (r/ok? c) (pr-str c))
    (is (get-in @(:state ctx) [:active sid :claims "coordinator:x" :until]) "an isolated section takes a wave claim")
    (dotimes [_ 3]
      (swap! clock + (* 120 60000))
      (let [observed (hive-compose.pipeline.swarm/observe! ctx)
            pass (ops/reap! ctx)]
        (is (empty? (isolated/after-reap! ctx (:reaped pass) observed)))
        (is (empty? (isolated/sweep-due! ctx)))))
    (is (contains? (:active @(:state ctx)) sid) "the section is never reaped under a live wave claim")
    (is (nil? (get-in @(:state ctx) [:runs rid :volumes-due-at])) "nor marked for volume removal")
    (is (contains? (stub/volumes e) "dc-dcabc123_pgdata"))))
