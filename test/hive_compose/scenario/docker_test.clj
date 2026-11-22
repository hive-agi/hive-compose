(ns hive-compose.scenario.docker-test
  "Phase B: S1-S7 against the real docker daemon, through the real pipeline
   (`hive-compose.pipeline.stacks`) and the CLI engine. One stack at a time;
   every compose project and network is named with a per-test stamp and
   taken down (never with volumes) in `finally`. Fixture services are memory
   capped (kafka KRaft -Xmx512m). Evidence (container ids, IPs, plan text)
   is appended to /tmp/hc-verify-evidence.edn."
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [hive-compose.adapter.cli :as cli]
            [hive-compose.pipeline.stacks :as stacks]
            [hive-compose.port :as port]
            [hive-dsl.result :as r]
            [hive-system.secrets.core :as sec]
            [hive-system.secrets.protocols :as proto]))

;; SPDX-License-Identifier: MIT

(def kafka-yml
  "services:
  kafka:
    image: apache/kafka:3.9.1
    mem_limit: 900m
    environment:
      KAFKA_HEAP_OPTS: \"-Xmx512m -Xms256m\"
      KAFKA_NODE_ID: \"1\"
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: \"1\"
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: \"1\"
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: \"1\"
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: \"0\"
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: \"true\"
      KAFKA_NUM_PARTITIONS: \"1\"
    healthcheck:
      test: [\"CMD-SHELL\", \"/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1\"]
      interval: 5s
      timeout: 10s
      retries: 24
      start_period: 10s
")

(def client-yml
  "A consumer: the kafka image idling, so its CLI tools can produce and consume."
  "services:
  app:
    image: apache/kafka:3.9.1
    mem_limit: 400m
    entrypoint: [\"sleep\", \"1200\"]
")

(def slow-yml
  "A provider healthy only after ~8 s."
  "services:
  slow:
    image: busybox:1.36
    mem_limit: 32m
    command: [\"sh\", \"-c\", \"sleep 8; touch /tmp/ready; sleep 1200\"]
    healthcheck:
      test: [\"CMD-SHELL\", \"test -f /tmp/ready\"]
      interval: 1s
      retries: 60
")

(def idle-yml
  "services:
  app:
    image: busybox:1.36
    mem_limit: 32m
    command: [\"sleep\", \"1200\"]
")

;; ---------------------------------------------------------------------------
;; docker helpers (read-only inspection, plus exec into our own containers)

(defn- docker [& args]
  (let [{:keys [exit out err]} (apply sh/sh "docker" args)]
    {:exit exit :out (str/trim out) :err (str/trim err)}))

(defn- lines [s] (vec (remove str/blank? (str/split-lines s))))

(defn- ps-ids [] (set (lines (:out (docker "ps" "-aq" "--no-trunc")))))
(defn- network-names [] (set (lines (:out (docker "network" "ls" "--format" "{{.Name}}")))))
(defn- volume-names [] (set (lines (:out (docker "volume" "ls" "-q")))))

(defn- cid [name] (let [o (:out (docker "inspect" "-f" "{{.Id}}" name))] (when-not (str/blank? o) o)))

(defn- ip-on [container network]
  (:out (docker "inspect" "-f" (str "{{(index .NetworkSettings.Networks \"" network "\").IPAddress}}") container)))

(defn- networks-of [container]
  (set (str/split (:out (docker "inspect" "-f" "{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}" container)) #" ")))

(defn- env-of [container]
  (into {} (map #(vec (str/split % #"=" 2)))
        (lines (:out (docker "inspect" "-f" "{{range .Config.Env}}{{println .}}{{end}}" container)))))

(defn- resolve-in
  "The IPs `host` resolves to from inside `container` (getent, as the app would)."
  [container host]
  (set (map #(first (str/split % #"\s+")) (lines (:out (docker "exec" container "getent" "hosts" host))))))

(defn- sh-in [container script] (docker "exec" container "sh" "-c" script))

(def evidence-file "/tmp/hc-verify-evidence.edn")

(defn- evidence! [scenario m]
  (spit evidence-file (str (pr-str (assoc m :scenario scenario :at (str (java.time.Instant/now)))) "\n") :append true))

;; ---------------------------------------------------------------------------
;; the world

(defn- dir-with [prefix files]
  (let [d (str (java.nio.file.Files/createTempDirectory prefix (make-array java.nio.file.attribute.FileAttribute 0)))]
    (doseq [[f s] files] (spit (io/file d f) s))
    d))

(def secret-path "hcverify/sasl")
(def secret-value "stub-sasl-value-55ab")

(def stub-resolver
  {:pass (reify proto/ISecretBackend
           (backend-id [_] :pass)
           (fetch [_ k _] (if (= k secret-path)
                            (r/ok (sec/make-secret secret-value :pass k))
                            (r/err :pass/not-found {:key k})))
           (available? [_] (r/ok true)))})

(defn- world
  "Projects named with `stamp`: provider kp<stamp>, consumers a/b/c/d."
  [stamp]
  (let [kdir (dir-with "hcv-kafka" {"compose.yml" kafka-yml})
        cl (fn [id] (dir-with (str "hcv-" id) {"compose.yml" client-yml}))
        kafka (str "kp" stamp)]
    {:kafka kafka
     :projects
     {"infra" {:project/id "infra" :project/dir kdir :project/name kafka
               :project/provides {"kafka" {:service "kafka" :version "3.9.1"
                                           :env {"KAFKA_BROKERS" "kafka:9092"
                                                 "KAFKA_HEAP_OPTS" "-Xmx128m"
                                                 "KAFKA_SASL_PASSWORD" {:pass secret-path}}
                                           :tenancy {:env "KAFKA_TOPIC_PREFIX"}}}}
      "a" {:project/id "a" :project/dir (cl "a") :project/name (str "ca" stamp)
           :project/needs {"kafka" {:version "^3"}}}
      "b" {:project/id "b" :project/dir (cl "b") :project/name (str "cb" stamp)
           :project/needs {"kafka" {:version "^3"}}}
      "c" {:project/id "c" :project/dir (cl "c") :project/name (str "cc" stamp)
           :project/needs {"kafka" {:version "^3" :share? false}}}
      "d" {:project/id "d" :project/dir (cl "d") :project/name (str "cd" stamp)
           :project/needs {"kafka" {:version "^2"}}}}}))

(def stack-defs
  {"ab" {:stack/id "ab" :stack/consumers ["a" "b"]}
   "abc" {:stack/id "abc" :stack/consumers ["a" "b" "c"]}
   "abd" {:stack/id "abd" :stack/consumers ["a" "b" "d"]}
   "a" {:stack/id "a" :stack/consumers ["a"]}
   "b" {:stack/id "b" :stack/consumers ["b"]}
   "c" {:stack/id "c" :stack/consumers ["c"]}})

(def ^:dynamic *explicit-files?*
  "Give every project :project/files [\"compose.yml\"]. Without it the CLI
   engine's stack override becomes the ONLY -f, and compose no longer reads
   the project's compose.yml (finding s0)."
  true)

(defn- ctx [projects stacks-map]
  (let [projects (if *explicit-files?*
                   (update-vals projects #(assoc % :project/files ["compose.yml"]))
                   projects)
        state-dir (str (java.nio.file.Files/createTempDirectory "hcv-state" (make-array java.nio.file.attribute.FileAttribute 0)))]
    {:engine (cli/make-engine {:compose/stack-dir (str state-dir "/stacks") :compose/resolver stub-resolver})
     :settings {:compose/default-ttl-minutes 60 :compose/default-idle-action :stop
                :compose/up-timeout-ms 180000 :compose/profiles {}
                :compose/projects projects :compose/stacks stacks-map}
     :state (atom {:active {} :current nil})
     :now #(System/currentTimeMillis)}))

(defn- stamp [] (str (mod (System/currentTimeMillis) 100000000)))

(defn- cleanup!
  "Take down every compose project this test may have created (they all
   carry `stamp`) and every hive-<run> network of its runs. Never volumes."
  [c runs]
  (let [e (:engine c)
        names (set (map :project/name (vals (get-in c [:settings :compose/projects]))))
        projects (lines (:out (docker "compose" "ls" "-a" "-q")))]
    (doseq [p projects :when (some #(str/starts-with? p %) names)]
      (port/-down-project! e {:project p :all? true}))
    (doseq [n (network-names) run runs :when (str/starts-with? n (str "hive-" run "-"))]
      (port/-network-rm! e n))))

(defn- consumer-ctr [projects id run] (str (get-in projects [id :project/name]) "-" run "-app-1"))

;; ---------------------------------------------------------------------------

(deftest ^:integration s1-docker-union-sharing
  "CLAIM (docker): A and B need kafka ^3 -> ONE kafka container; plan says
   shared; from A and from B `kafka` resolves to that container's IP on each
   one's own view network; A produces to <prefixA>orders, B consumes the
   message from the same broker; B's own topic carries B's prefix."
  (let [st (stamp) {:keys [projects kafka]} (world st) run (str "s1" st) c (ctx projects stack-defs)]
    (try
      (let [p (stacks/plan c {:stack "ab" :run run})
            up (stacks/stack-up! c {:stack "ab" :run run})
            k (str kafka "-kafka-1")
            ca (consumer-ctr projects "a" run) cb (consumer-ctr projects "b" run)
            na (str "hive-" run "-a") nb (str "hive-" run "-b")]
        (is (= {:instances 1 :shared 1 :private 0} (select-keys (get-in p [:ok :diff :summary]) [:instances :shared :private])))
        (is (r/ok? up) (pr-str up))
        (let [kafkas (lines (:out (docker "ps" "--filter" "ancestor=apache/kafka:3.9.1" "--filter" (str "label=com.docker.compose.project=" kafka) "-q")))
              ka (resolve-in ca "kafka") kb (resolve-in cb "kafka")
              envA (env-of ca) envB (env-of cb)
              pa (get envA "KAFKA_TOPIC_PREFIX") pb (get envB "KAFKA_TOPIC_PREFIX")
              prod (sh-in ca (str "echo hello-from-a | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server $KAFKA_BROKERS --topic " pa "orders"))
              cons (sh-in cb (str "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server $KAFKA_BROKERS --topic " pa "orders --from-beginning --max-messages 1 --timeout-ms 40000"))
              _ (sh-in cb (str "echo inv-b | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server $KAFKA_BROKERS --topic " pb "invoices"))
              topics (set (lines (:out (sh-in k "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"))))]
          (evidence! :s1 {:plan (get-in p [:ok :text]) :kafka-container (cid k)
                          :kafka-ip {na (ip-on k na) nb (ip-on k nb)} :a-resolves ka :b-resolves kb
                          :prefixes [pa pb] :consumed (:out cons) :topics topics})
          (is (= 1 (count kafkas)) "exactly one kafka container")
          (is (= #{(ip-on k na)} ka) "A's kafka is the shared container on A's network")
          (is (= #{(ip-on k nb)} kb))
          (is (= #{na} (disj (networks-of ca) "")) "A is on its view network alone")
          (is (= "kafka:9092" (get envA "KAFKA_BROKERS") (get envB "KAFKA_BROKERS")))
          (is (= [(str run ".a.") (str run ".b.")] [pa pb]))
          (is (zero? (:exit prod)) (:err prod))
          (is (str/includes? (:out cons) "hello-from-a") (pr-str cons))
          (is (set/subset? #{(str pa "orders") (str pb "invoices")} topics))))
      (let [v (volume-names) d (stacks/stack-down! c {:run run})]
        (is (r/ok? d) (pr-str d))
        (is (set/subset? v (volume-names)) "no volume removed"))
      (finally (cleanup! c [run])))))

(deftest ^:integration s2-docker-opt-out
  "CLAIM (docker): C's :share? false gives it a private kafka under compose
   project <provider>-c-<run>; from C `kafka` resolves to the PRIVATE
   container's IP, a different container id than the shared one A and B
   resolve to; a topic C writes does not exist on the shared broker."
  (let [st (stamp) {:keys [projects kafka]} (world st) run (str "s2" st) c (ctx projects stack-defs)]
    (try
      (let [up (stacks/stack-up! c {:stack "abc" :run run})
            shared (str kafka "-kafka-1") private (str kafka "-c-" run "-kafka-1")
            ca (consumer-ctr projects "a" run) cc (consumer-ctr projects "c" run)
            nc (str "hive-" run "-c") na (str "hive-" run "-a")]
        (is (r/ok? up) (pr-str up))
        (let [rc (resolve-in cc "kafka") ra (resolve-in ca "kafka")
              _ (sh-in cc (str "echo c-only | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server $KAFKA_BROKERS --topic " run ".c.audit"))
              shared-topics (set (lines (:out (sh-in shared "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"))))
              private-topics (set (lines (:out (sh-in private "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"))))]
          (evidence! :s2 {:shared-id (cid shared) :private-id (cid private) :c-resolves rc :a-resolves ra
                          :private-ip (ip-on private nc) :shared-ip (ip-on shared na)
                          :private-networks (networks-of private) :shared-networks (networks-of shared)})
          (is (some? (cid private)))
          (is (not= (cid shared) (cid private)))
          (is (= #{(ip-on private nc)} rc) "C reaches its private kafka")
          (is (= #{(ip-on shared na)} ra) "A reaches the shared one")
          (is (not (contains? (networks-of shared) nc)) "the shared kafka is not on C's network")
          (is (contains? private-topics (str run ".c.audit")))
          (is (not (contains? shared-topics (str run ".c.audit"))))))
      (is (r/ok? (stacks/stack-down! c {:run run})))
      (is (nil? (cid (str kafka "-c-" run "-kafka-1"))) "the private kafka went with the run")
      (finally (cleanup! c [run])))))

(deftest ^:integration s3-docker-conflict-starts-nothing
  "CLAIM (docker): D needs kafka ^2 while 3.9.1 is provided: plan and
   stack-up are errors naming d and ^2, and docker ps -a, network ls and
   volume ls are byte-for-byte what they were."
  (let [st (stamp) {:keys [projects]} (world st) run (str "s3" st) c (ctx projects stack-defs)
        before [(ps-ids) (network-names) (volume-names)]]
    (try
      (let [p (stacks/plan c {:stack "abd" :run run})
            up (stacks/stack-up! c {:stack "abd" :run run})
            after [(ps-ids) (network-names) (volume-names)]]
        (evidence! :s3 {:error (:error up) :message (:message up) :consumers (:consumers up) :ranges (:ranges up)
                        :unchanged? (= before after)})
        (is (= :compose/incompatible-version (:error p) (:error up)))
        (is (= ["d"] (:consumers up)))
        (is (= {"d" "^2"} (:ranges up)))
        (is (= before after) "nothing started"))
      (finally (cleanup! c [run])))))

(deftest ^:integration s4-docker-refcount-release
  "CLAIM (docker): runs of stacks a, b and c at once; down a keeps the shared
   kafka container (same id, still running) for b; down b removes it; down c
   removes the private compose project; no volume ever disappears."
  (let [st (stamp) {:keys [projects kafka]} (world st) c (ctx projects stack-defs)
        ra (str "s4a" st) rb (str "s4b" st) rc (str "s4c" st)
        shared (str kafka "-kafka-1") private (str kafka "-c-" rc "-kafka-1")
        vols (volume-names)]
    (try
      (doseq [[s r] [["a" ra] ["b" rb] ["c" rc]]]
        (is (r/ok? (stacks/stack-up! c {:stack s :run r})) s))
      (let [id0 (cid shared)
            d1 (stacks/stack-down! c {:run ra})
            id1 (cid shared)
            d2 (stacks/stack-down! c {:run rb})
            id2 (cid shared)
            pid (cid private)
            d3 (stacks/stack-down! c {:run rc})]
        (evidence! :s4 {:shared-id-before id0 :after-down-a id1 :after-down-b id2 :private-id pid
                        :kept-a (get-in d1 [:ok :runs 0 :kept]) :released-b (get-in d2 [:ok :runs 0 :released])
                        :released-c (get-in d3 [:ok :runs 0 :released])})
        (is (every? r/ok? [d1 d2 d3]))
        (is (and id0 (= id0 id1)) "down a kept the shared kafka for b")
        (is (nil? id2) "down b released it")
        (is (nil? (cid private)) "c's private kafka went with c")
        (is (set/subset? vols (volume-names)) "no volume removed"))
      (finally (cleanup! c [ra rb rc])))))

(deftest ^:integration s5-docker-readiness-gating
  "CLAIM (docker): a provider healthy only after ~8 s; the dependent
   consumer container starts only after the provider reported healthy
   (consumer StartedAt >= the provider's first passing health probe)."
  (let [st (stamp)
        projects {"slow" {:project/id "slow" :project/dir (dir-with "hcv-slow" {"compose.yml" slow-yml})
                          :project/name (str "sp" st) :project/provides {"slow" {:service "slow"}}}
                  "w" {:project/id "w" :project/dir (dir-with "hcv-w" {"compose.yml" idle-yml})
                       :project/name (str "cw" st) :project/needs {"slow" {}}}}
        run (str "s5" st) c (ctx projects {"w" {:stack/id "w" :stack/consumers ["w"]}})]
    (try
      (let [up (stacks/stack-up! c {:stack "w" :run run})
            prov (str "sp" st "-slow-1") cons (str "cw" st "-" run "-app-1")
            healthy-at (->> (lines (:out (docker "inspect" "-f" "{{range .State.Health.Log}}{{.ExitCode}} {{.End}}{{println}}{{end}}" prov)))
                            (filter #(str/starts-with? % "0 ")) first (#(subs % 2)))
            prov-started (:out (docker "inspect" "-f" "{{.State.StartedAt}}" prov))
            cons-started (:out (docker "inspect" "-f" "{{.State.StartedAt}}" cons))
            t #(java.time.Instant/parse (str/replace (first (str/split % #" ")) #"(\.\d{9})\d*" "$1"))]
        (evidence! :s5 {:provider-started prov-started :provider-first-healthy healthy-at :consumer-started cons-started})
        (is (r/ok? up) (pr-str up))
        (is (some? healthy-at))
        (is (not (.isBefore (t cons-started) (t healthy-at))) "consumer started after the provider was healthy")
        (is (>= (.getSeconds (java.time.Duration/between (t prov-started) (t cons-started))) 7)))
      (is (r/ok? (stacks/stack-down! c {:run run})))
      (finally (cleanup! c [run])))))

(deftest ^:integration s6-docker-env-layering
  "CLAIM (docker): inside A's container HIVE_RUN_ID=<run>, hive.run.id=<run>
   in OTEL_RESOURCE_ATTRIBUTES, KAFKA_BROKERS as the provider wrote it; the
   pass reference is resolved only into the container env, never into the
   generated override file, the run state or the stack-up answer."
  (let [st (stamp) {:keys [projects]} (world st) run (str "s6" st)
        projects (assoc-in projects ["a" :project/env] {"OTEL_RESOURCE_ATTRIBUTES" "team=pay,hive.run.id=spoofed"})
        c (ctx projects stack-defs)]
    (try
      (let [up (stacks/stack-up! c {:stack "a" :run run})
            ctr (consumer-ctr projects "a" run)
            env (env-of ctr)
            override-dir (:override-dir (:engine c))
            overrides (when override-dir (map slurp (filter #(.isFile %) (file-seq (io/file override-dir)))))]
        (evidence! :s6 {:env (dissoc env "KAFKA_SASL_PASSWORD") :sasl-present? (contains? env "KAFKA_SASL_PASSWORD")})
        (is (r/ok? up) (pr-str up))
        (is (= run (get env "HIVE_RUN_ID")))
        (is (str/includes? (str (get env "OTEL_RESOURCE_ATTRIBUTES")) (str "hive.run.id=" run)))
        (is (= "kafka:9092" (get env "KAFKA_BROKERS")))
        (is (seq overrides) "an override was written")
        (is (not-any? #(str/includes? % secret-value) overrides) "no value in any override file")
        (is (not (str/includes? (pr-str @(:state c)) secret-value)) "no value in the run state")
        (is (not (str/includes? (pr-str up) secret-value))))
      (is (r/ok? (stacks/stack-down! c {:run run})))
      (finally (cleanup! c [run])))))

(deftest ^:integration s7-docker-parallel-runs
  "CLAIM (docker): stack ab under two run ids at once: disjoint container
   sets and view networks, each consumer only on its own run's network, no
   host port published by any run container, one shared kafka for both."
  (let [st (stamp) {:keys [projects kafka]} (world st) r1 (str "s7x" st) r2 (str "s7y" st) c (ctx projects stack-defs)]
    (try
      (is (r/ok? (stacks/stack-up! c {:stack "ab" :run r1})))
      (is (r/ok? (stacks/stack-up! c {:stack "ab" :run r2})))
      (let [by-run (fn [r] (set (lines (:out (docker "ps" "--filter" (str "label=hive.run=" r) "--format" "{{.Names}}")))))
            nets (fn [r] (set (filter #(str/starts-with? % (str "hive-" r "-")) (network-names))))
            c1 (by-run r1) c2 (by-run r2)
            ports (lines (:out (docker "ps" "--filter" "label=hive.stack=ab" "--format" "{{.Names}} {{.Ports}}")))]
        (evidence! :s7 {:run1 c1 :run2 c2 :nets1 (nets r1) :nets2 (nets r2) :ports ports
                        :kafka (cid (str kafka "-kafka-1"))})
        (is (= 2 (count c1) (count c2)))
        (is (empty? (set/intersection c1 c2)))
        (is (empty? (set/intersection (nets r1) (nets r2))))
        (doseq [ctr c1] (is (every? #(str/includes? % r1) (disj (networks-of ctr) "")) ctr))
        (is (not-any? #(str/includes? % "->") ports) "no host port published"))
      (is (r/ok? (stacks/stack-down! c {:run r1})))
      (is (r/ok? (stacks/stack-down! c {:run r2})))
      (finally (cleanup! c [r1 r2])))))

(deftest ^:integration s0-docker-a-project-without-files-runs
  "CLAIM (docker, repro for hc-6): a consumer project configured with only
   :project/dir (no :project/files), as most configs are, comes up in a
   stack. FAILS on 689dc10: the CLI engine passes the generated override as
   the ONLY -f, so compose stops reading the project's compose.yml and
   refuses 'service app has neither an image nor a build context'."
  (binding [*explicit-files?* false]
    (let [st (stamp) {:keys [projects]} (world st) run (str "s0" st) c (ctx projects stack-defs)]
      (try
        (let [up (stacks/stack-up! c {:stack "a" :run run})]
          (evidence! :s0 {:error (:error up) :stderr (:stderr up) :argv (:argv up)})
          (is (r/ok? up) (pr-str (select-keys up [:error :stderr :argv]))))
        (stacks/stack-down! c {:run run})
        (finally (cleanup! c [run]))))))
