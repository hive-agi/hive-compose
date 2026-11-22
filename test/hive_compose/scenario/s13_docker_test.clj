(ns hive-compose.scenario.s13-docker-test
  "Scenario S13 (hc-16c): resource caps on the real docker daemon, through the
   real pipeline (`hive-compose.pipeline.stacks`), the CLI engine and the
   host suite runner (`hive-compose.adapter.suite-runner`).

   S13a  a stack whose capped total exceeds :compose/budget is refused by
         plan (its :admission) and by stack-up, and nothing starts.
   S13b  a service that allocates past its 256m cap is OOM-killed with the
         cap hive-compose planned: (1) as a private stack instance capped by
         :resources, (2) as a container suite runner capped by
         :suite/resources. The suite fails, `why` names the service and the
         cap, and the flake history classes the run :oom, never :flaky.
   S13c  sharing lowers the planned total: A+B sharing kafka < A+B private.

   One stack at a time. Every compose project and network carries a
   per-test stamp and is taken down (never with volumes) in `finally`. S13a
   and S13c start no container (plan only reads compose ps). The S13b
   fixture is test/resources/e2e/hog (busybox + hog.sh). Evidence is
   appended to /tmp/hc-verify-evidence.edn.

   Needs on staging: #52 (caps, budget), #53 (suite runner, OOM finding),
   #56 (stack admission, the plan :budget block, capped private overrides),
   #51 (the :oom flake class). Run under the heavy-run fence:
     timeout 1200 ~/.local/bin/hive-heavy clojure -J-Xmx1g -M:integration \\
       -n hive-compose.scenario.s13-docker-test"
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [hive-compose.adapter.cli :as cli]
            [hive-compose.adapter.suite-runner :as sr]
            [hive-compose.boundary.runs :as runs]
            [hive-compose.pipeline.stacks :as stacks]
            [hive-compose.pipeline.why :as why]
            [hive-compose.port :as port]
            [hive-compose.promote.flaky :as flaky]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def mib (* 1024 1024))

;; ---------------------------------------------------------------------------
;; docker helpers (read-only inspection)

(defn- docker [& args]
  (let [{:keys [exit out err]} (apply sh/sh "docker" args)]
    {:exit exit :out (str/trim out) :err (str/trim err)}))

(defn- lines [s] (vec (remove str/blank? (str/split-lines s))))

(defn- ps-all [] (set (lines (:out (docker "ps" "-a" "--no-trunc" "--format" "{{.ID}} {{.Names}}")))))
(defn- network-names [] (set (lines (:out (docker "network" "ls" "--format" "{{.Name}}")))))
(defn- volume-names [] (set (lines (:out (docker "volume" "ls" "-q")))))

(defn- ours [stamp xs] (set (filter #(str/includes? % stamp) xs)))

(defn- inspect [ctr fmt]
  (let [{:keys [exit out]} (docker "inspect" "-f" fmt ctr)] (when (zero? exit) out)))

(def evidence-file "/tmp/hc-verify-evidence.edn")

(defn- evidence! [scenario m]
  (spit evidence-file (str (pr-str (assoc m :scenario scenario :at (str (java.time.Instant/now)))) "\n") :append true))

;; ---------------------------------------------------------------------------
;; the world

(defn- tmp-dir [prefix]
  (str (java.nio.file.Files/createTempDirectory prefix (make-array java.nio.file.attribute.FileAttribute 0))))

(defn- dir-with [prefix files]
  (let [d (tmp-dir prefix)]
    (doseq [[f s] files] (spit (io/file d f) s))
    d))

(def kafka-yml
  "A kafka provider as compose sees it. S13a and S13c only plan, so it never
   starts."
  "services:
  kafka:
    image: apache/kafka:3.9.1
")

(def idle-yml
  "services:
  app:
    image: busybox:1.36
    command: [\"sleep\", \"1200\"]
")

(def hog-script
  "test/resources/e2e/hog/hog.sh, from the classpath."
  (some-> (io/resource "e2e/hog/hog.sh") io/file .getAbsolutePath))

(defn- hog-yml
  "The hog with NO mem_limit of its own: the only cap it can get is the one
   hive-compose writes into the stack override from :resources."
  [script]
  (str "services:
  hog:
    image: busybox:1.36
    entrypoint: [\"sh\", \"/hog/hog.sh\"]
    environment:
      HOG_TARGET_MB: \"512\"
      HOG_STEP_MB: \"16\"
      HOG_EXIT: \"1\"
    volumes:
      - " script ":/hog/hog.sh:ro
"))

(defn- kafka-world
  "Provider infra gives kafka capped at 768m; a and b share it, c and the
   private twins ap and bp opt out."
  [stamp]
  (let [kdir (dir-with "hc13-kafka" {"compose.yml" kafka-yml})
        cl (fn [id] (dir-with (str "hc13-" id) {"compose.yml" idle-yml}))
        need {"kafka" {:version "^3"}}
        private {"kafka" {:version "^3" :share? false}}
        consumer (fn [id needs] {:project/id id :project/dir (cl id) :project/name (str "c" id stamp)
                                 :project/needs needs})]
    {"infra" {:project/id "infra" :project/dir kdir :project/name (str "kp" stamp)
              :project/provides {"kafka" {:service "kafka" :version "3.9.1"
                                          :env {"KAFKA_BROKERS" "kafka:9092"}
                                          :tenancy {:env "KAFKA_TOPIC_PREFIX"}
                                          :resources {:mem "768m"}}}}
     "a" (consumer "a" need)
     "b" (consumer "b" need)
     "c" (consumer "c" private)
     "ap" (consumer "ap" private)
     "bp" (consumer "bp" private)}))

(def kafka-stacks
  {"ab" {:stack/id "ab" :stack/consumers ["a" "b"]}
   "abc" {:stack/id "abc" :stack/consumers ["a" "b" "c"]}
   "abp" {:stack/id "abp" :stack/consumers ["ap" "bp"]}})

(defn- ctx [projects stacks-map extra-settings]
  (let [state-dir (tmp-dir "hc13-state")]
    {:engine (cli/make-engine {:compose/stack-dir (str state-dir "/stacks")})
     :state-dir state-dir
     :settings (merge {:compose/default-ttl-minutes 60 :compose/default-idle-action :stop
                       :compose/up-timeout-ms 180000 :compose/profiles {}
                       :compose/projects projects :compose/stacks stacks-map}
                      extra-settings)
     :state (atom {:active {} :current nil})
     :now #(System/currentTimeMillis)}))

(defn- stamp [] (str (mod (System/currentTimeMillis) 100000000)))

(defn- cleanup!
  "Take down every compose project named with `stamp`, every hive-<run>
   network of `runs` and every container named with `stamp`. Never volumes."
  [c stamp runs]
  (let [e (:engine c)]
    (doseq [p (lines (:out (docker "compose" "ls" "-a" "-q"))) :when (str/includes? p stamp)]
      (port/-down-project! e {:project p :all? true}))
    (doseq [n (lines (:out (docker "ps" "-a" "--format" "{{.Names}}"))) :when (str/includes? n stamp)]
      (docker "rm" "-f" n))
    (doseq [n (network-names) run runs :when (str/starts-with? n (str "hive-" run "-"))]
      (port/-network-rm! e n))))

;; ---------------------------------------------------------------------------
;; S13a

(deftest ^:integration s13a-over-budget-stack-starts-nothing
  "CLAIM: with :compose/budget 1g, stack abc plans one shared kafka (a, b)
   and one private kafka (c), 2 x 768m = 1536m capped. plan's :admission and
   stack-up answer :compose/over-budget with basis :budget, limit 1 GiB,
   total 1536 MiB, over 512 MiB, both instances at 768 MiB; no run is
   recorded, and docker ps -a, network ls and volume ls are what they were.
   Stack ab (768m) is admitted under the same budget."
  (let [st (stamp) projects (kafka-world st) run (str "s13a" st)
        c (ctx projects kafka-stacks {:compose/budget "1g"})
        before {:ps (ps-all) :networks (network-names) :volumes (volume-names)}]
    (try
      (let [p (stacks/plan c {:stack "abc" :run run})
            adm (get-in p [:ok :admission])
            up (stacks/stack-up! c {:stack "abc" :run run})
            after {:ps (ps-all) :networks (network-names) :volumes (volume-names)}
            ok (stacks/plan c {:stack "ab" :run (str run "ok")})]
        (evidence! :s13a {:admission (select-keys adm [:error :basis :limit :total-mem :over :instances :message])
                          :up-error (select-keys up [:error :basis :total-mem :message])
                          :plan-text (get-in p [:ok :text])
                          :ps-before (count (:ps before)) :ps-after (count (:ps after))
                          :ps-unchanged? (= (:ps before) (:ps after))
                          :networks-unchanged? (= (:networks before) (:networks after))
                          :volumes-unchanged? (= (:volumes before) (:volumes after))
                          :admitted-ab {:admission (get-in ok [:ok :admission])
                                        :total (get-in ok [:ok :budget :budget/total-mem])}})
        (testing "plan says it would refuse"
          (is (r/ok? p) (pr-str p))
          (is (= :compose/over-budget (:error adm)) (pr-str adm))
          (is (= :budget (:basis adm)))
          (is (= (* 1024 mib) (:limit adm)))
          (is (= (* 1536 mib) (:total-mem adm)))
          (is (= (* 512 mib) (:over adm)))
          (is (= [(* 768 mib) (* 768 mib)] (map :mem (:instances adm))))
          (is (str/includes? (str (get-in p [:ok :text])) "REFUSED")))
        (testing "stack-up refuses the same way, before anything starts"
          (is (= :compose/over-budget (:error up)) (pr-str up))
          (is (= (:total-mem adm) (:total-mem up)))
          (is (nil? (get-in @(:state c) [:stacks run])) "no run recorded"))
        (testing "nothing of the run exists"
          (is (empty? (ours st (:ps after))) "no container")
          (is (empty? (ours run (:networks after))) "no network")
          (is (= (:volumes before) (:volumes after)) "volumes untouched")
          (is (empty? (remove (:ps before) (:ps after))) "docker ps -a gained nothing"))
        (testing "the stack under budget is admitted"
          (is (r/ok? ok) (pr-str ok))
          (is (= :ok (get-in ok [:ok :admission])))
          (is (= (* 768 mib) (get-in ok [:ok :budget :budget/total-mem])))))
      (finally (cleanup! c st [run])))))

;; ---------------------------------------------------------------------------
;; S13c

(deftest ^:integration s13c-sharing-lowers-the-planned-total
  "CLAIM: stack ab (a and b share one kafka) plans 768 MiB, unshared-mem
   1536 MiB, saved-mem 768 MiB; stack abp (the same two consumers, each
   :share? false) plans 1536 MiB and saves nothing. Shared < private, the
   shared plan's saving is exactly the difference, and the plan text prints
   both totals."
  (let [st (stamp) projects (kafka-world st) run (str "s13c" st)
        c (ctx projects kafka-stacks {})]
    (try
      (let [shared (stacks/plan c {:stack "ab" :run run})
            private (stacks/plan c {:stack "abp" :run (str run "p")})
            bs (get-in shared [:ok :budget]) bp (get-in private [:ok :budget])]
        (evidence! :s13c {:shared (select-keys bs [:budget/total-mem :budget/unshared-mem :budget/saved-mem])
                          :private (select-keys bp [:budget/total-mem :budget/unshared-mem :budget/saved-mem])
                          :shared-text (get-in shared [:ok :text]) :private-text (get-in private [:ok :text])})
        (is (r/ok? shared) (pr-str shared))
        (is (r/ok? private) (pr-str private))
        (is (= (* 768 mib) (:budget/total-mem bs)))
        (is (= (* 1536 mib) (:budget/unshared-mem bs)))
        (is (= (* 768 mib) (:budget/saved-mem bs)))
        (is (= (* 1536 mib) (:budget/total-mem bp)))
        (is (= 0 (:budget/saved-mem bp)))
        (is (and bs bp (< (:budget/total-mem bs) (:budget/total-mem bp))) "sharing plans less than private")
        (is (and bs bp (= (- (:budget/total-mem bp) (:budget/total-mem bs)) (:budget/saved-mem bs))))
        (is (= 1 (count (:budget/instances bs))))
        (is (= 2 (count (:budget/instances bp))))
        (is (str/includes? (str (get-in shared [:ok :text])) "total capped memory 768m"))
        (is (str/includes? (str (get-in private [:ok :text])) "total capped memory 1.5g")))
      (finally (cleanup! c st [run (str run "p")])))))

;; ---------------------------------------------------------------------------
;; S13b

(defn- wait-exited
  "The container's status once it no longer runs, or nil after `ms`."
  [ctr ms]
  (let [deadline (+ (System/currentTimeMillis) ms)]
    (loop []
      (let [s (inspect ctr "{{.State.Status}}")]
        (cond
          (#{"exited" "dead"} s) s
          (> (System/currentTimeMillis) deadline) nil
          :else (do (Thread/sleep 500) (recur)))))))

(deftest ^:integration s13b-1-capped-stack-instance-oom-killed
  "CLAIM: provider hogp gives `hog` with :resources {:mem \"256m\"} and no
   mem_limit of its own; consumer w needs it :share? false, so it runs as a
   private instance whose override carries the cap (hc-16a: a shared
   instance's cap is admitted but not written). Its container has
   HostConfig.Memory 268435456, allocates toward 512 MiB and is OOM-killed
   (OOMKilled true, exit 137) before it gets there; the planned cap is the
   enforced one."
  (is (some? hog-script) "test/resources/e2e/hog/hog.sh is on the classpath")
  (let [st (stamp) run (str "s13b" st)
        projects {"hogp" {:project/id "hogp" :project/dir (dir-with "hc13-hog" {"compose.yml" (hog-yml hog-script)})
                          :project/name (str "hp" st)
                          :project/provides {"hog" {:service "hog" :resources {:mem "256m"}}}}
                  "w" {:project/id "w" :project/dir (dir-with "hc13-w" {"compose.yml" idle-yml})
                       :project/name (str "cw" st) :project/needs {"hog" {:share? false}}}}
        c (ctx projects {"hog" {:stack/id "hog" :stack/consumers ["w"]}} {})]
    (try
      (let [p (stacks/plan c {:stack "hog" :run run})
            iid (first (keys (get-in p [:ok :budget :budget/instances])))
            up (stacks/stack-up! c {:stack "hog" :run run :wait? false})
            ctr (first (filter #(and (str/includes? % (str "hp" st)) (str/ends-with? % "-hog-1"))
                               (lines (:out (docker "ps" "-a" "--format" "{{.Names}}")))))
            status (when ctr (wait-exited ctr 120000))
            oom? (some-> ctr (inspect "{{.State.OOMKilled}}"))
            exit (some-> ctr (inspect "{{.State.ExitCode}}"))
            limit (some-> ctr (inspect "{{.HostConfig.Memory}}"))
            logs (when ctr (lines (str (:out (docker "logs" ctr)) "\n" (:err (docker "logs" ctr)))))]
        (evidence! :s13b-stack {:instance iid :planned-cap (get-in p [:ok :budget :budget/instances iid :mem])
                                :plan-text (get-in p [:ok :text]) :container ctr :status status
                                :oom-killed oom? :exit exit :host-config-memory limit
                                :last-log (last (filter #(str/starts-with? % "hog:") logs))})
        (is (r/ok? p) (pr-str p))
        (is (= (* 256 mib) (get-in p [:ok :budget :budget/instances iid :mem])) "the plan caps the hog at 256m")
        (is (r/ok? up) (pr-str up))
        (is (some? ctr) "the private hog container exists")
        (is (= "exited" status))
        (is (= (str (* 256 mib)) limit) "the override wrote mem_limit from :resources")
        (is (= "true" oom?))
        (is (= "137" exit))
        (is (not-any? #(str/starts-with? % "hog: done") logs) "it never reached its target")
        (is (r/ok? (stacks/stack-down! c {:run run}))))
      (finally (cleanup! c st [run])))))

(deftest ^:integration s13b-2-capped-suite-runner-oom-explained
  "CLAIM: suite hog-suite (image hive-compose-e2e/hog:dev, built from
   test/resources/e2e/hog) runs through the host suite runner with
   :resources {:mem 256m}: it is OOM-killed (exit 137) and the answer
   carries the finding `hog-suite/w OOM-killed at 256m`. Its cell result
   is :error, written to runs/<run>/hog-suite/w/result.edn. `compose why`
   on that run names the runner and the cap, and the flake history classes
   it :oom, never :flaky, also over several runs that alternate pass and OOM."
  (let [st (stamp) run (str "s13bs" st)
        build (docker "build" "-q" "-t" "hive-compose-e2e/hog:dev"
                      (str (.getParentFile (io/file hog-script))))
        c (ctx {} {} {})
        state-dir (:state-dir c)
        runner (sr/make-runner {})
        spec {:runner :container :image "hive-compose-e2e/hog:dev" :name (str "hc13-hog-" st)
              :test-id "hog-suite/w" :env {"HOG_TARGET_MB" "512" "HOG_EXIT" "1"}
              :mounts [] :labels {"hive.run" run} :timeout-ms 120000
              :resources {:mem (* 256 mib)}}]
    (try
      (is (zero? (:exit build)) (:err build))
      (let [res (port/-run! runner spec)
            ans (:ok res)
            finding (:oom ans)
            cell (cond-> {:run/id run :suite/id "hog-suite" :project "w" :exit (:exit ans)
                          :status (if (= 0 (:exit ans)) :passed :error)
                          :started-at (:started-at ans) :finished-at (:finished-at ans)}
                   finding (assoc :oom finding))
            written (runs/write-result! state-dir cell)
            w (why/why! c {:test "hog-suite/w" :run run})
            why-text (str (get-in w [:ok :why/summary]) "\n" (get-in w [:ok :why/markdown]))
            rows (vec (mapcat (fn [[i r]] (flaky/run-outcomes (assoc cell :run/id r :oom (when (even? i) finding)
                                                                     :exit (if (even? i) 137 0))
                                                              (* 1000 i)))
                              (map-indexed vector [(str run "a") (str run "b") (str run "c") (str run "d")])))
            ranked (flaky/rank (flaky/config nil) rows)]
        (evidence! :s13b-suite {:runner (select-keys ans [:exit :cap :timed-out?]) :oom finding
                                :output-tail (take-last 3 (:output ans))
                                :written written :why (if (r/ok? w) (get-in w [:ok :why/summary]) (select-keys w [:error :hint]))
                                :flaky (mapv #(select-keys % [:case :class :ooms :last-oom]) ranked)})
        (testing "the runner was OOM-killed at its cap"
          (is (r/ok? res) (pr-str res))
          (is (= 137 (:exit ans)))
          (is (= (* 256 mib) (get-in ans [:cap :mem])))
          (is (= "hog-suite/w OOM-killed at 256m" (:oom/line finding)) (pr-str finding)))
        (testing "the suite failed"
          (is (= :error (:status cell)))
          (is (r/ok? written) (pr-str written)))
        (testing "why names the service and the cap"
          (is (r/ok? w) (pr-str (select-keys w [:error :hint :known])))
          (is (str/includes? why-text "OOM-killed at 256m") why-text))
        (testing "flake history: :oom, never :flaky"
          (is (seq ranked))
          (is (some #(= :oom (:class %)) ranked) (pr-str ranked))
          (is (not-any? #(= :flaky (:class %)) ranked) (pr-str ranked))))
      (finally (cleanup! c st [run])))))
