(ns hive-compose.scenario.s13-docker-test
  "Scenario S13 (hc-16c): resource caps on the real docker daemon, through the
   real pipeline (`hive-compose.pipeline.stacks`) and the CLI engine.

   S13a  a stack whose capped total exceeds :compose/budget is refused by
         plan and by stack-up, and nothing starts.
   S13b  a service that allocates past its 256m cap is OOM-killed with the
         cap hive-compose planned; `why` names the service and the cap; the
         flake history records :oom.
   S13c  sharing lowers the planned total: A+B sharing kafka < A+B private.

   One stack at a time. Every compose project and network carries a
   per-test stamp and is taken down (never with volumes) in `finally`. S13a
   and S13c start no container (plan only reads compose ps). The S13b
   fixture is test/resources/e2e/hog (busybox + hog.sh). Evidence is
   appended to /tmp/hc-verify-evidence.edn.

   hc-16b's vars (OOM diagnosis, why line, :oom flake class) are resolved at
   run time, so this namespace loads before hc-16b lands; a missing one
   fails its assertion by name.

   Run under the heavy-run fence:
     timeout 1200 ~/.local/bin/hive-heavy clojure -J-Xmx1g -M:integration \\
       -n hive-compose.scenario.s13-docker-test"
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [hive-compose.adapter.cli :as cli]
            [hive-compose.pipeline.stacks :as stacks]
            [hive-compose.port :as port]
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

(defn- ours
  "The members of `xs` that contain `stamp`: what this test made."
  [stamp xs]
  (set (filter #(str/includes? % stamp) xs)))

(defn- inspect [ctr fmt]
  (let [{:keys [exit out]} (docker "inspect" "-f" fmt ctr)] (when (zero? exit) out)))

(def evidence-file "/tmp/hc-verify-evidence.edn")

(defn- evidence! [scenario m]
  (spit evidence-file (str (pr-str (assoc m :scenario scenario :at (str (java.time.Instant/now)))) "\n") :append true))

;; ---------------------------------------------------------------------------
;; the world

(defn- dir-with [prefix files]
  (let [d (str (java.nio.file.Files/createTempDirectory prefix (make-array java.nio.file.attribute.FileAttribute 0)))]
    (doseq [[f s] files] (spit (io/file d f) s))
    d))

(def kafka-yml
  "A kafka provider as compose sees it. S13a and S13c only plan, so it never
   starts; the image is the one the S1-S7 scenarios run."
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

(def kafka-cap "768m")

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
                                          :resources {:mem kafka-cap}}}}
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
  (let [state-dir (str (java.nio.file.Files/createTempDirectory "hc13-state" (make-array java.nio.file.attribute.FileAttribute 0)))]
    {:engine (cli/make-engine {:compose/stack-dir (str state-dir "/stacks")})
     :settings (merge {:compose/default-ttl-minutes 60 :compose/default-idle-action :stop
                       :compose/up-timeout-ms 180000 :compose/profiles {}
                       :compose/projects projects :compose/stacks stacks-map}
                      extra-settings)
     :state (atom {:active {} :current nil})
     :now #(System/currentTimeMillis)}))

(defn- stamp [] (str (mod (System/currentTimeMillis) 100000000)))

(defn- cleanup!
  "Take down every compose project named with `stamp` and every hive-<run>
   network of `runs`. Never volumes."
  [c stamp runs]
  (let [e (:engine c)]
    (doseq [p (lines (:out (docker "compose" "ls" "-a" "-q"))) :when (str/includes? p stamp)]
      (port/-down-project! e {:project p :all? true}))
    (doseq [n (network-names) run runs :when (str/starts-with? n (str "hive-" run "-"))]
      (port/-network-rm! e n))))

(defn- plan-budget
  "The Budget (hc-16a schema/Budget) a `stacks/plan` answer carries."
  [p]
  (or (get-in p [:ok :budget]) (get-in p [:ok :diff :budget])))

(defn- hc16b
  "A var hc-16b provides, resolved at run time; nil when absent."
  [sym]
  (try (requiring-resolve sym) (catch Exception _ nil)))

;; ---------------------------------------------------------------------------
;; S13a

(deftest ^:integration s13a-over-budget-stack-starts-nothing
  "CLAIM: with :compose/budget 1g, stack abc plans one shared kafka (a, b)
   and one private kafka (c), 2 x 768m = 1536m capped: plan and stack-up
   answer :compose/over-budget with basis :budget, limit 1 GiB, total 1536
   MiB, over 512 MiB, both instances listed at 768 MiB; no run is recorded,
   docker ps -a, network ls and volume ls are what they were. Stack ab
   (768m) passes the same budget."
  (let [st (stamp) projects (kafka-world st) run (str "s13a" st)
        c (ctx projects kafka-stacks {:compose/budget "1g"})
        before {:ps (ps-all) :networks (network-names) :volumes (volume-names)}]
    (try
      (let [p (stacks/plan c {:stack "abc" :run run})
            up (stacks/stack-up! c {:stack "abc" :run run})
            after {:ps (ps-all) :networks (network-names) :volumes (volume-names)}
            ok (stacks/plan c {:stack "ab" :run (str run "ok")})]
        (evidence! :s13a {:plan-error (select-keys p [:error :basis :limit :total-mem :over :instances :message])
                          :up-error (select-keys up [:error :basis :total-mem :message])
                          :ps-before (count (:ps before)) :ps-after (count (:ps after))
                          :ps-unchanged? (= (:ps before) (:ps after))
                          :networks-unchanged? (= (:networks before) (:networks after))
                          :volumes-unchanged? (= (:volumes before) (:volumes after))
                          :ours-after {:ps (ours st (:ps after)) :networks (ours st (:networks after))}
                          :admitted-ab (some-> (plan-budget ok) :budget/total-mem)})
        (testing "plan refuses"
          (is (= :compose/over-budget (:error p)) (pr-str p))
          (is (= :budget (:basis p)))
          (is (= (* 1024 mib) (:limit p)))
          (is (= (* 1536 mib) (:total-mem p)))
          (is (= (* 512 mib) (:over p)))
          (is (= [(* 768 mib) (* 768 mib)] (map :mem (:instances p))))
          (is (str/includes? (str (:message p)) ":compose/budget")))
        (testing "stack-up refuses the same way, before anything starts"
          (is (= :compose/over-budget (:error up)) (pr-str up))
          (is (= (:total-mem p) (:total-mem up)))
          (is (nil? (get-in @(:state c) [:stacks run])) "no run recorded"))
        (testing "nothing of the run exists"
          (is (empty? (ours st (:ps after))) "no container")
          (is (empty? (ours run (:networks after))) "no network")
          (is (= (:volumes before) (:volumes after)) "volumes untouched")
          (is (empty? (remove (:ps before) (:ps after))) "docker ps -a gained nothing"))
        (testing "the stack under budget is admitted"
          (is (r/ok? ok) (pr-str ok))
          (is (= (* 768 mib) (:budget/total-mem (plan-budget ok))))))
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
            bs (plan-budget shared) bp (plan-budget private)]
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
  "The container's status once it is no longer running, or nil after `ms`."
  [ctr ms]
  (let [deadline (+ (System/currentTimeMillis) ms)]
    (loop []
      (let [s (inspect ctr "{{.State.Status}}")]
        (cond
          (#{"exited" "dead"} s) s
          (> (System/currentTimeMillis) deadline) nil
          :else (do (Thread/sleep 500) (recur)))))))

(deftest ^:integration s13b-capped-service-oom-explained
  "CLAIM: the hog provider has :resources {:mem \"256m\"} and no mem_limit of
   its own; stack-up writes the cap into the override, so its container has
   HostConfig.Memory 268435456; the hog allocates toward 512 MiB and is
   OOM-killed (State.OOMKilled true, exit 137) before it reaches it. The
   OOM diagnosis (hc-16b) names the service and the cap (`hog OOM-killed
   at 256m`), and the flake history classifies the run :oom, never :flaky."
  (is (some? hog-script) "test/resources/e2e/hog/hog.sh is on the classpath")
  (let [st (stamp) run (str "s13b" st)
        projects {"hogp" {:project/id "hogp" :project/dir (dir-with "hc13-hog" {"compose.yml" (hog-yml hog-script)})
                          :project/name (str "hp" st)
                          :project/provides {"hog" {:service "hog" :resources {:mem "256m"}}}}
                  "w" {:project/id "w" :project/dir (dir-with "hc13-w" {"compose.yml" idle-yml})
                       :project/name (str "cw" st) :project/needs {"hog" {}}}}
        c (ctx projects {"hog" {:stack/id "hog" :stack/consumers ["w"]}} {})
        ctr (str "hp" st "-hog-1")]
    (try
      (let [p (stacks/plan c {:stack "hog" :run run})
            up (stacks/stack-up! c {:stack "hog" :run run :wait? false})
            status (wait-exited ctr 120000)
            oom? (inspect ctr "{{.State.OOMKilled}}")
            exit (inspect ctr "{{.State.ExitCode}}")
            limit (inspect ctr "{{.HostConfig.Memory}}")
            logs (lines (:out (docker "logs" ctr)))
            diagnose (hc16b 'hive-compose.pipeline.oom/diagnose)
            why-line (hc16b 'hive-compose.promote.oom/why-line)
            diag (when diagnose (diagnose (:engine c) {:container ctr :service "hog" :cap (* 256 mib)}))
            text (when (and why-line diag (r/ok? diag)) (why-line (:ok diag)))
            outcomes (hc16b 'hive-compose.promote.flaky/run-outcomes)
            rank (hc16b 'hive-compose.promote.flaky/rank)
            config (hc16b 'hive-compose.promote.flaky/config)
            rows (when outcomes
                   (outcomes {:run/id run :suite/id "hog-suite" :project "w"
                              :exit (some-> exit parse-long)
                              :oom (when (and diag (r/ok? diag)) (:ok diag))}
                             0))
            ranked (when (and rank config (seq rows)) (rank (config nil) rows))]
        (evidence! :s13b {:planned-cap (get-in (plan-budget p) [:budget/instances "hogp/hog" :mem])
                          :status status :oom-killed oom? :exit exit :host-config-memory limit
                          :last-log (last (filter #(str/starts-with? % "hog:") logs))
                          :why text :flaky (mapv #(select-keys % [:case/id :class]) ranked)})
        (testing "the plan caps the hog at 256m"
          (is (r/ok? p) (pr-str p))
          (is (= (* 256 mib) (get-in (plan-budget p) [:budget/instances "hogp/hog" :mem]))))
        (testing "the kernel enforced the planned cap and killed the hog"
          (is (r/ok? up) (pr-str up))
          (is (= "exited" status))
          (is (= (str (* 256 mib)) limit) "the override wrote mem_limit from :resources")
          (is (= "true" oom?))
          (is (= "137" exit))
          (is (not-any? #(str/starts-with? % "hog: done") logs) "it never reached its target"))
        (testing "why names the service and the cap (hc-16b)"
          (is (some? diagnose) "hive-compose.pipeline.oom/diagnose (hc-16b) is not on the classpath")
          (is (some? why-line) "hive-compose.promote.oom/why-line (hc-16b) is not on the classpath")
          (is (and diag (r/ok? diag)) (pr-str diag))
          (is (str/includes? (str text) "hog OOM-killed at 256m") (pr-str text)))
        (testing "flake history records :oom, not a flake"
          (is (seq ranked) "no flaky rows")
          (is (some #(= :oom (:class %)) ranked) (pr-str ranked))
          (is (not-any? #(= :flaky (:class %)) ranked))))
      (is (r/ok? (stacks/stack-down! c {:run run})))
      (finally (cleanup! c st [run])))))
