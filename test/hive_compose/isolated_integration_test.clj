(ns hive-compose.isolated-integration-test
  "Isolated section runs (FUN-D) against the REAL docker daemon, through the
   real CLI adapter and the pipeline entry points. Every object carries the
   throwaway prefix hcit<6 hex>; cleanup removes only those. Run with:

     hive-clj -J-Xmx2g -M:integration -n hive-compose.isolated-integration-test"
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [hive-compose.adapter.cli :as cli]
            [hive-compose.pipeline.isolated :as isolated]
            [hive-compose.pipeline.ops :as ops]
            [hive-compose.port :as port]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def offset 1000)

(defn- free-port []
  (with-open [s (java.net.ServerSocket. 0)] (.getLocalPort s)))

(defn- compose-yml
  "The fixture: web (image-only, container_name, loopback + ephemeral +
   interpolated host ports, a named volume with a marker), app (build-only,
   container_name, depends_on web), an explicit-name volume, a profiled
   service and a service with several ports."
  [prefix ports extra-line]
  (let [{:keys [web pg e1 e2 e3]} ports]
    (str "services:
  web:
    image: busybox:1.36
    container_name: " prefix "-web
    command: [\"sh\", \"-c\", \"echo shared > /data/marker-${HIVE_RUN:-shared}; sleep 600\"]
    environment:
      PUBLIC_URL: http://localhost:${HIVE_PORT_WEB_8080:-8080}
      OFFSET: \"${HIVE_PORT_OFFSET:-none}\"
    ports:
      - \"127.0.0.1:" web ":8080\"
      - \"127.0.0.1:0:9000\"
      - \"${PG_HOST_PORT:-" pg "}:5432\"
    volumes:
      - data:/data
      - fixed:/fixed
  app:
    build: .
    container_name: " prefix "-app
    command: [\"sleep\", \"600\"]
    depends_on: [web]
  extra:
    image: busybox:1.36
    command: [\"sleep\", \"600\"]
    profiles: [e2e]
  envoy:
    image: busybox:1.36
    command: [\"sleep\", \"600\"]
    profiles: [e2e]
    ports: [\"" e1 ":10000\", \"" e2 ":10001\", \"" e3 ":9901\"]
volumes:
  data: {}
  fixed:
    name: " prefix "-fixed
" extra-line)))

(defn- sh! [& args] (apply sh/sh args))

(defn- lines [s] (vec (remove str/blank? (str/split-lines (str s)))))

(defn- objects
  "Every container, volume and network whose name holds `prefix`."
  [prefix]
  {:containers (lines (:out (sh! "docker" "ps" "-a" "--filter" (str "name=" prefix) "--format" "{{.Names}}")))
   :volumes (lines (:out (sh! "docker" "volume" "ls" "--filter" (str "name=" prefix) "--format" "{{.Name}}")))
   :networks (lines (:out (sh! "docker" "network" "ls" "--filter" (str "name=" prefix) "--format" "{{.Name}}")))})

(defn- cleanup!
  "Our own projects only: every compose project whose name starts with
   `prefix`, `down -v`; then the explicit-name volume and the built images."
  [prefix dir]
  (doseq [p (distinct (concat [prefix]
                              (lines (:out (sh! "docker" "ps" "-a" "--filter" (str "label=com.docker.compose.project")
                                                "--format" "{{.Label \"com.docker.compose.project\"}}")))
                              (lines (:out (sh! "docker" "volume" "ls" "--format" "{{.Label \"com.docker.compose.project\"}}")))))
          :when (str/starts-with? p prefix)]
    (sh! "docker" "compose" "-p" p "--project-directory" dir "-f" (str dir "/compose.yml")
         "--profile" "*" "down" "-v" "--remove-orphans" :dir dir))
  (doseq [v (:volumes (objects prefix)) :when (str/starts-with? v prefix)]
    (sh! "docker" "volume" "rm" v))
  (doseq [n (:networks (objects prefix)) :when (str/starts-with? n prefix)]
    (sh! "docker" "network" "rm" n))
  (doseq [i (lines (:out (sh! "docker" "images" "--format" "{{.Repository}}:{{.Tag}}")))
          :when (str/starts-with? i prefix)]
    (sh! "docker" "rmi" "-f" i)))

(defn- fixture [prefix ports extra]
  (let [dir (str "/tmp/hcit/" prefix)]
    (.mkdirs (io/file dir))
    (spit (io/file dir "compose.yml") (compose-yml prefix ports extra))
    (spit (io/file dir "Dockerfile") "FROM busybox:1.36\nLABEL hcit=1\n")
    dir))

(defn- ctx-of [prefix dir]
  (let [engine (cli/make-engine {:compose/stack-dir (str dir "/state/stacks")})]
    {:engine engine
     :settings {:compose/projects {prefix {:project/id prefix :project/dir dir :project/name prefix}}
                :compose/profiles {"web" {:profile/id "web" :profile/dir dir :profile/project prefix
                                          :profile/files ["compose.yml"] :profile/services ["app"]
                                          :profile/compose-profiles ["e2e"]
                                          :profile/env {"WEB_SRC" dir}}
                                   "extra" {:profile/id "extra" :profile/dir dir :profile/project prefix
                                            :profile/files ["compose.yml"] :profile/services ["extra" "envoy"]
                                            :profile/compose-profiles ["e2e"]}}
                :compose/default-ttl-minutes 60
                :compose/default-idle-action :stop
                :compose/state-file (str dir "/state/state.edn")}
     :state (atom {:active {} :current nil})
     :now #(System/currentTimeMillis)}))

(defn- preset [c id] (:ok (ops/lookup c id)))

(defn- exec-out [container & cmd]
  (str/trim (:out (apply sh! "docker" "exec" container cmd))))

(defn- run-container [prefix run svc] (str prefix "-" run "-" svc "-1"))

(deftest ^:integration isolated-run-against-docker
  (let [prefix (str "hcit" (format "%06x" (rand-int 0xffffff)))
        ports {:web (free-port) :pg (free-port) :e1 (free-port) :e2 (free-port) :e3 (free-port)}
        dir (fixture prefix ports "")
        c (ctx-of prefix dir)
        e (:engine c)
        shared {:profile/id "shared" :profile/dir dir :profile/project prefix :profile/files ["compose.yml"]
                :profile/compose-profiles ["e2e"]}]
    (try
      (testing "1. the shared project up, its image built"
        (is (r/ok? (port/-up! e shared ["web" "app"])))
        (is (= "shared" (exec-out (str prefix "-web") "cat" "/data/marker-shared"))))
      (let [built (str prefix "-app")
            built-id (str/trim (:out (sh! "docker" "image" "inspect" built "--format" "{{.Id}}")))
            up (isolated/up! c (preset c "web") {:ports {:offset offset}})
            rid (get-in up [:ok :run])
            cp (str prefix "-" rid)]
        (testing "2. an isolated run beside the shared project"
          (is (r/ok? up) (pr-str up))
          (is (str/starts-with? built-id "sha256:"))
          (is (= #{"web" "app"} (set (:ok (port/-running e shared)))) "the shared project still runs")
          (let [row (first (filter #(= rid (:run %)) (:runs (isolated/status c))))]
            (is (= {"8080" (+ (:web ports) offset) "5432" (+ (:pg ports) offset)}
                   (dissoc (get-in row [:ports "web"]) "9000"))
                "the engine's bindings: loopback and interpolated ports moved by the offset")
            (is (pos? (get-in row [:ports "web" "9000"] 0)) "the ephemeral bind was read back after up")
            (is (= built-id (get-in row [:images "app"])) "the run runs the shared project's built image")
            (is (= built-id (get-in row [:pinned-images "app"])))
            (is (seq (get-in row [:volumes :created])))
            (is (every? #(str/starts-with? % cp) (get-in row [:volumes :created])) "only the run's own volumes")
            (is (= "127.0.0.1" (str/trim (:out (sh! "docker" "inspect" (run-container prefix rid "web") "--format"
                                                     "{{(index (index .NetworkSettings.Ports \"8080/tcp\") 0).HostIp}}"))))))
          (is (= 0 (count (lines (:out (sh! "docker" "images" "--filter" (str "reference=" cp "*") "-q"))))) "no image built for the run")
          (testing "fresh state: the shared marker is not in the run's volume, the run's own is"
            (Thread/sleep 1000)
            (let [ls (exec-out (run-container prefix rid "web") "ls" "/data")]
              (is (not (str/includes? ls "marker-shared")) ls)
              (is (str/includes? ls (str "marker-" rid)) ls))))
        (testing "8. run variables interpolate"
          (is (= (str "http://localhost:" (+ (:web ports) offset))
                 (exec-out (run-container prefix rid "web") "printenv" "PUBLIC_URL")))
          (is (= (str offset) (exec-out (run-container prefix rid "web") "printenv" "OFFSET"))))
        (testing "B1: the run's volumes carry the ownership labels"
          (doseq [v (get-in (first (:runs (isolated/status c))) [:volumes :created])]
            (is (= [rid prefix] (str/split (str/trim (:out (sh! "docker" "volume" "inspect" v "--format"
                                                                 "{{index .Labels \"hive.compose.run\"}} {{index .Labels \"hive.compose.base\"}}")))
                                           #" "))
                v)))
        (testing "B1: a pre-existing project with the colliding name is refused and left untouched"
          (let [squat (str prefix "-squat")]
            (sh! "docker" "volume" "create" "--label" (str "com.docker.compose.project=" squat)
                 "--label" "com.docker.compose.volume=data" (str squat "_data"))
            (let [x (isolated/up! c (preset c "web") {:run "squat" :ports {:offset (+ offset 50)}})]
              (is (= :project-exists (:reason x)) (pr-str x)))
            (is (some #{(str squat "_data")} (:volumes (objects prefix))) "the squatter's volume is untouched")
            (is (empty? (filter #(str/starts-with? % squat) (:containers (objects prefix)))) "nothing started")))
        (testing "3. a second section joins the same compose project; the native profile's service runs"
          (let [x (isolated/up! c (preset c "extra") {:run rid})]
            (is (r/ok? x) (pr-str x))
            (is (= cp (:compose-project (:ok x))))
            (is (contains? (set (:ok (port/-running e (get-in @(:state c) [:active (str "extra@" rid) :profile]))))
                           "extra"))
            (is (= (sort (map #(str "HIVE_PORT_ENVOY_" %) ["10000" "10001" "9901"]))
                   (sort (filter #(str/starts-with? % "HIVE_PORT_ENVOY") (keys (get-in x [:ok :env-run]))))))))
        (testing "5. dry_run leaves nothing behind"
          (let [before (objects prefix)
                st @(:state c)
                x (isolated/plan c (preset c "web") {:ports {:offset (+ offset 7)}})]
            (is (r/ok? x) (pr-str x))
            (is (= before (objects prefix)))
            (is (= st @(:state c)))
            (is (not (.exists (io/file (get-in x [:ok :path])))))))
        (testing "4. the build-only image changed: could-not-run image-changed"
          (sh! "docker" "build" "-q" "-t" built "--label" "hcit=2" dir)
          (let [x (isolated/up! c (preset c "extra") {:run rid})]
            (is (= :compose/could-not-run (:error x)) (pr-str x))
            (is (= :image-changed (:reason x)))
            (is (= built-id (:was x)))))
        (testing "6. down keeps the run's volumes; volumes=true removes only them"
          (let [created (get-in (first (:runs (isolated/status c))) [:volumes :created])
                d (isolated/down-run! c rid false)]
            (is (r/ok? d) (pr-str d))
            (is (empty? (filter #(str/includes? % cp) (:containers (objects prefix)))))
            (is (every? (set (:volumes (objects prefix))) created))
            (is (= (sort created) (sort (get-in d [:ok :volumes-kept]))))))
        (testing "6b. down run=<id> volumes=true removes only the run's own volumes"
          (let [removed (isolated/down-run! c rid true)]
            (is (r/ok? removed) (pr-str removed))
            (is (seq (get-in removed [:ok :volumes :removed])))
            (is (empty? (get-in removed [:ok :volumes :kept])))
            (is (empty? (filter #(str/starts-with? % (str cp "_")) (:volumes (objects prefix)))))
            (is (some #{(str prefix "_data")} (:volumes (objects prefix))) "the shared project's volume stays")
            (is (some #{(str prefix "-fixed")} (:volumes (objects prefix))) "the explicit-name shared volume stays")
            (is (= "shared" (exec-out (str prefix "-web") "cat" "/data/marker-shared")))))
        (testing "7. config digest: equal across runs, different after a one-line change"
          (let [a (get-in (isolated/plan c (preset c "web") {:ports {:offset offset}}) [:ok :config-digest])
                b (get-in (isolated/plan c (preset c "web") {:ports {:offset (+ 3 offset)}}) [:ok :config-digest])]
            (is (= a b))
            (spit (io/file dir "compose.yml") (compose-yml prefix ports "x-hcit-changed: 1\n"))
            (spit (io/file dir "compose.yml") (str/replace (slurp (io/file dir "compose.yml")) "sleep 600\"]\n  extra" "sleep 601\"]\n  extra"))
            (is (not= a (get-in (isolated/plan c (preset c "web") {:ports {:offset offset}}) [:ok :config-digest]))))))
      (finally
        (cleanup! prefix dir)
        (is (= {:containers [] :volumes [] :networks []} (objects prefix)) "nothing of ours is left")))))
