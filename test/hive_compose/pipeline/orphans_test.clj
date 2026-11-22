(ns hive-compose.pipeline.orphans-test
  "up, switch and restart refuse while processes of a dead rootless dockerd
   hold the section's volumes or ports (card 20261105145158-160c1c51)."
  (:require [clojure.java.io :as io]
            [clojure.data.json :as json]
            [clojure.test :refer [deftest is testing]]
            [hive-compose.adapter.cli :as cli]
            [hive-compose.adapter.procfs :as procfs]
            [hive-compose.addon :as addon]
            [hive-compose.pipeline.orphans :as orphans]
            [hive-compose.port :as port]
            [hive-compose.promote.orphan :as orphan]
            [hive-compose.promote.orphan-test :as fx]
            [hive-compose.stub :as stub]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defrecord FixedTable [snap]
  port/IProcessTable
  (-snapshot [_] (if (r/err? snap) snap (r/ok snap))))

(def settings
  {:compose/default-ttl-minutes 60
   :compose/default-idle-action :stop
   :compose/profiles {"db" {:profile/id "db" :profile/dir "/w/dc" :profile/services ["postgres"]}
                      "api" {:profile/id "api" :profile/dir "/w/dc" :profile/services ["api"]}}})

(defn- ctx [snap]
  (let [e (stub/engine {"db" ["postgres"] "api" ["api" "postgres"]} {})]
    (stub/holds! e "dc" {"postgres" {:volumes #{"dc_postgres_data"} :ports #{5434}}
                         "api" {:volumes #{} :ports #{8080}}})
    {:engine e
     :processes (->FixedTable snap)
     :settings settings
     :state (atom {:active {} :current nil})
     :now (constantly 0)
     :save! (constantly (r/ok nil))}))

(defn- p [c id] (get-in c [:settings :compose/profiles id]))

(defn- up-guarded [c id allow?]
  (orphans/guarded c (p c id) allow? #(r/ok {:started id})))

(deftest refuses-with-names-and-pids
  (let [c (ctx fx/snapshot)
        res (up-guarded c "db" false)]
    (is (= :compose/orphans-hold-resources (:error res)))
    (is (= [{:pid 795001 :name "postgres" :pids [795001 795002] :container fx/pg-id
             :holds {:volumes #{"dc_postgres_data"}}}]
           (:orphans res)))
    (is (re-find #"postgres \(pid 795001\)" (:message res)))
    (is (re-find #"allow_orphans=true" (:hint res)))))

(deftest the-closure-counts-not-only-the-named-service
  (testing "api depends on postgres: with no named services the closure is checked"
    (let [c (ctx fx/snapshot)
          p' (dissoc (p c "api") :profile/services)]
      (is (r/err? (orphans/guarded c p' false #(r/ok {}))))))
  (testing "a named service whose resources no orphan holds goes ahead"
    (is (= {:started "api"} (:ok (up-guarded (ctx fx/snapshot) "api" false))))))

(deftest the-override-goes-ahead-and-says-what-it-overrode
  (let [out (:ok (up-guarded (ctx fx/snapshot) "db" true))]
    (is (= "db" (:started out)))
    (is (= [795001] (mapv :pid (:orphans-overridden out))))))

(deftest a-failed-check-never-blocks
  (let [out (:ok (up-guarded (ctx (r/err :compose/proc-unreadable {})) "db" false))]
    (is (= "db" (:started out)))
    (is (= :compose/proc-unreadable (get-in out [:orphan-check :error])))))

(deftest live-container-processes-do-not-refuse
  (let [c (ctx fx/snapshot)]
    (swap! (:world (:engine c)) assoc :live #{fx/pg-id})
    (is (r/ok? (up-guarded c "db" false)))))

(deftest off-without-a-process-table
  (let [c (dissoc (ctx fx/snapshot) :processes)]
    (is (not (orphans/enabled? c)))
    (is (= {:started "db"} (:ok (up-guarded c "db" false))))))

(defn- tool-call [snap params]
  (let [e (stub/engine {"db" ["postgres"]} {})
        _ (stub/holds! e "dc" {"postgres" {:volumes #{"dc_postgres_data"}}})
        a (addon/make-addon {:compose/engine e
                             :compose/process-table (->FixedTable snap)
                             :compose/runner nil
                             :compose/save! (constantly (r/ok nil))
                             ;; never the host's real state file: a live section there trips the ownership guard
                             :compose/state-file (str (System/getProperty "java.io.tmpdir") "/hc-orph-" (System/nanoTime) ".edn")
                             :compose/config-source (constantly {})
                             :compose/profiles-file "/nonexistent/profiles.edn"
                             :compose/profiles {"db" {:profile/id "db" :profile/dir "/w/dc"
                                                      :profile/services ["postgres"]}}
                             :compose/swarm? false :compose/notify? false :compose/adopt? false
                             :compose/initial-delay-ms 3600000})]
    (try
      (.initialize! ^hive_addon.protocol.IAddon a {})
      (let [res ((:handler (addon/tool a)) params)]
        {:res (json/read-str (get-in res [:content 0 :text]) :key-fn keyword)
         :error? (:isError res)
         :calls (stub/calls e)})
      (finally (.shutdown! ^hive_addon.protocol.IAddon a)))))

(deftest the-tool-refuses-up-and-restart-and-honours-allow-orphans
  (doseq [params [{"command" "up" "profile" "db"}
                  {"command" "switch" "profile" "db"}
                  {"command" "restart" "profile" "db" "recreate" true}]]
    (let [{:keys [res error? calls]} (tool-call fx/snapshot params)]
      (is error? (str params))
      (is (= "orphans-hold-resources" (:error res)) (str params "; JSON writes a keyword by its name"))
      (is (= [795001] (mapv :pid (:orphans res))))
      (is (not-any? #(#{:up :recreate :restart} (first %)) calls) "nothing was started")))
  (let [{:keys [res error? calls]} (tool-call fx/snapshot {"command" "restart" "profile" "db"
                                                            "recreate" "true" "allow_orphans" "true"})]
    (is (not error?))
    (is (= [795001] (mapv :pid (:orphans-overridden res))))
    (is (some #(= :recreate (first %)) calls))))

(deftest cli-resources-read-the-real-volume-names-and-published-ports
  (let [doc (json/write-str
             {"services" {"postgres" {"volumes" [{"type" "volume" "source" "postgres_data" "target" "/d"}
                                                 {"type" "bind" "source" "/w/init.sh" "target" "/i"}]
                                      "ports" [{"target" 5432 "published" "5434"}]}
                          "redis" {"volumes" [{"type" "volume" "source" "redis_data"}]
                                   "ports" [{"target" 6379 "published" 6380} {"target" 1}]}}
              "volumes" {"postgres_data" {"name" "dc_postgres_data"}}})]
    (is (= {:volumes #{"dc_postgres_data"} :ports #{5434}} (cli/service-resources doc ["postgres"])))
    (is (= {:volumes #{"redis_data"} :ports #{6380}} (cli/service-resources doc ["redis"])))))

(deftest procfs-snapshot-of-a-fixture-tree
  (let [root (doto (io/file (System/getProperty "java.io.tmpdir") (str "procfs-" (System/nanoTime))) .mkdirs)
        put (fn [pid f s] (let [x (io/file root (str pid) f)] (io/make-parents x) (spit x s)))
        status (fn [nm ppid uid] (str "Name:\t" nm "\nPPid:\t" ppid "\nUid:\t" uid "\t" uid "\t" uid "\t" uid "\n"))]
    (try
      (put 8221 "status" (status "systemd" 1 1000))
      (put 8221 "cmdline" "/usr/lib/systemd/systemd\u0000--user\u0000")
      (put 795001 "status" (status "postgres" 8221 100998))
      (put 795001 "cgroup" (str "0::/user.slice/docker-" fx/pg-id ".scope"))
      (put 795001 "mountinfo" (str "1 2 252:1 " fx/pg-data " /var/lib/postgresql/data rw - ext4 /dev/dm-1 rw\n"))
      (put 795001 "net/tcp" (str "  sl  local_address rem_address   st x x x x x uid\n"
                                 "   0: 00000000:1538 00000000:0000 0A 0:0 0:0 0 100998 0 1\n"))
      (put 4242 "status" (status "pipewire" 8221 1000))
      (spit (io/file root "subuid") "klein:100000:65536\n")
      (let [snap (procfs/snapshot {:root (str root) :subuid-file (str (io/file root "subuid"))
                                   :user "klein" :uid 1000})
            pg (first (filter #(= 795001 (:pid %)) (:procs snap)))]
        (is (= 8221 (:manager-pid snap)))
        (is (= [100000 65536] (:subuid snap)))
        (is (= [fx/pg-data] (:mounts pg)))
        (is (= #{5432} (:listening pg)))
        (is (nil? (:mounts (first (filter #(= 4242 (:pid %)) (:procs snap))))) "only candidates are read deep")
        (is (= [795001] (mapv :pid (orphan/orphans snap #{} {:volumes #{"dc_postgres_data"} :ports #{}})))))
      (finally
        (doseq [f (reverse (file-seq root))] (io/delete-file f true))))))
