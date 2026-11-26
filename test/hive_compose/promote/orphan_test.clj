(ns hive-compose.promote.orphan-test
  "Incident 20261105135748-5afc73f2: systemd-oomd killed rootless
   docker.service; the container processes lived on, reparented to `systemd
   --user`, and a force-recreate started a second postgres on
   dc_postgres_data. The fixtures are that host, reduced."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.promote.orphan :as orphan]))

;; SPDX-License-Identifier: MIT

(def mgr 8221)
(def subuid [100000 65536])

(defn- scope [id] (str "0::/user.slice/user-1000.slice/user@1000.service/user.slice/docker-" id ".scope"))
(def shim-cgroup "0::/user.slice/user-1000.slice/user@1000.service/app.slice/docker.service")
(def pg-id (apply str "a1b2c3" (repeat 58 "0")))
(def redis-id (apply str "d4e5f6" (repeat 58 "0")))
(def haproxy-id (apply str "0d6e23" (repeat 58 "0")))
(def chroma-id (apply str "463b04" (repeat 58 "0")))

(def pg-data "/home/klein/.local/share/docker/volumes/dc_postgres_data/_data")

(def snapshot
  {:manager-pid mgr
   :subuid subuid
   :procs [{:pid mgr :ppid 1 :uid 1000 :name "systemd" :args "/usr/lib/systemd/systemd --user"}
           {:pid 8556 :ppid mgr :uid 1000 :name "rootlesskit" :args "rootlesskit ..." :cgroup shim-cgroup}
           {:pid 8558 :ppid mgr :uid 1000 :name "pipewire" :args "/usr/bin/pipewire"}
           ;; orphan postmaster and one of its backends: dead daemon, parent systemd --user
           {:pid 795001 :ppid mgr :uid 100998 :name "postgres" :args "postgres" :cgroup (scope pg-id)
            :mounts [pg-data "/" "/home/klein/PP/funeraria/dc/scripts/init-db.sh"] :listening #{5432}}
           {:pid 795002 :ppid 795001 :uid 100998 :name "postgres" :args "postgres: checkpointer"
            :cgroup (scope pg-id) :mounts [pg-data]}
           ;; orphan redis, its volume not asked for
           {:pid 795100 :ppid mgr :uid 100998 :name "redis-server" :args "redis-server *:6379"
            :cgroup (scope redis-id)
            :mounts ["/home/klein/.local/share/docker/volumes/dc_redis_data/_data"] :listening #{6379}}
           ;; haproxy-k8s-lb: a live container on the host network, its process under its shim
           {:pid 12229 :ppid mgr :uid 1000 :name "containerd-shim" :args "containerd-shim-runc-v2 -id 0d6e" :cgroup shim-cgroup}
           {:pid 12262 :ppid 12229 :uid 100098 :name "haproxy" :args "haproxy -W -db" :cgroup (scope haproxy-id)
            :listening #{16443 8405}}
           ;; a live container whose root runs as the user's own uid (container root)
           {:pid 9904 :ppid 9729 :uid 1000 :name "chroma" :args "chroma run" :cgroup (scope chroma-id)
            :mounts ["/home/klein/.local/share/docker/volumes/hive-mcp_chroma-data/_data"]}]})

(def pg-resources {:volumes #{"dc_postgres_data"} :ports #{5434}})

(deftest the-orphan-postmaster-is-found-with-what-it-holds
  (let [[o & more] (orphan/orphans snapshot #{haproxy-id chroma-id} pg-resources)]
    (is (nil? more) "redis holds nothing the section asks for")
    (is (= 795001 (:pid o)))
    (is (= "postgres" (:name o)))
    (is (= [795001 795002] (:pids o)) "its children are named with it")
    (is (= pg-id (:container o)))
    (is (= {:volumes #{"dc_postgres_data"}} (:holds o)))
    (is (re-find #"postgres \(pid 795001\) holds volumes dc_postgres_data" (orphan/describe [o])))))

(deftest roots-are-container-processes-parented-by-systemd-user
  (is (= [795001 795100] (mapv :pid (orphan/roots snapshot #{})))
      "user services (pipewire, rootlesskit, the shim) are not container processes")
  (testing "a process of a container the daemon runs is owned"
    (is (= [795100] (mapv :pid (orphan/roots snapshot #{pg-id})))))
  (testing "a container root (user's own uid) reparented to systemd --user is an orphan too"
    (let [s (update snapshot :procs conj {:pid 777 :ppid mgr :uid 1000 :name "qdrant" :cgroup (scope redis-id)
                                          :mounts ["/x/volumes/q/_data"]})]
      (is (some #{777} (map :pid (orphan/roots s #{}))))))
  (testing "not rootless: no manager or no subuid range answers nothing"
    (is (= [] (orphan/roots (dissoc snapshot :manager-pid) #{})))
    (is (= [] (orphan/roots (assoc snapshot :subuid nil) #{})))))

(deftest haproxy-on-the-host-network-is-never-an-orphan
  ;; It listens on host ports; it sits under its live shim, not systemd --user.
  (is (empty? (orphan/orphans snapshot #{} {:volumes #{} :ports #{16443}}))))

(deftest a-port-held-is-a-refusal-too
  (let [found (orphan/orphans snapshot #{} {:volumes #{} :ports #{6379}})]
    (is (= [795100] (mapv :pid found)))
    (is (= {:ports #{6379}} (:holds (first found))))))

(deftest nothing-asked-nothing-held
  (is (empty? (orphan/orphans snapshot #{} {:volumes #{} :ports #{}}))))

(deftest boundary-parsers-are-pure
  (is (= [100000 65536] (orphan/parse-subuid "root:1:2\nklein:100000:65536\n" "klein")))
  (is (nil? (orphan/parse-subuid "klein:x:65536" "klein")))
  (is (nil? (orphan/parse-subuid nil "klein")))
  (is (orphan/in-subuid? [100000 65536] 165535))
  (is (not (orphan/in-subuid? [100000 65536] 165536)))
  (is (= {:name "haproxy" :ppid 12229 :uid 100098}
         (orphan/parse-status "Name:\thaproxy\nState:\tS\nPPid:\t12229\nUid:\t100098\t100098\t100098\t100098\n")))
  (is (nil? (orphan/parse-status "Name:\tx\n")))
  (is (= 8221 (orphan/manager-pid [{:pid 8221 :uid 1000 :name "systemd" :args "/usr/lib/systemd/systemd --user"}
                                   {:pid 1 :uid 0 :name "systemd" :args "/sbin/init"}]
                                  1000)))
  (is (= pg-id (orphan/container-of (scope pg-id))))
  (is (nil? (orphan/container-of shim-cgroup)))
  (is (= "dc_postgres_data" (orphan/volume-of pg-data)))
  (is (= "v" (orphan/volume-of "/x/volumes/v/_data/sub")))
  (is (nil? (orphan/volume-of "/home/klein/PP/funeraria/dc/scripts")))
  (is (= pg-data (orphan/mount-root (str "1201 1180 252:1 " pg-data " /var/lib/postgresql/data rw,relatime - ext4 /dev/dm-1 rw"))))
  (testing "listening sockets of the process's own uid only"
    (let [tcp (str "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
                   "   0: 00000000:1538 00000000:0000 0A 00000000:00000000 00:00000000 00000000  100998        0 1\n"
                   "   1: 0100007F:4792 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 2\n"
                   "   2: 00000000:1538 0200007F:B000 01 00000000:00000000 00:00000000 00000000  100998        0 3\n")]
      (is (= #{5432} (orphan/listening-ports tcp 100998)))
      (is (= #{} (orphan/listening-ports nil 1))))))
