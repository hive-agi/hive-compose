(ns hive-compose.adapter.cli-load-test
  "IContainerLoad over the docker CLI: running containers by compose label,
   one `docker stats --no-stream` sample of them, the busiest container per
   service."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.adapter.cli :as cli]
            [hive-compose.port :as port]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def p {:profile/id "shop" :profile/dir "/w/shop"})

(def ps-out "shop-api-1\tapi\nshop-worker-1\tworker\nshop-worker-2\tworker\n")

(def stats-out "shop-api-1\t0.12%\nshop-worker-1\t87.50%\nshop-worker-2\t143.2%\n")

(deftest cpu-percent-reads-the-stats-column
  (is (= 12.5 (cli/cpu-percent "12.5%")))
  (is (= 143.2 (cli/cpu-percent " 143.2% ")) "above one core is kept")
  (is (= 0.0 (cli/cpu-percent "0%")))
  (is (nil? (cli/cpu-percent "--")) "a container that just stopped has no reading")
  (is (nil? (cli/cpu-percent nil))))

(deftest service-cpu-is-the-busiest-container-of-each-service
  (is (= {"api" 0.12 "worker" 143.2}
         (cli/service-cpu (cli/container-services ps-out) stats-out)))
  (testing "a container stats did not name, or one with no reading, is left out"
    (is (= {"api" 0.12}
           (cli/service-cpu (cli/container-services ps-out)
                            "shop-api-1\t0.12%\nshop-worker-1\t--\nstranger\t99%\n")))))

(deftest the-engine-samples-the-running-containers-of-the-project
  (let [calls (atom [])
        exec (fn [argv _opts]
               (swap! calls conj argv)
               (r/ok {:exit 0 :stderr ""
                      :stdout (if (= "ps" (second argv)) ps-out stats-out)}))
        e (cli/make-engine {:exec exec})]
    (is (satisfies? port/IContainerLoad e))
    (is (= {"api" 0.12 "worker" 143.2} (:ok (port/-cpu e p))))
    (let [[ps stats] @calls]
      (is (some #{"label=com.docker.compose.project=shop"} ps) "found by label, no compose file loaded")
      (is (some #{"status=running"} ps))
      (is (= ["docker" "stats" "--no-stream"] (take 3 stats)))
      (is (= #{"shop-api-1" "shop-worker-1" "shop-worker-2"} (set (take-last 3 stats)))))
    (testing "no running container asks nothing more"
      (let [calls (atom [])
            e (cli/make-engine {:exec (fn [argv _] (swap! calls conj argv)
                                        (r/ok {:exit 0 :stdout "" :stderr ""}))})]
        (is (= {} (:ok (port/-cpu e p))))
        (is (= 1 (count @calls)))))
    (testing "a docker that fails answers an error, never a throw"
      (let [e (cli/make-engine {:exec (fn [_ _] (r/ok {:exit 1 :stdout "" :stderr "no daemon"}))})]
        (is (r/err? (port/-cpu e p)))))))
