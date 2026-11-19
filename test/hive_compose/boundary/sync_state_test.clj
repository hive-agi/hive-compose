(ns hive-compose.boundary.sync-state-test
  "Two instances on one state file, as during a hot remount: each one's writes
   survive the other's, on the real file."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.boundary.files :as files]
            [hive-dsl.result :as r])
  (:import (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)))

;; SPDX-License-Identifier: MIT

(defn- temp-path []
  (str (Files/createTempDirectory "hive-compose-sync" (make-array FileAttribute 0)) "/state.edn"))

(defn- instance
  "What build-ctx keeps per instance: the live atom and the synced base, both
   from the file as it is now."
  [path]
  (let [st (merge {:active {} :current nil} (:ok (files/load-state path)))]
    {:state (atom st) :base (atom st)}))

(defn- save! [{:keys [state base]} path]
  (files/sync-state! path base state))

(defn- section [touch claims]
  {:profile {:profile/id "dc/escala"} :started-at 0 :last-touch touch
   :claims (into {} (map (fn [[k seen]] [k {:since 0 :seen seen}])) claims)})

(deftest remount-keeps-the-old-instance-claim
  (let [path (temp-path)
        old (instance path)]
    (swap! (:state old) assoc-in [:active "dc/api"] (section 1 {}))
    (is (r/ok? (save! old path)))
    (let [new (instance path)]
      (testing "the old instance finishes a claim after the new one loaded the file"
        (swap! (:state old) assoc-in [:active "dc/escala"] (section 2 {"ling-1" 2}))
        (is (r/ok? (save! old path))))
      (testing "the new instance writes its own change without dropping the claim"
        (swap! (:state new) assoc-in [:active "dc/api" :last-touch] 3)
        (is (r/ok? (save! new path)))
        (is (= {"ling-1" {:since 0 :seen 2}}
               (get-in @(:state new) [:active "dc/escala" :claims]))
            "the claim reached the new instance's memory")
        (is (= #{"dc/api" "dc/escala"} (set (keys (:active (:ok (files/load-state path))))))))
      (testing "a later write of the old instance keeps the new one's change"
        (is (r/ok? (save! old path)))
        (is (= 3 (get-in (:ok (files/load-state path)) [:active "dc/api" :last-touch]))))
      (testing "a removal by one instance holds"
        (swap! (:state new) update :active dissoc "dc/api")
        (is (r/ok? (save! new path)))
        (is (r/ok? (save! old path)))
        (is (= #{"dc/escala"} (set (keys (:active (:ok (files/load-state path)))))))
        (is (nil? (get-in @(:state old) [:active "dc/api"])))))))

(deftest unreadable-file-is-overwritten
  (let [path (temp-path)
        inst (instance path)]
    (spit path "{:active")
    (swap! (:state inst) assoc-in [:active "dc/api"] (section 1 {}))
    (is (r/ok? (save! inst path)))
    (is (= #{"dc/api"} (set (keys (:active (:ok (files/load-state path)))))))))
