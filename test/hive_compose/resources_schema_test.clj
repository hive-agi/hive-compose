(ns hive-compose.resources-schema-test
  "hc-16a: the resource-cap schema is additive and normalizes from JSON."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.promote.config :as config]
            [hive-compose.schema :as schema]
            [malli.core :as m]))

;; SPDX-License-Identifier: MIT

(deftest mem-size
  (doseq [ok [1 536870912 "512m" "2g" "1536M" "64kb" "100" "7b"]]
    (is (m/validate schema/MemSize ok) (pr-str ok)))
  (doseq [bad [0 -1 "" "2 g" "1.5g" "2gib" "g" "2g\n" 1.5]]
    (is (not (m/validate schema/MemSize bad)) (pr-str bad))))

(deftest resources
  (is (m/validate schema/Resources :unbounded))
  (is (m/validate schema/Resources {}))
  (is (m/validate schema/Resources {:mem "1g" :cpus 1.5 :jvm? true}))
  (is (not (m/validate schema/Resources {:mem "1g" :swap "1g"})) "closed")
  (is (not (m/validate schema/Resources {:cpus 0})))
  (is (not (m/validate schema/Resources :none))))

(deftest every-level-accepts-resources
  (is (m/validate schema/Provide {:service "kafka" :resources {:mem "1g"}}))
  (is (m/validate schema/Need {:resources :unbounded}))
  (is (m/validate schema/Stack {:stack/id "s" :stack/consumers ["a"]
                                :stack/resources {"kafka" {:mem "2g"}}
                                :stack/budget "6g" :stack/priority -1}))
  (is (m/validate schema/Suite {:suite/id "e2e" :suite/command "make e2e" :suite/resources {:mem "1g" :cpus 1}}))
  (is (m/validate schema/Suite {:suite/id "e2e" :suite/command "make e2e" :suite/resources :unbounded}))
  (is (not (m/validate schema/Suite {:suite/id "e2e" :suite/command "x" :suite/resources {:mem "lots"}})))
  (is (= :unbounded (get-in (:ok (config/settings {:compose/suites [{:suite/id "e2e" :suite/command "x"
                                                                       :suite/resources "unbounded"}]}
                                                  nil "/home/u"))
                            [:compose/suites "e2e" :suite/resources]))))

(deftest config-normalizes-and-stays-additive
  (testing "a config without the new keys yields no new keys"
    (let [s (:ok (config/settings {} nil "/home/u"))]
      (is (some? s))
      (is (not-any? #(contains? s %) [:compose/default-resources :compose/budget :compose/host-reserve]))))
  (testing "JSON-shaped resources normalize"
    (let [cfg {:compose/default-resources {:mem "512m"}
               :compose/budget "8g"
               :compose/host-reserve 2147483648
               :compose/projects
               {:a {:project/dir "/a"
                    :project/provides {:kafka {:service "kafka" :resources "unbounded"}}
                    :project/needs {:pg {:resources {:mem "256m"}}}}}
               :compose/stacks [{:stack/id "s" :stack/consumers ["a"]
                                 :stack/resources {:kafka "unbounded"}}]}
          r (config/settings cfg nil "/home/u")
          s (:ok r)]
      (is (some? s) (pr-str r))
      (is (= :unbounded (get-in s [:compose/projects "a" :project/provides "kafka" :resources])))
      (is (= {:mem "256m"} (get-in s [:compose/projects "a" :project/needs "pg" :resources])))
      (is (= {"kafka" :unbounded} (get-in s [:compose/stacks "s" :stack/resources])))
      (is (= "8g" (:compose/budget s)))))
  (testing "a bad cap is refused as config"
    (is (= :compose/invalid-config
           (:error (config/settings {:compose/budget "lots"} nil "/home/u"))))))
