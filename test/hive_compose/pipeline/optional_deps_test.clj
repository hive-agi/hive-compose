(ns hive-compose.pipeline.optional-deps-test
  "depends_on entries marked `required: false`: `up` neither builds nor starts
   them; one already running is still needed (waited on, spared on release)."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.pipeline.ops :as ops]
            [hive-compose.stub :as stub]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

;; crm depends_on:
;;   postgres:  required (default)
;;   contracts: required: false, NOT running  (and contracts needs its own build dep)
;;   notes:     required: false, running
(def funeral
  {"postgres" {}
   "builder" {}
   "contracts" {:deps ["builder"]}
   "notes" {:deps ["postgres"]}
   "crm" {:deps ["postgres"] :optional ["contracts" "notes"]}})

(def settings
  {:compose/default-ttl-minutes 60
   :compose/default-idle-action :stop
   :compose/profiles {}
   :compose/projects {"funeral" {:project/id "funeral" :project/dir "/w/funeral"}}})

(defn- ctx [running]
  {:engine (stub/engine {} {"funeral" running} {"funeral" funeral})
   :settings settings
   :state (atom {:active {} :current nil})
   :now (constantly 0)})

(defn- up-target! [c t] (r/bind (ops/subject c {:target t}) #(ops/up-profile! c %)))

(deftest required-closure-follows-only-required-edges
  (let [graph {"crm" {"postgres" {:required? true}
                      "contracts" {:required? false}
                      "notes" {:required? false}}
               "contracts" {"builder" {}}
               "notes" {"postgres" {:required? true}}}]
    (is (= {:start ["crm" "postgres"] :needs ["crm" "notes" "postgres"]}
           (ops/required-closure graph ["crm"] #{"notes"})))
    (is (= {:start ["builder" "contracts"] :needs ["builder" "contracts"]}
           (ops/required-closure graph ["contracts"] #{}))
        "an edge without :required? is required")))

(deftest up-skips-optional-deps-and-needs-only-the-running-ones
  (let [c (ctx #{"notes" "postgres"})
        res (up-target! c "crm")
        e (:engine c)]
    (is (r/ok? res))
    (testing "only the target and its required deps are started"
      (is (= ["crm" "postgres"] (:services (:ok res))))
      (is (= [:up "funeral" ["crm" "postgres"]] (last (stub/calls e))))
      (is (true? (:last-up-no-deps? @(:world e)))
          "compose is told --no-deps so it does not pull the optional ones in itself"))
    (testing "the optional dep that is not running is neither built nor started"
      (is (not (contains? (stub/running e "funeral") "contracts")))
      (is (not (contains? (stub/running e "funeral") "builder"))))
    (testing "the optional dep already running is needed, so release spares it"
      (is (= ["crm" "notes" "postgres"] (:needs (:ok res))))
      (is (= ["crm" "notes" "postgres"] (get-in @(:state c) [:active "funeral/crm" :needs]))))))

(deftest a-target-without-optional-deps-behaves-as-before
  (let [c (ctx #{})
        res (up-target! c "notes")
        e (:engine c)]
    (is (= ["notes" "postgres"] (:services (:ok res))))
    (is (nil? (:needs (:ok res))))
    (is (false? (:last-up-no-deps? @(:world e))) "no --no-deps when nothing was left out")))

(deftest naming-an-optional-dep-starts-it
  (let [c (ctx #{})
        res (up-target! c "crm,contracts")]
    (is (= ["builder" "contracts" "crm" "postgres"] (:services (:ok res))))))

(deftest switch-also-skips-optional-deps
  (let [c (ctx #{})
        res (r/bind (ops/subject c {:target "crm"}) #(ops/switch-profile! c %))]
    (is (= ["crm" "postgres"] (:services (:ok res))))
    (is (= #{"crm" "postgres"} (stub/running (:engine c) "funeral")))))
