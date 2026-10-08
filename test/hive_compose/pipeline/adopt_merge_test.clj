(ns hive-compose.pipeline.adopt-merge-test
  "Re-adopting strays into an existing `<project>/adopted` section keeps its
   `:profile` snapshot in step with `:services`, so logs and ps address every
   adopted service, not the subset adopted first."
  (:require [clojure.test :refer [is]]
            [clojure.test.check.generators :as gen]
            [hive-compose.pipeline.ops :as ops]
            [hive-test.trifecta :refer [deftrifecta]]))

;; SPDX-License-Identifier: MIT

(def ^:private old-entry
  {:profile/id "shop/adopted" :project "shop" :services ["ledger"]
   :profile {:profile/id "shop/adopted" :profile/services ["ledger"]}
   :started-at 0 :last-touch 0})

(def ^:private new-entry
  {:profile/id "shop/adopted" :project "shop" :services ["minio"]
   :profile {:profile/id "shop/adopted" :profile/services ["minio"]}
   :started-at 5 :last-touch 5})

(def ^:private service-gen
  (gen/vector (gen/elements ["ledger" "minio" "redis" "postgres"]) 1 3))

(deftrifecta merge-adopted
  ops/merge-adopted
  {:apply? true
   :golden-path "test/golden/adopt-merge.edn"
   :cases {:fresh [nil new-entry]
           :merge [old-entry new-entry]
           :overlap [old-entry (assoc new-entry :services ["ledger" "minio"])]
           :needs [(assoc old-entry :needs ["ledger" "postgres"])
                   (assoc new-entry :needs ["minio" "redis"])]}
   :gen (gen/fmap (fn [[a b]]
                    [(assoc old-entry :services a :profile {:profile/services a})
                     (assoc new-entry :services b)])
                  (gen/tuple service-gen service-gen))
   :pred (fn [r] (= (:services r) (get-in r [:profile :profile/services])))
   :num-tests 50
   :mutations [["snapshot-stale" (fn [old e]
                                   (update old :services (comp vec distinct into) (:services e)))]
               ["drops-new" (fn [old _] old)]]
   :assert (fn []
             (let [r (ops/merge-adopted old-entry new-entry)]
               (is (= ["ledger" "minio"] (:services r)))
               (is (= ["ledger" "minio"] (get-in r [:profile :profile/services]))
                   "the snapshot logs/ps read follows the merged services")
               (is (= 0 (:started-at r)) "the existing entry keeps its clock"))
             (is (= new-entry (ops/merge-adopted nil new-entry))))})
