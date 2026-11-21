(ns hive-compose.pipeline.optional-deps-test
  "depends_on entries marked `required: false`: `up` neither builds nor starts
   them; one already running is still needed (waited on, spared on release)."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.pipeline.ops :as ops]
            [hive-compose.stub :as stub]
            [hive-dsl.result :as r]
            [hive-compose.port :as port]))

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

;; ---------------------------------------------------------------------------
;; retained optional deps keep what they require

;; crm --optional--> notes --required--> db
(def chain
  {"db" {}
   "notes" {:deps ["db"]}
   "crm" {:optional ["notes"]}})

(defn- chain-ctx [running]
  {:engine (stub/engine {} {"funeral" running} {"funeral" chain})
   :settings settings
   :state (atom {:active {} :current nil})
   :now (constantly 0)})

(deftest required-closure-keeps-what-a-running-optional-dep-requires
  (let [graph {"crm" {"notes" {:required? false}}
               "notes" {"db" {:required? true} "cache" {:required? false}}
               "cache" {"redis" {}}
               "db" {}}]
    (is (= {:start ["crm"] :needs ["crm" "db" "notes"]}
           (ops/required-closure graph ["crm"] #{"notes" "db"}))
        "db is kept for notes, never started")
    (is (= {:start ["crm"] :needs ["cache" "crm" "db" "notes" "redis"]}
           (ops/required-closure graph ["crm"] #{"notes" "cache"}))
        "a running optional met under a kept one is kept with its own requirements")
    (is (= {:start ["crm"] :needs ["crm"]}
           (ops/required-closure graph ["crm"] #{"db"}))
        "a non-running optional dep and its descendants are left out")))

(deftest up-keeps-the-required-deps-of-a-running-optional-dep-without-starting-them
  (let [c (chain-ctx #{"notes" "db"})
        res (up-target! c "crm")
        e (:engine c)]
    (is (= ["crm"] (:services (:ok res))))
    (is (= ["crm" "db" "notes"] (:needs (:ok res))))
    (is (= [:up "funeral" ["crm"]] (last (stub/calls e))))
    (is (true? (:last-up-no-deps? @(:world e))))))

(deftest switching-from-an-optional-dep-keeps-what-it-requires
  (let [c (chain-ctx #{})
        e (:engine c)]
    (is (r/ok? (up-target! c "notes")))
    (is (= #{"db" "notes"} (stub/running e "funeral")))
    (let [res (r/bind (ops/subject c {:target "crm"}) #(ops/switch-profile! c %))]
      (is (r/ok? res))
      (is (= ["crm"] (:services (:ok res))))
      (is (= "funeral/crm" (:current @(:state c))))
      (is (not (contains? (:active @(:state c)) "funeral/notes")) "the notes section was released")
      (is (= #{"crm" "db" "notes"} (stub/running e "funeral")) "db still runs for the kept notes"))))

(deftest down-of-the-owner-spares-what-a-kept-optional-dep-requires
  (let [c (chain-ctx #{})
        e (:engine c)]
    (is (r/ok? (up-target! c "notes")))
    (is (r/ok? (up-target! c "crm")))
    (is (r/ok? (r/bind (ops/subject c {:target "notes"}) #(ops/down-profile! c % :stop))))
    (is (= #{"crm" "db" "notes"} (stub/running e "funeral")))))

(deftest reaping-the-owner-spares-what-a-kept-optional-dep-requires
  (let [c (chain-ctx #{})
        e (:engine c)]
    (is (r/ok? (up-target! c "notes")))
    (is (r/ok? (up-target! c "crm")))
    (swap! (:state c) assoc-in [:active "funeral/notes" :last-touch] (- (* 61 60000)))
    (let [{:keys [reaped]} (ops/reap! c)]
      (is (= ["funeral/notes"] (mapv :profile/id reaped))))
    (is (= #{"crm" "db" "notes"} (stub/running e "funeral")))))

;; ---------------------------------------------------------------------------
;; no pin on a graph that cannot be trusted

(defrecord PlainEngine [inner]
  port/IComposeEngine
  (-version [_] (port/-version inner))
  (-closure [_ p] (port/-closure inner p))
  (-listed [_ p] (port/-listed inner p))
  (-profiles [_ p] (port/-profiles inner p))
  (-running [_ p] (port/-running inner p))
  (-projects [_] (port/-projects inner))
  (-up! [_ p s] (port/-up! inner p s))
  (-stop! [_ p s] (port/-stop! inner p s))
  (-down! [_ p s] (port/-down! inner p s))
  (-logs [_ p s t] (port/-logs inner p s t)))

(deftest an-engine-without-the-graph-port-takes-the-old-path
  (let [inner (stub/engine {} {"funeral" #{"notes" "postgres"}} {"funeral" funeral})
        c (assoc (ctx #{}) :engine (->PlainEngine inner))
        res (up-target! c "crm")]
    (is (not (satisfies? port/IDependencyGraph (:engine c))))
    (is (= ["builder" "contracts" "crm" "notes" "postgres"] (:services (:ok res))))
    (is (nil? (:needs (:ok res))))
    (is (false? (:last-up-no-deps? @(:world inner))))))

(deftest a-graph-missing-a-closure-service-gives-no-pin
  (let [c (ctx #{})
        e (:engine c)]
    (stub/graph! e "funeral" {"crm" {"postgres" {:required? true}
                                     "contracts" {:required? false}
                                     "notes" {:required? false}}
                              "postgres" {}})
    (let [res (up-target! c "crm")]
      (is (= ["builder" "contracts" "crm" "notes" "postgres"] (:services (:ok res))))
      (is (false? (:last-up-no-deps? @(:world e)))))))

(deftest a-required-graph-node-outside-the-closure-gives-no-pin
  (let [c (ctx #{})
        e (:engine c)]
    (stub/graph! e "funeral" {"crm" {"postgres" {:required? true}
                                     "contracts" {:required? false}
                                     "notes" {:required? false}
                                     "ghost" {:required? true}}
                              "contracts" {"builder" {}}
                              "notes" {"postgres" {}}
                              "builder" {}
                              "postgres" {}
                              "ghost" {}})
    (let [res (up-target! c "crm")]
      (is (= ["builder" "contracts" "crm" "notes" "postgres"] (:services (:ok res))))
      (is (false? (:last-up-no-deps? @(:world e)))))))

;; ---------------------------------------------------------------------------
;; explicitly targeted services behind a native profile (the stub honours
;; native profiles: an untargeted graph leaves the profiled service out)

(deftest a-profiled-target-with-a-required-and-an-optional-dep
  (let [model {"app" {:profiles #{"debug"} :deps ["db"] :optional ["opt"]}
               "db" {}
               "opt" {}}
        e (stub/engine {} {} {"funeral" model})
        c (assoc (ctx #{}) :engine e)]
    (is (not (contains? (:ok (port/-dependencies e {:profile/dir "/w/funeral"})) "app"))
        "the untargeted graph omits the profiled service, as compose does")
    (let [res (up-target! c "app")]
      (is (= ["app" "db"] (:services (:ok res))))
      (is (true? (:last-up-no-deps? @(:world e))))
      (is (= #{"app" "db"} (stub/running e "funeral")) "opt is not started"))))

(deftest a-profiled-target-whose-required-dep-has-an-optional-dep
  (let [model {"app" {:profiles #{"debug"} :deps ["db"]}
               "db" {:optional ["opt"]}
               "opt" {}}
        e (stub/engine {} {} {"funeral" model})
        c (assoc (ctx #{}) :engine e)
        res (up-target! c "app")]
    (is (= ["app" "db"] (:services (:ok res))) "db must start")
    (is (true? (:last-up-no-deps? @(:world e))))
    (is (= #{"app" "db"} (stub/running e "funeral")))))
