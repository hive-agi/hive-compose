(ns hive-compose.pipeline.suites-race-test
  "PR #60 review B1: a fresh-run test! held no claim between stack-up! and
   its first lease, every matrix on a run shared the claim id test:<run>, and
   the end of a matrix (unclaim, check, down) was three separate steps. A
   second `test run=<same>` arriving in that gap claimed the run, attached,
   and was taken down under its running cell while still answering :passed.
   The repro: the injected :now launches B from inside A's first step after
   stack-up!. No with-redefs."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.pipeline.stacks :as stacks]
            [hive-compose.pipeline.suites :as suites]
            [hive-compose.stub :as stub]
            [hive-compose.stub-runner :as sr]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def projects
  {"pg" {:project/id "pg" :project/dir "/w/pg"
         :project/provides {"postgres" {:service "db" :version "16" :env {"PGHOST" "db"}}}}
   "shop" {:project/id "shop" :project/dir "/w/shop" :project/needs {"postgres" {}}}})

(def world {"pg" {"db" {}} "shop" {"api" {}}})

(def suite-defs
  {"load" {:suite/id "load" :suite/command ["k6" "run" "x.js"] :suite/projects ["shop"]}})

(defn- tmp-dir []
  (str (java.nio.file.Files/createTempDirectory "hc8race" (make-array java.nio.file.attribute.FileAttribute 0))))

(defn- base-ctx [state]
  {:engine (stub/engine {} {} world)
   :suite-runner (sr/suite-runner {})
   :settings {:compose/projects projects :compose/profiles {} :compose/suites suite-defs
              :compose/default-ttl-minutes 60}
   :state state
   :state-dir (tmp-dir)
   :id-source (fn [n] (apply str (repeat (* 2 n) "a")))})

(def stack-settings
  {:compose/projects projects :compose/profiles {}
   :compose/stacks {"s" {:stack/id "s" :stack/consumers ["shop"]}}})

(deftest a-fresh-run-is-never-taken-down-under-a-second-matrix
  (let [state (atom {:active {} :current nil})
        a-thread (Thread/currentThread)
        b-done (promise)
        b-result (atom nil)
        b-started (atom false)
        b-down-seen (atom nil)
        c (base-ctx state)
        b-ctx (assoc c :record-run! (fn [_ _]
                                      (reset! b-down-seen (not (contains? (:stacks @state) "ci001")))
                                      (r/ok {})))
        ;; the first time A reads the clock with its run recorded :up (it sits
        ;; between stack-up! and its cells), B asks for the same run
        now-fn (fn []
                 (when (and (identical? (Thread/currentThread) a-thread)
                            (= :up (get-in @state [:stacks "ci001" :status]))
                            (compare-and-set! b-started false true))
                   (future (try (reset! b-result (suites/test! (assoc b-ctx :now (constantly 5000))
                                                               {:suites "load" :run "ci001"}))
                                (finally (deliver b-done true))))
                   (deref b-done 10000 nil))
                 5000)
        a-res (suites/test! (assoc c :now now-fn) {:suites "load" :run "ci001"})]
    (deref b-done 10000 nil)
    (is (true? @b-started) "B arrived while A held a fresh run")
    (is (r/ok? a-res) (pr-str a-res))
    (testing "A's run is claimed from the instant stack-up! records it: B is refused before it attaches"
      (is (= :compose/run-busy (:error @b-result)) (pr-str @b-result))
      (is (nil? @b-down-seen) "B never ran a cell on A's run"))
    (is (= 1 (count (sr/suite-runs (:suite-runner c)))) "only A ran its cell")
    (is (= :passed (get-in a-res [:ok :status])))
    (is (empty? (:stacks @state)) "A took its passing run down, nobody else held it")))

(deftest stack-up-records-the-run-already-claimed
  (let [state (atom {:active {} :current nil})
        seen (atom ::none)
        c (assoc (base-ctx state) :now (constantly 5000) :settings stack-settings)
        ;; the first state in which the run is recorded already carries the claim
        _ (add-watch state ::w (fn [_ _ _ st] (when (and (= ::none @seen) (get-in st [:stacks "r001"]))
                                                (reset! seen (get-in st [:stacks "r001" :claims])))))
        up (stacks/stack-up! c {:stack "s" :run "r001" :claim {"test:r001:x" {:note "n" :seen 9000}}})]
    (remove-watch state ::w)
    (is (r/ok? up) (pr-str up))
    (is (= {"test:r001:x" {:since 5000 :seen 9000 :note "n"}} @seen) "claimed in the swap that recorded it")))

(deftest two-matrices-on-one-run-hold-distinct-claims
  (let [state (atom {:active {} :current nil})
        n (atom 0)
        c (assoc (base-ctx state) :now (constantly 5000) :claim-nonce #(str "n" (swap! n inc)))
        _ (suites/test! c {:suites "load" :run "ci002" :keep-up? true})
        claims-during (atom nil)
        ;; while the second matrix runs its cell on the kept run, a foreign agent claims it too
        c2 (assoc c :record-run! (fn [cx _]
                                   (reset! claims-during (set (keys (get-in @state [:stacks "ci002" :claims]))))
                                   (stacks/claim! cx "ci002" "ling-9" {})
                                   (r/ok {})))
        res (suites/test! c2 {:suites "load" :run "ci002"})]
    (is (r/ok? res) (pr-str res))
    (is (= #{"test:ci002:n2"} @claims-during) "each matrix holds its own test:<run>:<nonce>")
    (is (= ["ling-9"] (get-in res [:ok :claimed-by])))
    (is (= #{"ling-9"} (set (keys (get-in @state [:stacks "ci002" :claims])))) "only its own claim dropped")))

(deftest end-claim-is-one-step-under-the-lock
  (let [state (atom {:active {} :current nil})
        c (assoc (base-ctx state) :now (constantly 5000) :settings stack-settings)
        _ (stacks/stack-up! c {:stack "s" :run "r002" :claim {"a" {} "b" {}}})]
    (is (= {:others ["b"]} (stacks/end-claim! c "r002" "a" {:down? true})) "another claim left: never down")
    (is (contains? (:stacks @state) "r002"))
    (let [res (stacks/end-claim! c "r002" "b" {:down? true})]
      (is (= [] (:others res)))
      (is (r/ok? (:down res)))
      (is (not (contains? (:stacks @state) "r002"))))
    (is (= {:others []} (stacks/end-claim! c "r002" "b" {:down? true})) "a run already gone is left alone")))
