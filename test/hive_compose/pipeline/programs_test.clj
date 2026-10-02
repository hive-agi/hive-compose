(ns hive-compose.pipeline.programs-test
  (:require [clojure.test :refer [deftest is testing]]
            [hive-compose.pipeline.ops :as ops]
            [hive-compose.stub :as stub]
            [hive-compose.stub-runner :as runner]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def minute 60000)

(def web {:program/id "web" :program/dir "/w/web" :program/with ["web"]})
(def repl {:program/id "repl" :program/dir "/w/api" :program/with ["api"]})

(def settings
  {:compose/default-ttl-minutes 60
   :compose/default-idle-action :stop
   :compose/log-dir "/logs"
   :compose/programs? true
   :compose/profiles {"api" {:profile/id "api" :profile/dir "/w/shop" :profile/services ["api"]
                             :profile/programs [web repl]}
                      "web" {:profile/id "web" :profile/dir "/w/shop" :profile/services ["web"]
                             :profile/programs [web repl]}
                      "full" {:profile/id "full" :profile/dir "/w/shop" :profile/services ["api" "web"]
                              :profile/programs [web repl]}
                      "blog" {:profile/id "blog" :profile/dir "/w/blog"}}})

(def closures {"api" ["api" "postgres"]
               "web" ["postgres" "web"]
               "full" ["api" "postgres" "web"]
               "blog" ["ghost" "mysql"]})

(def facts
  {"/w/web" {"shadow-cljs.edn" {:nrepl {:port 7902} :builds {:app {}}}}
   "/w/api" {"deps.edn" {:aliases {:nrepl {:main-opts ["-m" "nrepl.cmdline" "--port" "7999"]}}}}})

(defn- ctx
  ([] (ctx settings))
  ([settings]
   (let [clock (atom 0)
         saved (atom nil)]
     {:engine (stub/engine closures)
      :runner (runner/runner facts)
      :settings settings
      :state (atom {:active {} :current nil})
      :now #(deref clock)
      :clock clock
      :saved saved
      :save! #(r/ok (reset! saved %))})))

(defn- held [c id] (get-in @(:state c) [:active id :programs]))

(deftest up-starts-the-paired-program-with-its-nrepl
  (let [c (ctx)
        res (ops/up! c "web")
        [row] (:programs (:ok res))]
    (is (r/ok? res))
    (is (= [[:start "web" ["npx" "shadow-cljs" "watch" "app"] "/logs/shop-web.log"]]
           (runner/starts (:runner c)))
        "only the program paired with a service of the section")
    (is (= {:program/id "web" :kind :shadow :nrepl-port 7902 :started? true}
           (select-keys row [:program/id :kind :nrepl-port :started?])))
    (is (= #{"web"} (set (keys (held c "web")))))
    (is (= 7902 (get-in @(:saved c) [:active "web" :programs "web" :nrepl-port])) "persisted")))

(deftest a-deps-program-starts-through-its-nrepl-alias
  (let [c (ctx)
        [row] (:programs (:ok (ops/up! c "api")))]
    (is (= [[:start "repl" ["clojure" "-M:nrepl"] "/logs/shop-repl.log"]] (runner/starts (:runner c))))
    (is (= 7999 (:nrepl-port row)))))

(deftest a-section-with-no-paired-program-starts-none
  (let [c (ctx)
        res (ops/up! c "blog")]
    (is (r/ok? res))
    (is (empty? (runner/calls (:runner c))))
    (is (not (contains? (:ok res) :programs)))))

(deftest sections-share-a-running-program
  (let [c (ctx)]
    (ops/up! c "web")
    (let [res (ops/up! c "full")]
      (is (= 2 (count (runner/starts (:runner c)))) "web once, repl once")
      (is (= #{true} (set (keep :reused? (:programs (:ok res))))))
      (is (= (get-in (held c "web") ["web" :pid]) (get-in (held c "full") ["web" :pid]))))
    (testing "taking one down keeps what the other holds"
      (let [res (ops/down! c "web" :down)]
        (is (r/ok? res))
        (is (empty? (runner/stops (:runner c))))
        (is (not (contains? (:ok res) :programs)))))
    (testing "the last holder stops them"
      (let [res (ops/down! c "full" :down)]
        (is (= 2 (count (runner/stops (:runner c)))))
        (is (= #{"repl" "web"} (set (map :program/id (:programs (:ok res))))))
        (is (empty? (runner/alive (:runner c))))))))

(deftest switch-keeps-the-programs-the-next-section-wants
  (let [c (ctx)]
    (ops/up! c "full")
    (let [res (ops/switch! c "web")]
      (is (r/ok? res))
      (is (= 2 (count (runner/starts (:runner c)))) "web is carried over, not restarted")
      (is (= 1 (count (runner/stops (:runner c)))) "only repl stops")
      (is (= #{"web"} (set (keys (held c "web")))))
      (is (= 1 (count (runner/alive (:runner c))))))))

(deftest switch-across-projects-stops-the-previous-programs
  (let [c (ctx)]
    (ops/up! c "web")
    (ops/switch! c "blog")
    (is (empty? (runner/alive (:runner c))))))

(deftest a-program-that-fails-to-start-does-not-fail-the-section
  (let [c (ctx)]
    (runner/fail! (:runner c) :start)
    (let [res (ops/up! c "web")
          [row] (:programs (:ok res))]
      (is (r/ok? res))
      (is (= #{"postgres" "web"} (stub/running (:engine c) "shop")))
      (is (r/err? (:error row)))
      (is (empty? (held c "web"))))))

(deftest a-program-with-no-command-is-reported
  (let [c (ctx (assoc-in settings [:compose/profiles "web" :profile/programs]
                         [{:program/id "bare" :program/dir "/w/nothing"}]))
        [row] (:programs (:ok (ops/up! c "web")))]
    (is (= :compose/program-needs-command (:error (:error row))))
    (is (empty? (runner/starts (:runner c))))))

(deftest a-dead-program-is-started-again
  (let [c (ctx)]
    (ops/up! c "web")
    (runner/kill! (:runner c) (get-in (held c "web") ["web" :pid]))
    (let [[row] (:programs (:ok (ops/up! c "web")))]
      (is (:started? row))
      (is (= 2 (count (runner/starts (:runner c))))))))

(deftest the-reaper-stops-the-programs-of-an-idle-section
  (let [c (ctx)]
    (ops/up! c "web")
    (swap! (:clock c) + (* 61 minute))
    (let [pass (ops/reap! c)]
      (is (= ["web"] (mapv :profile/id (:reaped pass))))
      (is (= ["web"] (mapv :program/id (:programs (first (:reaped pass))))))
      (is (empty? (runner/alive (:runner c)))))))

(deftest a-section-taken-down-outside-loses-its-programs
  (let [c (ctx)]
    (ops/up! c "web")
    (swap! (:world (:engine c)) assoc :running {})
    (is (= ["web"] (ops/reconcile! c)))
    (is (empty? (runner/alive (:runner c))))))

(deftest status-tells-whether-the-nrepl-answers
  (let [c (ctx)]
    (ops/up! c "web")
    (let [row (fn [] (->> (:profiles (ops/status c)) (filter #(= "web" (:id %))) first :programs first))]
      (is (= {:program/id "web" :alive? true :nrepl-port 7902 :nrepl-ready? false}
             (select-keys (row) [:program/id :alive? :nrepl-port :nrepl-ready?])))
      (runner/listen! (:runner c) 7902)
      (is (:nrepl-ready? (row))))))

(deftest programs-off-or-no-runner-starts-nothing
  (testing "switched off in the settings"
    (let [c (ctx (assoc settings :compose/programs? false))]
      (is (r/ok? (ops/up! c "web")))
      (is (empty? (runner/calls (:runner c))))))
  (testing "a context without a runner"
    (let [c (dissoc (ctx) :runner)
          res (ops/up! c "web")]
      (is (r/ok? res))
      (is (not (contains? (:ok res) :programs)))
      (is (r/ok? (ops/down! c "web" :down))))))
