(ns hive-compose.promote.why-test
  "The failure bundle as values. Properties: the failing span is always a span
   of the traces, an error below a root, no error with a message lies deeper;
   the span path runs root to failing span; the log window filter is
   monotonic in its bounds and slack and never invents a line."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.promote.trace :as trace]
            [hive-compose.promote.why :as why]))

;; SPDX-License-Identifier: MIT

(defn- sp
  ([id parent start status] (sp id parent start status nil))
  ([id parent start status msg]
   {:span/id id :span/parent-id parent :span/name id :span/service "orders"
    :span/start-ms start :span/status status :span/status-message msg
    :span/attrs {"test.id" "orders.place_boom_order"}}))

(def boom
  "As hc-11's orders fixture exports it: otelhttp's 5xx server span errs
   without a message; its children carry the cause."
  {:trace/id "t1"
   :trace/spans [(sp "suite" nil 0 :unset)
                 (sp "POST /orders" "suite" 10 :error)
                 (sp "orders validate" "POST /orders" 12 :error "sku boom rejected")
                 (sp "kafka produce" "POST /orders" 20 :error)]})

(deftest the-child-with-the-message-explains-the-5xx
  (let [[t s] (why/failing-span [boom])]
    (is (= "t1" (:trace/id t)))
    (is (= "orders validate" (:span/name s)))
    (is (= ["suite" "POST /orders" "orders validate"] (mapv :span/name (why/span-path t s)))))
  (testing "a deeper error without a message loses to a shallower one with one"
    (let [t (update boom :trace/spans conj (sp "deep" "kafka produce" 30 :error))]
      (is (= "orders validate" (:span/name (why/trace-failing-span t))))))
  (testing "with no message anywhere, the deepest error wins, as trace/failing-span"
    (let [t (update boom :trace/spans (fn [ss] (mapv #(assoc % :span/status-message nil) ss)))]
      (is (= (trace/failing-span t) (why/trace-failing-span t)))))
  (is (nil? (why/failing-span [{:trace/id "ok" :trace/spans [(sp "a" nil 0 :ok)]}])))
  (is (nil? (why/failing-span []))))

(deftest log-window-keeps-the-window-and-continuations
  (let [text (str/join "\n" ["2026-11-22T10:00:00.000Z early"
                             "2026-11-22T10:00:10.000Z in window"
                             "  at stack.frame(Line:1)"
                             "orders-1  | 2026-11-22T10:00:12.500000000Z prefixed, ns precision"
                             "2026-11-22T10:00:30.000-03:00 later, offset"
                             "2026-11-22T10:01:00.000Z late"])
        t0 (why/line-ms "2026-11-22T10:00:10.000Z")]
    (is (= ["2026-11-22T10:00:10.000Z in window" "  at stack.frame(Line:1)"
            "orders-1  | 2026-11-22T10:00:12.500000000Z prefixed, ns precision"]
           (why/log-window text [t0 (+ t0 3000)] 0)))
    (is (= 6 (count (why/log-window text [nil nil] 0))) "open bounds keep every line")
    (is (= [] (why/log-window "no timestamps at all" [0 1] 0)))
    (is (nil? (why/line-ms "nothing here")))))

(deftest compose-logs-split-by-service
  (is (= {"orders" "orders-1  | a\norders-1  | c" "kafka" "kafka-1  | b" "" "stray"}
         (why/split-compose-logs "orders-1  | a\nkafka-1  | b\norders-1  | c\nstray\n"))))

(def cell {:run/id "r1" :suite/id "orders-suite" :project "orders" :exit 1 :status :failed
           :started-at 1000 :finished-at 5000})

(def kase {:case/id "orders-suite/orders.place_boom_order" :case/name "place_boom_order"
           :case/classname "orders" :case/status :failed
           :case/message "POST /orders returned 500, expected 202"})

(deftest a-bundle-and-its-markdown
  (let [b (why/bundle {:case kase :test-id "orders.place_boom_order" :cell-result cell
                       :traces [boom]
                       :logs {"orders" "1970-01-01T00:00:02.000Z boom rejected\n1970-01-01T00:01:40.000Z much later"}
                       :plan {:run "r1" :stack "shop" :status :up :instances {} :consumers {} :claims {}}})
        md (why/render-markdown b)]
    (is (= "orders validate" (get-in b [:why/failing-span :span/name])))
    (is (= "t1" (get-in b [:why/failing-span :trace/id])))
    (is (= "sku boom rejected" (get-in b [:why/failing-span :span/status-message])))
    (is (= [1000 5000] (:why/window b)) "the cell's own window")
    (is (= {"orders" ["1970-01-01T00:00:02.000Z boom rejected"]} (:why/logs b)))
    (is (= {:run "r1" :stack "shop" :status :up :instances {} :consumers {}} (:why/plan b)))
    (is (str/includes? (:why/summary b) "sku boom rejected"))
    (doseq [s ["# why orders.place\\_boom\\_order" "## Failing span" "sku boom rejected"
               "## Span path" "## Logs" "## Plan" "POST /orders returned 500"]]
      (is (str/includes? md s) s)))
  (testing "no traces: the bundle says so and still carries the case"
    (let [b (why/bundle {:case kase :cell-result cell :traces [] :logs {}})]
      (is (nil? (:why/failing-span b)))
      (is (= [] (:why/span-path b)))
      (is (str/includes? (why/render-markdown b) "No error span found")))))

;; ---------------------------------------------------------------- properties

(def gen-trace
  (gen/let [tid (gen/elements ["a" "b" "c"])
            n (gen/choose 1 15)
            picks (gen/vector (gen/tuple (gen/choose -2 20) (gen/choose 0 50)
                                         (gen/elements [:ok :error :unset])
                                         (gen/elements [nil nil "boom" "bad"])) n)]
    {:trace/id tid
     :trace/spans (vec (map-indexed
                        (fn [i [p start status msg]]
                          (sp (str tid (format "%03d" i))
                              (cond (= -2 p) nil
                                    (= -1 p) "missing"
                                    (< p i) (str tid (format "%03d" p))
                                    :else nil)
                              start status (when (= :error status) msg)))
                        picks))}))

(defspec failing-span-is-within-its-trace 300
  (prop/for-all [traces (gen/vector gen-trace 0 4)]
    (let [res (why/failing-span traces)
          errs (mapcat trace/error-spans traces)]
      (if (nil? res)
        (every? (fn [t] (every? #(nil? (trace/depth (trace/by-id t) %)) (trace/error-spans t))) traces)
        (let [[t s] res
              index (trace/by-id t)
              path (why/span-path t s)]
          (and (some #{t} traces)
               (some #{s} (:trace/spans t))
               (= :error (:span/status s))
               (trace/root? index (get index (:span/id (first path))))
               (= (:span/id s) (:span/id (last path)))
               (or (:span/status-message s)
                   (not-any? :span/status-message
                             (filter #(trace/depth (trace/by-id t) %) (trace/error-spans t))))
               (boolean (seq errs))))))))

(def gen-log
  (gen/let [lines (gen/vector (gen/tuple (gen/one-of [(gen/return nil) (gen/choose 0 100)])
                                         gen/string-alphanumeric) 0 30)]
    (str/join "\n" (for [[t s] lines]
                     (if t
                       (str (.toString (java.time.Instant/ofEpochMilli (* 1000 t))) " " s)
                       (str "  cont " s))))))

(defn- sub? [a b] (every? (set b) a))

(defspec log-window-is-monotonic 300
  (prop/for-all [text gen-log
                 lo (gen/choose 0 100)
                 span (gen/choose 0 50)
                 widen (gen/choose 0 30)
                 slack (gen/choose 0 5000)]
    (let [w [(* 1000 lo) (* 1000 (+ lo span))]
          wider [(* 1000 (- lo widen)) (* 1000 (+ lo span widen))]
          inner (why/log-window text w slack)
          outer (why/log-window text wider slack)
          more-slack (why/log-window text w (+ slack 1000))
          all (remove str/blank? (str/split-lines text))]
      (and (sub? inner outer)
           (sub? inner more-slack)
           (sub? outer all)))))
