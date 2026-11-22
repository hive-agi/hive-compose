(ns hive-compose.promote.trace-test
  "Fixtures under test/resources/tempo/ are answers recorded from Tempo 2.8.2
   (grafana/otel-lgtm 0.11.10) after posting two OTLP traces: checkout
   (trace 00f067aa.., a leading zero byte Tempo's search drops, three error
   spans down to a failed INSERT) and cart (all ok)."
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.promote.trace :as trace]))

;; SPDX-License-Identifier: MIT

(defn fixture [name]
  (json/read-str (slurp (io/resource (str "tempo/" name)))))

(def checkout-id "00f067aa0ba902b7a1b2c3d4e5f60718")
(def cart-id "4bf92f3577b34da6a3ce929d0e0e4736")

;; ---------------------------------------------------------------- TraceQL

(deftest traceql-names-scopes-and-sorts-keys
  (is (= "{ resource.hive.run.id = \"run-42\" && span.test.id = \"checkout \\\"pays\\\"\" }"
         (trace/traceql {"test.id" "checkout \"pays\"" "hive.run.id" "run-42"})))
  (is (= "{ }" (trace/traceql {})))
  (testing "a key naming its scope keeps it; others are unscoped"
    (is (= "{ resource.service.name = \"orders-api\" }" (trace/traceql {"resource.service.name" "orders-api"})))
    (is (= "{ .http.route = \"/orders\" }" (trace/traceql {"http.route" "/orders"})))
    (is (= "{ span.\"a b\" = \"x\" }" (trace/traceql {"span.a b" "x"}))))
  (testing "numbers and booleans are bare literals"
    (is (= "{ .cost = 0.5 && .retry = false && span.test.id = 1 }"
           (trace/traceql {"test.id" 1 "retry" false "cost" 0.5})))))

(deftest search-params-widen-the-window-to-seconds
  (is (= {"q" "{ span.test.id = \"t\" }" "start" "1700000000" "end" "1700000002" "limit" "5"}
         (trace/search-params {:attrs {"test.id" "t"} :start-ms 1700000000999 :end-ms 1700000001001 :limit 5})))
  (is (= {"q" "{ }"} (trace/search-params {}))))

(defn read-string-literal
  "A TraceQL lexer's reading of the string literal opening `s` at index 0:
   {:value decoded :end index after the closing quote}, nil when it never
   closes or holds a raw line break. Escapes as Go's strconv.Unquote,
   which Tempo's lexer uses."
  [^String s]
  (when (= \" (first s))
    (loop [i 1 sb (StringBuilder.)]
      (when (< i (count s))
        (let [c (.charAt s i)]
          (cond
            (= c \") {:value (str sb) :end (inc i)}
            (#{\newline \return} c) nil
            (= c \\) (when (< (inc i) (count s))
                       (let [e (.charAt s (inc i))]
                         (case e
                           \n (recur (+ i 2) (.append sb \newline))
                           \r (recur (+ i 2) (.append sb \return))
                           \t (recur (+ i 2) (.append sb \tab))
                           \\ (recur (+ i 2) (.append sb \\))
                           \" (recur (+ i 2) (.append sb \"))
                           \u (recur (+ i 6) (.append sb (char (Integer/parseInt (subs s (+ i 2) (+ i 6)) 16))))
                           nil)))
            :else (recur (inc i) (.append sb c))))))))

(def gen-nasty-string
  "Strings dense in quotes, backslashes, braces, ampersands and controls."
  (gen/fmap str/join
            (gen/vector (gen/one-of [(gen/elements ["\"" "\\" "\\\"" "\"\\" "}" "{" "&&" "\n" "\r" "\t" "\u0001" "\u001f" " " "=" "é" "ü"])
                                     gen/char
                                     gen/char-alphanumeric]))))

(defspec escaped-value-never-terminates-the-string 300
  (prop/for-all [v gen-nasty-string
                 tail gen/string-alphanumeric]
    (let [lit (trace/string-literal v)
          read (read-string-literal (str lit tail " && x"))]
      (and read
           (= v (:value read)) "it reads back as the value"
           (= (count lit) (:end read)) "the literal ends only at its own closing quote"
           (not (re-find #"[\n\r]" lit))))))

(defn- literals
  "The string literals of TraceQL `q` in order as a lexer reads them, :open
   when one never closes."
  [^String q]
  (loop [i 0 acc []]
    (cond
      (>= i (count q)) acc
      (= \" (.charAt q i)) (if-let [{:keys [value end]} (read-string-literal (subs q i))]
                             (recur (+ i end) (conj acc value))
                             (conj acc :open))
      :else (recur (inc i) acc))))

(defspec traceql-literals-read-back-as-the-values 200
  (prop/for-all [attrs (gen/map (gen/elements ["test.id" "hive.run.id" "x"]) gen-nasty-string)]
    (let [q (trace/traceql attrs)]
      (and (str/starts-with? q "{ ") (str/ends-with? q " }")
           (= (mapv val (sort-by key attrs)) (literals q))))))

;; ---------------------------------------------------------------- ids

(deftest ids-are-hex-whatever-tempo-wrote
  (is (= checkout-id (trace/normalize-id "f067aa0ba902b7a1b2c3d4e5f60718" 32)) "search drops leading zeros")
  (is (= checkout-id (trace/normalize-id "APBnqgupArehssPU5fYHGA==" 32)) "traces answer base64")
  (is (= "00f0000000000001" (trace/normalize-id "APAAAAAAAAE=" 16)))
  (is (= cart-id (trace/normalize-id "4BF92F3577B34DA6A3CE929D0E0E4736" 32)))
  (is (nil? (trace/normalize-id "" 16)))
  (is (nil? (trace/normalize-id nil 16)))
  (is (nil? (trace/normalize-id "not base64!!" 16)))
  (testing "an id longer than its width is refused, never misread"
    (is (nil? (trace/normalize-id (apply str (repeat 33 "a")) 32)))
    (is (nil? (trace/normalize-id checkout-id 16)))
    (is (nil? (trace/normalize-id "APBnqgupArehssPU5fYHGA==" 16)))))

(deftest odd-shapes-read-or-are-left-out-never-throw
  (is (= 0 (:trace/duration-ms (first (trace/search-result {"traces" [{"traceID" "ab" "durationMs" 1e30}]}))))
      "a duration outside the long range")
  (is (nil? (:trace/start-ms (first (trace/search-result {"traces" [{"traceID" "ab" "startTimeUnixNano" 99999999999999999999N}]})))))
  (let [s (first (:trace/spans (trace/trace-result "ab" {"batches" [{"scopeSpans" [{"spans" [{"spanId" "01" "status" {"message" 5 "code" 2}}]}]}]})))]
    (is (= :error (:span/status s)))
    (is (nil? (:span/status-message s)) "a message that is not a string")))

(deftest ties-within-one-ms-go-to-the-earlier-ns
  (is (= "b" (:span/id (trace/failing-span {:trace/spans [{:span/id "a" :span/start-ms 10 :span/start-ns 10000900 :span/status :error}
                                                          {:span/id "b" :span/start-ms 10 :span/start-ns 10000100 :span/status :error}]})))))

(defspec hex-and-base64-agree 200
  (prop/for-all [bs (gen/vector (gen/choose 0 255) 16)]
    (let [arr (byte-array (map unchecked-byte bs))
          hex (apply str (map #(format "%02x" %) bs))
          b64 (.encodeToString (java.util.Base64/getEncoder) arr)]
      (= hex (trace/normalize-id b64 32) (trace/normalize-id hex 32)))))

;; ---------------------------------------------------------------- reading

(deftest search-answers-read-into-summaries
  (is (= [{:trace/id checkout-id
           :trace/root-service "suite-runner"
           :trace/root-name "test checkout pays"
           :trace/start-ms 1795367923560
           :trace/duration-ms 900}]
         (trace/search-result (fixture "search-checkout.json"))))
  (is (= [checkout-id cart-id] (mapv :trace/id (trace/search-result (fixture "search-run.json")))))
  (is (= [] (trace/search-result (fixture "search-empty.json"))))
  (is (= [] (trace/search-result {}))))

(deftest trace-answers-read-into-spans
  (let [t (trace/trace-result checkout-id (fixture "trace-checkout.json"))
        spans (:trace/spans t)
        by-name (into {} (map (juxt :span/name identity)) spans)
        insert (by-name "INSERT orders")]
    (is (= checkout-id (:trace/id t)))
    (is (= ["test checkout pays" "GET /cart" "POST /orders" "SELECT stock" "INSERT orders"] (mapv :span/name spans))
        "sorted by start")
    (is (every? #(re-matches #"[0-9a-f]{16}" (:span/id %)) spans))
    (is (= {:span/id "00f0000000000004"
            :span/parent-id "00f0000000000003"
            :span/name "INSERT orders"
            :span/service "orders-api"
            :span/start-ms 1795367923690
            :span/duration-ms 570.0
            :span/status :error
            :span/status-message "duplicate key value violates unique constraint"}
           (dissoc insert :span/attrs :span/start-ns)))
    (is (= 1795367923690529920 (:span/start-ns insert)) "the exact start kept for ordering")
    (is (= {"service.name" "orders-api" "hive.run.id" "run-42" "test.id" "checkout \"pays\""
            "db.system" "postgresql" "retry" false "cost" 0.5}
           (:span/attrs insert))
        "resource and span attributes merge, every value type read")
    (is (= 1 (get-in by-name ["test checkout pays" :span/attrs "test.attempt"])))
    (is (nil? (get-in by-name ["test checkout pays" :span/parent-id])))
    (is (= :ok (get-in by-name ["GET /cart" :span/status])))
    (is (= :unset (get-in by-name ["SELECT stock" :span/status])))
    (is (nil? (get-in by-name ["GET /cart" :span/status-message]))))
  (testing "the v2 envelope {trace {resourceSpans}} reads the same"
    (is (= (trace/trace-result checkout-id (fixture "trace-checkout.json"))
           (trace/trace-result checkout-id (fixture "trace-checkout-v2.json"))))))

(deftest any-value-reads-every-otlp-type
  (is (= "x" (trace/any-value {"stringValue" "x"})))
  (is (= 9007199254740993 (trace/any-value {"intValue" "9007199254740993"})) "int64 as a string, exact")
  (is (= 3 (trace/any-value {"intValue" 3})))
  (is (= true (trace/any-value {"boolValue" true})))
  (is (= 1.5 (trace/any-value {"doubleValue" 1.5})))
  (is (= ["a" 1] (trace/any-value {"arrayValue" {"values" [{"stringValue" "a"} {"intValue" "1"}]}})))
  (is (= {"k" "v"} (trace/any-value {"kvlistValue" {"values" [{"key" "k" "value" {"stringValue" "v"}}]}})))
  (is (nil? (trace/any-value {}))))

(deftest status-codes
  (is (= [:error :ok :unset :error :ok :unset :unset]
         (mapv trace/status [2 1 0 "STATUS_CODE_ERROR" "STATUS_CODE_OK" "STATUS_CODE_UNSET" nil]))))

;; ---------------------------------------------------------------- why

(deftest the-failed-insert-explains-the-checkout
  (let [t (trace/trace-result checkout-id (fixture "trace-checkout.json"))
        f (trace/failing-span t)]
    (is (= ["test checkout pays" "POST /orders" "INSERT orders"] (mapv :span/name (trace/error-spans t))))
    (is (= "INSERT orders" (:span/name f)) "the deepest error, not the test span")
    (is (= ["INSERT orders" "POST /orders" "test checkout pays"]
           (mapv :span/name (trace/ancestry (trace/by-id t) f))))
    (is (= "orders-api \"INSERT orders\" failed: duplicate key value violates unique constraint"
           (trace/describe-span f))))
  (is (nil? (trace/failing-span (trace/trace-result cart-id (fixture "trace-cart.json")))) "a passing trace"))

(defn- sp [id parent start status]
  {:span/id id :span/parent-id parent :span/name id :span/start-ms start :span/status status})

(deftest ties-go-to-the-earliest-and-loops-are-never-chosen
  (is (= "b" (:span/id (trace/failing-span {:trace/spans [(sp "r" nil 0 :ok)
                                                          (sp "a" "r" 20 :error)
                                                          (sp "b" "r" 10 :error)]}))))
  (is (= "a" (:span/id (trace/failing-span {:trace/spans [(sp "r" nil 0 :ok)
                                                          (sp "a" "r" 10 :error)
                                                          (sp "b" "r" 10 :error)]})))
      "equal start: the least id")
  (testing "a span whose parent is missing is a root"
    (is (= "o" (:span/id (trace/failing-span {:trace/spans [(sp "o" "gone" 5 :error)]})))))
  (testing "a parent loop reaches no root"
    (is (nil? (trace/failing-span {:trace/spans [(sp "x" "y" 0 :error) (sp "y" "x" 0 :error)]})))
    (is (= "r" (:span/id (trace/failing-span {:trace/spans [(sp "x" "y" 0 :error) (sp "y" "x" 0 :error)
                                                            (sp "r" nil 9 :error)]}))))))

(def gen-trace
  "A random span forest: each span's parent is an earlier span, none, or an
   id the trace does not hold; statuses mixed."
  (gen/let [n (gen/choose 1 25)
            picks (gen/vector (gen/tuple (gen/choose -2 30) (gen/choose 0 50)
                                         (gen/elements [:ok :error :unset])) n)]
    {:trace/id "t"
     :trace/spans (vec (map-indexed
                        (fn [i [p start status]]
                          (sp (format "%016x" i)
                              (cond (= -2 p) nil
                                    (= -1 p) "ffffffffffffffff"
                                    (< p i) (format "%016x" p)
                                    :else nil)
                              start status))
                        picks))}))

(defspec failing-span-is-an-error-below-a-root 300
  (prop/for-all [t gen-trace]
    (let [f (trace/failing-span t)
          index (trace/by-id t)
          errors (trace/error-spans t)]
      (if (empty? errors)
        (nil? f)
        (let [chain (trace/ancestry index f)
              d (trace/depth index f)]
          (and (= :error (:span/status f))
               (some #{f} (:trace/spans t))
               (seq chain)
               (trace/root? index (last chain)) "descendant-or-self of a root"
               (every? #(<= (trace/depth index %) d) errors) "no error lies deeper"
               (every? #(<= (:span/start-ms f) (:span/start-ms %))
                       (filter #(= d (trace/depth index %)) errors)) "earliest of the deepest"))))))

(defspec span-order-does-not-change-the-answer 200
  (prop/for-all [t gen-trace
                 seed gen/nat]
    (let [shuffled (update t :trace/spans #(vec (sort-by (fn [s] (hash [seed (:span/id s)])) %)))]
      (= (trace/failing-span t) (trace/failing-span shuffled)))))
