(ns hive-compose.promote.report-test
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.boundary.report :as br]
            [hive-compose.promote.report :as report]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- fixture [f] (slurp (io/resource (str "reports/" f))))

(defn- parsed [f]
  (let [res (br/parse (fixture f))]
    (is (r/ok? res) (pr-str res))
    (:ok res)))

(defn- by-status [rep]
  (into {} (map (juxt :case/id :case/status)) (:report/cases rep)))

;; ---------------------------------------------------------------------------
;; JUnit fixtures
;; ---------------------------------------------------------------------------

(deftest surefire
  (let [rep (parsed "surefire.xml")]
    (is (report/valid-report? rep))
    (is (= :junit (:report/format rep)))
    (is (= {:passed 1 :failed 1 :error 1 :skipped 1} (:report/totals rep)))
    (is (= {"com.example.CalcTest/com.example.CalcTest.adds" :passed
            "com.example.CalcTest/com.example.CalcTest.subtracts" :failed
            "com.example.CalcTest/com.example.CalcTest.divides" :error
            "com.example.CalcTest/com.example.CalcTest.multiplies" :skipped}
           (by-status rep)))
    (let [sub (second (:report/cases rep))]
      (is (= 0.012 (:case/time-s sub)))
      (is (str/starts-with? (:case/message sub) "expected: <3> but was: <4>"))
      (is (str/includes? (:case/message sub) "CalcTest.java:18") "the CDATA body is kept")
      (is (= "computing 7 - 4" (:case/output sub))))))

(deftest jest-junit
  (let [rep (parsed "jest-junit.xml")]
    (is (= {"sum/sum adds 1 + 2 to equal 3" :passed
            "sum/sum adds negatives" :failed
            "sum/sum handles bigint" :skipped}
           (by-status rep))
        "a classname repeating the name is not doubled into the id")
    (is (str/includes? (:case/message (second (:report/cases rep))) "Received: -1"))))

(deftest go-junit-report
  (let [rep (parsed "go-junit-report.xml")]
    (is (= {:passed 1 :failed 1 :error 1 :skipped 1} (:report/totals rep)))
    (is (= "example.com/shop/cart/example.com/shop/cart.TestTotal"
           (:case/id (first (report/failed-cases rep)))))
    (is (str/includes? (:case/message (last (:report/cases rep))) "index out of range"))))

(deftest pytest
  (let [rep (parsed "pytest.xml")
        [_ cancel refund ship] (:report/cases rep)]
    (is (= "pytest/tests.test_orders.test_cancel[paid]" (:case/id cancel)))
    (is (= "tests.test_orders" (:case/classname cancel)))
    (is (= "test_cancel[paid]" (:case/name cancel)))
    (is (str/starts-with? (:case/message cancel) "assert 'paid' == 'cancelled'\n"))
    (is (= "refunds not wired yet" (first (str/split-lines (:case/message refund)))))
    (is (= :error (:case/status ship)))
    (is (= "connecting..." (:case/output ship)) "system-err is output too")))

(deftest junit-tolerates-missing-attributes-and-nesting
  (let [xml "<testsuites>
               <testsuite name=\"outer\">
                 <testsuite name=\"inner\"><testcase name=\"a\" time=\"bogus\"/></testsuite>
                 <testsuite><testcase classname=\"K\" name=\"b\" time=\"1,234.5\"/></testsuite>
                 <testcase/>
               </testsuite>
               <testcase name=\"loose\"><failure/><skipped/></testcase>
             </testsuites>"
        rep (:ok (br/parse xml))]
    (is (report/valid-report? rep))
    ;; the nearest named suite, not the path of suites
    (is (= ["inner/a" "outer/K.b" "outer" "loose"] (mapv :case/id (:report/cases rep))))
    (is (= [nil 1234.5 nil nil] (mapv :case/time-s (:report/cases rep))))
    (is (= :failed (:case/status (last (:report/cases rep)))) "a failure outranks a skip")
    (testing "a bare <testsuite> root"
      (is (= ["s/x"] (mapv :case/id (:report/cases (:ok (br/parse "<testsuite name='s'><testcase name='x'/></testsuite>")))))))))

(deftest junit-refuses-what-is-not-junit
  (is (= :junit/unknown-root (:reason (br/parse "<html><body/></html>"))))
  (is (= :xml/unparseable (:reason (br/parse "<testsuite><testcase></testsuite>"))))
  (is (= :report/malformed (:error (br/parse-junit nil)))))

(deftest xml-is-xxe-safe
  (testing "an external entity is never resolved"
    (let [res (br/parse "<?xml version=\"1.0\"?><!DOCTYPE t [<!ENTITY x SYSTEM \"file:///etc/passwd\">]><testsuite name=\"&x;\"/>")]
      (is (r/err? res))
      (is (= :report/malformed (:error res)))
      (is (not (str/includes? (pr-str res) "root:")))))
  (testing "nor is entity expansion (billion laughs)"
    (is (r/err? (br/parse (str "<?xml version=\"1.0\"?><!DOCTYPE l [<!ENTITY a \"aaaaaaaaaa\">"
                               "<!ENTITY b \"&a;&a;&a;&a;&a;&a;&a;&a;&a;&a;\">"
                               "<!ENTITY c \"&b;&b;&b;&b;&b;&b;&b;&b;&b;&b;\">]>"
                               "<testsuite name=\"&c;\"/>")))))
  (testing "nor an external DTD"
    (is (r/err? (br/parse "<!DOCTYPE testsuite SYSTEM \"http://127.0.0.1:1/x.dtd\"><testsuite/>")))))

;; ---------------------------------------------------------------------------
;; TAP fixtures
;; ---------------------------------------------------------------------------

(deftest node-test-tap
  (let [rep (parsed "node-test.tap")
        [adds subtracts kafka refunds] (:report/cases rep)]
    (is (report/valid-report? rep))
    (is (= :tap (:report/format rep)))
    (is (= {"math/adds" :passed "math/subtracts" :failed "needs kafka" :skipped "refunds" :skipped}
           (by-status rep))
        "subtests flatten into parent/child; a suite failing through its subtests is not a case")
    (is (= {:passed 1 :failed 1 :error 0 :skipped 2} (:report/totals rep)))
    (is (= 5.1E-4 (:case/time-s adds)) "duration_ms of the YAML block")
    (is (str/includes? (:case/message subtracts) "1 !== 2") "the YAML block is kept raw")
    (is (str/includes? (:case/message subtracts) "failureType: 'testCodeFailure'"))
    (is (str/includes? (:case/message kafka) "duration_ms: 0.1"))
    (is (= :skipped (:case/status refunds)) "a failing TODO is not a failure")))

(deftest bats-tap
  (let [rep (parsed "bats.tap")
        [_ sub div slow] (:report/cases rep)]
    (is (= ["addition works" "subtraction works" "division by zero" "slow path"]
           (mapv :case/id (:report/cases rep))))
    (is (= :failed (:case/status sub)))
    (is (str/includes? (:case/output sub) "test/math.bats, line 9") "trailing comments are output")
    (is (= "SKIP not implemented" (:case/message div)))
    (is (= :skipped (:case/status slow)) "the directive is case-insensitive")))

(deftest bail-out-tap
  (let [rep (parsed "bail-out.tap")]
    (is (= "database unreachable" (:report/bail-out rep)))
    (is (= {:passed 1 :failed 1 :error 3 :skipped 0} (:report/totals rep)))
    (is (= ["test 3" "test 4" "test 5"] (mapv :case/id (filter #(= :error (:case/status %)) (:report/cases rep)))))
    (is (str/includes? (:case/message (second (:report/cases rep))) "relation \"orders\" does not exist"))))

(deftest tap-edges
  (testing "a test the plan promised and nobody ran is an error"
    (is (= {:passed 1 :failed 0 :error 2 :skipped 0}
           (:report/totals (:ok (report/parse-tap "1..3\nok 2 - b\n"))))))
  (testing "unnumbered points count on from the last"
    (is (= {:passed 3 :failed 0 :error 0 :skipped 0}
           (:report/totals (:ok (report/parse-tap "ok\nok - two\nok\n1..3"))))))
  (testing "a description may carry an escaped #"
    (let [[c] (:report/cases (:ok (report/parse-tap "ok 1 - issue \\#12 # SKIP later\n1..1")))]
      (is (= "issue #12" (:case/id c)))
      (is (= "SKIP later" (:case/message c)))))
  (testing "Bail out! without a plan is an error case of its own"
    (let [rep (:ok (report/parse-tap "ok 1 - a\nBail out!"))]
      (is (= {:passed 1 :failed 0 :error 1 :skipped 0} (:report/totals rep)))
      (is (= "" (:report/bail-out rep)))))
  (testing "a TODO that passes passes"
    (is (= :passed (:case/status (first (:report/cases (:ok (report/parse-tap "ok 1 - x # TODO soon"))))))))
  (testing "a failing parent whose subtests all pass stays a case (a hook failed)"
    (is (= {"p/c" :passed "p" :failed}
           (by-status (:ok (report/parse-tap "    ok 1 - c\n    1..1\nnot ok 1 - p\n1..1"))))))
  (testing "malformed"
    (is (= :tap/no-tests (:reason (report/parse-tap "hello\nworld"))))
    (is (= :tap/not-text (:reason (report/parse-tap nil))))
    (is (= :tap/implausible-plan (:reason (report/parse-tap "1..999999999"))))))

;; ---------------------------------------------------------------------------
;; detect, merge, query
;; ---------------------------------------------------------------------------

(deftest detect-format
  (is (= :junit (report/detect-format (fixture "surefire.xml"))))
  (is (= :junit (report/detect-format (str "\uFEFF  " (fixture "pytest.xml")))))
  (is (= :tap (report/detect-format (fixture "node-test.tap"))))
  (is (= :tap (report/detect-format (fixture "bats.tap"))))
  (is (= :tap (report/detect-format "# preamble\nok 1 - a")))
  (is (nil? (report/detect-format "PASS src/sum.test.js")))
  (is (nil? (report/detect-format nil)))
  (is (= :format/unknown (:reason (br/parse "")))))

(deftest declared-format
  (testing ":suite/report-format :auto | :junit | :tap"
    (is (= :junit (:report/format (:ok (br/parse (fixture "pytest.xml") :junit)))))
    (is (= :tap (:report/format (:ok (br/parse (fixture "bats.tap") :tap)))))
    (is (= (br/parse (fixture "bats.tap")) (br/parse (fixture "bats.tap") :auto)))
    (is (= :tap/no-tests (:reason (br/parse (fixture "pytest.xml") :tap))) "a declared format is not second-guessed")
    (is (= :xml/unparseable (:reason (br/parse (fixture "bats.tap") :junit))))
    (is (= :format/unsupported (:reason (br/parse "ok 1" :xunit))))
    (is (= br/formats #{:auto :junit :tap}))
    (is (= 4 (count (:report/cases (:ok (br/read-report (io/resource "reports/bats.tap") :tap))))))))

(deftest merge-and-query
  (let [reps (map parsed ["surefire.xml" "bats.tap" "bail-out.tap"])
        merged (report/merge-reports reps)]
    (is (report/valid-report? merged))
    (is (= :mixed (:report/format merged)))
    (is (= (apply merge-with + (map :report/totals reps)) (:report/totals merged)))
    (is (= (mapcat :report/cases reps) (:report/cases merged)))
    (is (= "database unreachable" (:report/bail-out merged)))
    (is (= 7 (count (report/failed-cases merged))))
    (is (not (report/passed? merged)))
    (is (= :tap (:report/format (report/merge-reports [(parsed "bats.tap") (parsed "node-test.tap")]))))
    (is (report/passed? (report/merge-reports [])))))

(deftest read-reports-from-a-directory
  (let [res (br/read-reports (io/file (io/resource "reports")))]
    (is (r/ok? res))
    (is (= 28 (count (:report/cases (:ok res)))))
    (is (= {:passed 7 :failed 7 :error 6 :skipped 8} (:report/totals (:ok res)))))
  (is (= :report/none (:error (br/read-reports (io/file (io/resource "reports") "nowhere")))))
  (is (= :report/unreadable (:error (br/read-report "/nonexistent/report.xml")))))

;; ---------------------------------------------------------------------------
;; Properties: a test-only renderer, parsed back
;; ---------------------------------------------------------------------------

(defn- esc [s]
  (-> s (str/replace "&" "&amp;") (str/replace "<" "&lt;") (str/replace ">" "&gt;")
      (str/replace "\"" "&quot;") (str/replace "'" "&apos;")))

(def ^:private gen-word
  (gen/fmap (fn [[c cs]] (str/trim (apply str c cs)))
            (gen/tuple gen/char-alpha (gen/vector (gen/elements (seq "abcXYZ019 _.-[]<>&\"'é")) 0 12))))

(def ^:private gen-time
  (gen/one-of [(gen/return nil)
               (gen/fmap #(/ (double %) 1000.0) (gen/choose 0 1000000))]))

(def gen-junit-case
  (gen/let [suite (gen/one-of [(gen/return nil) gen-word])
            classname (gen/one-of [(gen/return nil) gen-word])
            nm gen-word
            status (gen/elements report/statuses)
            t gen-time
            msg (gen/one-of [(gen/return nil) gen-word])]
    (let [classname (when (not= classname nm) classname)]
      {:suite suite
       :case {:case/id (report/junit-case-id suite classname nm)
              :case/name nm :case/classname classname :case/status status :case/time-s t
              :case/message (when (not= :passed status) msg) :case/output nil}})))

(defn- render-junit [entries]
  (str "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<testsuites>\n"
       (apply str
              (for [{:keys [suite] c :case} entries
                    :let [{:case/keys [name classname status time-s message]} c
                          attrs (str " name=\"" (esc name) "\""
                                     (when classname (str " classname=\"" (esc classname) "\""))
                                     (when time-s (str " time=\"" time-s "\"")))
                          body (case status
                                 :passed ""
                                 :failed (str "<failure" (when message (str " message=\"" (esc message) "\"")) "/>")
                                 :error (str "<error" (when message (str " message=\"" (esc message) "\"")) "/>")
                                 :skipped (str "<skipped" (when message (str " message=\"" (esc message) "\"")) "/>"))
                          tc (str "<testcase" attrs ">" body "</testcase>")]]
                (if suite (str "  <testsuite name=\"" (esc suite) "\">" tc "</testsuite>\n") (str "  " tc "\n"))))
       "</testsuites>\n"))

(defspec junit-round-trips 200
  (prop/for-all [entries (gen/vector gen-junit-case 0 15)]
    (let [cases (mapv :case entries)
          res (br/parse (render-junit entries))
          back (:ok res)]
      (and (r/ok? res)
           (report/valid-report? back)
           (= cases (:report/cases back))
           (= (report/totals cases) (:report/totals back))))))

(def ^:private gen-desc
  (gen/fmap (fn [[c cs]] (str/trim (apply str c cs)))
            (gen/tuple gen/char-alpha (gen/vector (gen/elements (seq "abcXYZ019 _.-[]:/é")) 0 12))))

(def gen-tap-case
  (gen/tuple gen-desc (gen/elements report/statuses)))

(defn- render-tap
  "TAP for [desc status] pairs: an :error is a test that never ran."
  [pairs]
  (str "TAP version 14\n1.." (count pairs) "\n"
       (apply str (map-indexed (fn [i [desc status]]
                                 (case status
                                   :passed (str "ok " (inc i) " - " desc "\n")
                                   :failed (str "not ok " (inc i) " - " desc "\n  ---\n  at: x\n  ...\n")
                                   :skipped (str "ok " (inc i) " - " desc " # SKIP why\n")
                                   :error ""))
                               pairs))))

(defn- expected-tap [pairs]
  (let [indexed (map-indexed vector pairs)
        ran (remove #(= :error (second (second %))) indexed)
        missing (filter #(= :error (second (second %))) indexed)]
    (concat (map (fn [[_ [desc status]]] [desc status]) ran)
            (map (fn [[i _]] [(str "test " (inc i)) :error]) missing))))

(defspec tap-round-trips 200
  (prop/for-all [pairs (gen/vector gen-tap-case 0 15)]
    (let [res (report/parse-tap (render-tap pairs))
          back (:ok res)]
      (and (r/ok? res)
           (report/valid-report? back)
           (= (expected-tap pairs) (map (juxt :case/id :case/status) (:report/cases back)))
           (= (report/totals (map (fn [[_ s]] {:case/status s}) pairs)) (:report/totals back))))))

(defn- all-counted? [rep]
  (= (:report/totals rep)
     (into {} (map (fn [s] [s (count (filter #(= s (:case/status %)) (:report/cases rep)))]))
           report/statuses)))

(defspec totals-always-equal-per-status-counts 200
  (prop/for-all [entries (gen/vector gen-junit-case 0 20)
                 pairs (gen/vector gen-tap-case 0 20)]
    (let [a (report/->report :junit (map :case entries))
          b (:ok (report/parse-tap (render-tap pairs)))]
      (and (all-counted? a) (all-counted? b) (all-counted? (report/merge-reports [a b]))))))

(def ^:private gen-noise
  (gen/one-of
   [gen/string
    gen/string-ascii
    (gen/fmap #(str "<" %) gen/string-ascii)
    (gen/fmap #(str/join "\n" %)
              (gen/vector (gen/one-of [(gen/elements ["ok" "not ok" "1..3" "1..x" "---" "..." "Bail out!"
                                                      "TAP version 13" "# SKIP" "ok 9999999999999999999"
                                                      "<testsuite>" "<testcase name='a'>" "</testsuites>"
                                                      "    ok 1 - deep" "        not ok 2 # todo" "  ---"])
                                       gen/string-ascii])
                          0 30))]))

(defspec parse-never-throws 500
  (prop/for-all [s gen-noise]
    (let [res (try (br/parse s) (catch Throwable t {:thrown t}))]
      (and (not (:thrown res))
           (or (and (r/ok? res) (report/valid-report? (:ok res)) (all-counted? (:ok res)))
               (= :report/malformed (:error res)))))))
