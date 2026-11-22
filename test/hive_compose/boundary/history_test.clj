(ns hive-compose.boundary.history-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [hive-compose.boundary.history :as history]
            [hive-compose.port :as port]))

;; SPDX-License-Identifier: MIT

(defn- temp-dir []
  (str (java.nio.file.Files/createTempDirectory "hive-compose-history"
                                                (make-array java.nio.file.attribute.FileAttribute 0))))

(defn- row [run case status]
  {:run/id run :suite/id "s" :project "p" :case/id case :case/status status :at 1})

(deftest an-absent-file-is-an-empty-history
  (let [h (history/file-history (str (temp-dir) "/none/flake-history.ednl"))]
    (is (= {:ok []} (select-keys (port/-history h {}) [:ok])))))

(deftest appends-accumulate-in-order-and-never-rewrite
  (let [path (str (temp-dir) "/sub/flake-history.ednl")
        h (history/file-history path)
        a [(row "run-1" "s/a" :passed) (row "run-1" "s/b" :failed)]
        b [(assoc (row "run-2" "s/a" :failed) :project "q")]]
    (is (= 2 (:appended (:ok (port/-append! h a)))))
    (let [before (slurp path)]
      (is (= 1 (:appended (:ok (port/-append! h b)))))
      (is (.startsWith ^String (slurp path) before) "earlier lines are untouched"))
    (is (= 0 (:appended (:ok (port/-append! h [])))))
    (is (= (into a b) (:ok (port/-history h {}))))
    (is (= a (:ok (port/-history h {:project "p"}))))
    (is (= 3 (count (line-seq (io/reader path)))) "one row per line")))

(deftest a-torn-line-is-skipped-and-counted
  (let [path (str (temp-dir) "/flake-history.ednl")
        h (history/file-history path)]
    (port/-append! h [(row "run-1" "s/a" :passed)])
    (spit path "{:run/id \"run-2\" :case/id" :append true)
    (testing "a later append after a torn line still reads"
      (spit path "\n" :append true)
      (port/-append! h [(row "run-3" "s/a" :passed)]))
    (let [res (port/-history h {})]
      (is (= ["run-1" "run-3"] (mapv :run/id (:ok res))))
      (is (= 1 (:skipped (meta (:ok res))))))))

(deftest concurrent-appends-lose-no-row
  (let [path (str (temp-dir) "/flake-history.ednl")
        h (history/file-history path)
        futs (doall (for [i (range 8)]
                      (future (port/-append! h (for [j (range 25)]
                                                 (row (str "run-" i) (str "s/c" j) :passed))))))]
    (run! deref futs)
    (is (= 200 (count (:ok (port/-history h {})))))))
