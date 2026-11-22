(ns hive-compose.boundary.runs
  "The files a test run leaves under the state directory:

     <state-dir>/runs/<run>/<suite>/<project>/result.edn   the hc-8 cell result
     <state-dir>/runs/<run>/why/<case>.md and .edn          hc-10 failure bundles

   Reads answer Results; an unreadable result file is skipped and reported,
   never thrown."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def result-file
  "The name of a cell's result file inside its run dir (agreed with hc-8)."
  "result.edn")

(defn runs-dir [state-dir]
  (str (str/replace (str state-dir) #"/+\z" "") "/runs"))

(defn- dirs [^java.io.File f]
  (sort-by #(.getName ^java.io.File %) (filter #(.isDirectory ^java.io.File %) (or (seq (.listFiles f)) []))))

(defn result-files
  "[{:run :suite :project :path}] of every result file under `state-dir`,
   narrowed to run `run` when given. Sorted by run, suite, project."
  [state-dir run]
  (let [root (io/file (runs-dir state-dir))]
    (vec (for [rd (dirs root)
               :when (or (nil? run) (= run (.getName ^java.io.File rd)))
               sd (dirs rd)
               :when (not= "why" (.getName ^java.io.File sd))
               pd (dirs sd)
               :let [f (io/file pd result-file)]
               :when (.isFile f)]
           {:run (.getName ^java.io.File rd)
            :suite (.getName ^java.io.File sd)
            :project (.getName ^java.io.File pd)
            :path (str f)}))))

(defn read-result
  "Result<cell-result> of the EDN file at `path`."
  [path]
  (r/try-effect* :compose/result-unreadable
    (edn/read-string (slurp (io/file path)))))

(defn read-results
  "{:results [cell-result (with :result/path)] :skipped [{:path :error}]} of
   every result file under `state-dir` (of `run` when given)."
  [state-dir run]
  (reduce (fn [acc {:keys [path]}]
            (let [res (read-result path)]
              (if (and (r/ok? res) (map? (:ok res)))
                (update acc :results conj (assoc (:ok res) :result/path path))
                (update acc :skipped conj {:path path :error (or (:error res) :compose/result-not-a-map)}))))
          {:results [] :skipped []}
          (result-files state-dir run)))

(defn file-slug
  "`s` as a file name: every char outside [A-Za-z0-9._-] made `_`, at most
   120 chars, never empty, never `.` or `..`."
  [s]
  (let [x (str/replace (str s) #"[^A-Za-z0-9._-]" "_")
        x (subs x 0 (min 120 (count x)))]
    (if (or (str/blank? x) (re-matches #"\.+" x)) "_" x)))

(defn bundle-paths
  "{:md :edn} paths of the bundle of `case-id` in `run`."
  [state-dir run case-id]
  (let [base (str (runs-dir state-dir) "/" run "/why/" (file-slug case-id))]
    {:md (str base ".md") :edn (str base ".edn")}))

(defn write-bundle!
  "Result<{:md :edn}>: `markdown` and `bundle` (as EDN) written beside each other."
  [state-dir run case-id bundle markdown]
  (r/try-effect* :compose/bundle-unwritable
    (let [{:keys [md edn] :as paths} (bundle-paths state-dir run case-id)]
      (io/make-parents (io/file md))
      (spit (io/file md) markdown)
      (spit (io/file edn) (binding [*print-namespace-maps* false
                                    *print-length* nil
                                    *print-level* nil]
                            (pr-str bundle)))
      paths)))
