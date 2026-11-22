(ns hive-compose.boundary.runs
  "The files a test run leaves under the state directory:

     <state-dir>/runs/<run>/<suite>/<project>/result.edn   the hc-8 cell result
     <state-dir>/runs/<run>/why/<case>.md and .edn          hc-10 failure bundles

   result.edn is the one codec shared by hc-8 and hc-10: the suite runner
   writes a CellResult with `write-result!`, `why` reads it with
   `read-result`/`read-results`. Both validate against `CellResult`; the run
   directory's name is the authority on the run id, and a file naming another
   run is skipped. Reads answer Results; an unreadable or invalid result file
   is skipped and reported, never thrown."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [hive-dsl.result :as r]
            [malli.core :as m]
            [malli.error :as me])
  (:import (java.nio.file CopyOption Files StandardCopyOption)))

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

;; ---------------------------------------------------------------- codec
;; One codec for both sides: hc-8 (suite runner) writes with `write-result!`,
;; hc-10 (why) reads with `read-result`.

(def RunDir
  "A run directory's name: a RunId (hive-compose.schema/RunId)."
  [:re #"^[a-z0-9]{3,40}\z"])

(def PathPart
  "A suite or project directory name: one path segment, never . or .."
  [:and [:re #"^[A-Za-z0-9][A-Za-z0-9_.-]*\z"] [:fn #(not (re-matches #"\.+" %))]])

(def CellResult
  "What result.edn holds: hc-8 `hive-compose.promote.suite/cell-result`.
   Open: extra keys are kept. `:report` is a Report
   (hive-compose.promote.report/Report), checked here only for its cases."
  [:map
   [:run/id RunDir]
   [:suite/id PathPart]
   [:project PathPart]
   [:status [:enum :passed :failed :error]]
   [:exit {:optional true} [:maybe :int]]
   [:report {:optional true}
    [:map [:report/cases [:sequential [:map [:case/id :string] [:case/status :keyword]]]]]]
   [:report-error {:optional true} :map]
   [:started-at {:optional true} :int]
   [:finished-at {:optional true} :int]
   [:duration-ms {:optional true} :int]])

(def ^:private valid-result? (m/validator CellResult))

(defn result-path
  "`<state-dir>/runs/<run>/<suite>/<project>/result.edn`, the same dir as
   hc-8 `hive-compose.promote.suite/run-dir`."
  [state-dir run suite project]
  (str (runs-dir state-dir) "/" run "/" suite "/" project "/" result-file))

(defn- write-edn! [path v]
  (let [f (io/file path)
        tmp (io/file (str path ".tmp"))]
    (io/make-parents f)
    (spit tmp (binding [*print-namespace-maps* false *print-length* nil *print-level* nil]
                (pr-str v)))
    (Files/move (.toPath tmp) (.toPath f)
                (into-array CopyOption [StandardCopyOption/REPLACE_EXISTING StandardCopyOption/ATOMIC_MOVE]))
    path))

(defn write-result!
  "Result<path>: cell result `res` (a CellResult) written atomically to its
   `result-path` under `state-dir`. A value that is no CellResult is
   :compose/invalid-result, nothing written."
  [state-dir res]
  (if-not (valid-result? res)
    (r/err :compose/invalid-result {:problems (me/humanize (m/explain CellResult res))})
    (r/try-effect* :compose/result-unwritable
      (write-edn! (result-path state-dir (:run/id res) (:suite/id res) (:project res)) res))))

(defn read-result
  "Result<CellResult> of the EDN file at `path`: unreadable EDN is
   :compose/result-unreadable, a value that is no CellResult
   :compose/invalid-result."
  [path]
  (r/let-ok [v (r/try-effect* :compose/result-unreadable (edn/read-string (slurp (io/file path))))]
    (if (valid-result? v)
      (r/ok v)
      (r/err :compose/invalid-result {:path path :problems (me/humanize (m/explain CellResult v))}))))

(defn read-results
  "{:results [cell-result (with :result/path)] :skipped [{:path :error}]} of
   every result file under `state-dir` (of `run` when given)."
  [state-dir run]
  (reduce (fn [acc {:keys [path run]}]
            (let [res (read-result path)]
              (cond
                (r/err? res)
                (update acc :skipped conj {:path path :error (:error res)})

                ;; the directory is the authority: a file naming another run is not trusted
                (not= run (get-in res [:ok :run/id]))
                (update acc :skipped conj {:path path :error :compose/result-run-mismatch
                                           :dir run :run/id (get-in res [:ok :run/id])})

                :else (update acc :results conj (assoc (:ok res) :result/path path)))))
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
