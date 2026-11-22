(ns hive-compose.boundary.report
  "Test report files: the JDK XML parser, hardened, and the reading of a
   runner's report directory. The folding into a Report is pure, in
   `hive-compose.promote.report`; here XML text becomes a plain element tree
   {:tag :attrs :content :text} and files become text.

   The parser forbids DOCTYPE declarations outright, so neither external
   entities (XXE) nor entity expansion (billion laughs) can happen, and loads
   no external DTD or schema."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [hive-compose.promote.report :as report]
            [hive-dsl.result :as r])
  (:import (java.io StringReader)
           (javax.xml XMLConstants)
           (javax.xml.parsers DocumentBuilderFactory)
           (org.w3c.dom Element Node)
           (org.xml.sax ErrorHandler InputSource)))

;; SPDX-License-Identifier: MIT

(defn- factory ^DocumentBuilderFactory []
  (doto (DocumentBuilderFactory/newInstance)
    (.setFeature "http://apache.org/xml/features/disallow-doctype-decl" true)
    (.setFeature "http://xml.org/sax/features/external-general-entities" false)
    (.setFeature "http://xml.org/sax/features/external-parameter-entities" false)
    (.setFeature "http://apache.org/xml/features/nonvalidating/load-external-dtd" false)
    (.setFeature XMLConstants/FEATURE_SECURE_PROCESSING true)
    (.setAttribute XMLConstants/ACCESS_EXTERNAL_DTD "")
    (.setAttribute XMLConstants/ACCESS_EXTERNAL_SCHEMA "")
    (.setXIncludeAware false)
    (.setExpandEntityReferences false)
    (.setNamespaceAware false)))

(def ^:private quiet
  "Errors are thrown and become a Result; nothing is printed to stderr."
  (reify ErrorHandler
    (warning [_ _])
    (error [_ e] (throw e))
    (fatalError [_ e] (throw e))))

(defn- element->tree
  "A DOM element as {:tag :attrs :content :text}: :content its child
   elements, :text its character data (CDATA included) concatenated."
  [^Element el]
  (let [attrs (.getAttributes el)
        kids (.getChildNodes el)
        nodes (map #(.item kids %) (range (.getLength kids)))]
    {:tag (.getTagName el)
     :attrs (into {} (map (fn [i] (let [a (.item attrs i)] [(.getNodeName a) (.getNodeValue a)])))
                  (range (.getLength attrs)))
     :content (mapv element->tree (filter #(= Node/ELEMENT_NODE (.getNodeType ^Node %)) nodes))
     :text (apply str (keep (fn [^Node n]
                              (when (#{Node/TEXT_NODE Node/CDATA_SECTION_NODE} (.getNodeType n))
                                (.getNodeValue n)))
                            nodes))}))

(defn xml-tree
  "Result<element tree> of XML text `s`; unparseable XML, or XML declaring a
   DOCTYPE, is `:report/malformed`."
  [s]
  (if-not (string? s)
    (report/malformed :xml/not-text)
    (let [res (r/try-effect* :report/malformed
                (let [builder (doto (.newDocumentBuilder (factory)) (.setErrorHandler quiet))]
                  (element->tree (.getDocumentElement
                                  (.parse builder (InputSource. (StringReader. (str/replace-first s #"^\uFEFF" ""))))))))]
      (if (r/err? res)
        (report/malformed :xml/unparseable :message (:message res))
        res))))

(defn parse-junit
  "Result<Report> from JUnit XML text."
  [s]
  (r/let-ok [tree (xml-tree s)]
    (report/from-junit-tree tree)))

(def max-bytes
  "Largest report file read, 64 MiB."
  (* 64 1024 1024))

(def formats
  "What a suite may declare as its report format (:suite/report-format)."
  #{:auto :junit :tap})

(defn parse
  "Result<Report> from report text. `format` is :junit, :tap or :auto (the
   default), which detects it from the content. Never throws."
  ([s] (parse s :auto))
  ([s format]
   (try
     (case (if (= :auto format) (report/detect-format s) format)
       :junit (parse-junit s)
       :tap (report/parse-tap s)
       nil (report/malformed :format/unknown)
       (report/malformed :format/unsupported :format format))
     (catch StackOverflowError _
       (report/malformed :report/too-deep)))))

(defn report-file?
  "True for a file a runner leaves its report in: .xml or .tap."
  [f]
  (let [f (io/file f)]
    (and (.isFile f) (re-find #"(?i)\.(xml|tap)$" (.getName f)))))

(defn read-report
  "Result<Report> of the report file at `path` in `format` (default :auto),
   the file named in an error."
  ([path] (read-report path :auto))
  ([path format]
   (r/let-ok [text (r/try-effect* :report/unreadable
                     (let [f (io/file path)]
                       (when (> (.length f) max-bytes)
                         (throw (ex-info "report file too large" {:bytes (.length f) :max max-bytes})))
                       (slurp f)))]
     (r/map-err (parse text format) #(assoc % :file (str path))))))

(defn- foreign-xml?
  "True for an err of an .xml file that is XML but not JUnit (Maven's
   failsafe-summary.xml, TestNG's testng-results.xml): in :auto such a file
   lies beside the reports and is skipped, not a failure."
  [res]
  (= :junit/unknown-root (:reason res)))

(def mtime-slack-ms
  "How much older than a run's start a report file may look and still be
   read: file systems keep modification times coarsely."
  2000)

(defn read-reports
  "Result<Report> merging every report file under directory `dir` (a suite's
   :suite/reports-dir), sorted by path, each read in `format` (default
   :auto). With `since-ms`, files last modified before it (less
   `mtime-slack-ms`) are left out: the leftovers of an earlier run in a
   directory hive-compose does not own. A file that cannot be read fails the
   whole, except that :auto skips XML whose root is not JUnit (listed under
   :report/skipped-files); a directory holding no report is `:report/none`."
  ([dir] (read-reports dir :auto))
  ([dir format] (read-reports dir format nil))
  ([dir format since-ms]
   (r/let-ok [files (r/try-effect* :report/unreadable
                      (vec (sort-by str (filter #(and (report-file? %)
                                                      (or (nil? since-ms)
                                                          (>= (.lastModified (io/file %)) (- since-ms mtime-slack-ms))))
                                                (file-seq (io/file dir))))))]
     (loop [files files acc [] skipped []]
       (if-let [f (first files)]
         (let [res (read-report f format)]
           (cond
             (r/ok? res) (recur (rest files) (conj acc (:ok res)) skipped)
             (and (= :auto format) (foreign-xml? res)) (recur (rest files) acc (conj skipped (str f)))
             :else res))
         (if (empty? acc)
           (r/err :report/none {:dir (str dir) :skipped-files skipped})
           (r/ok (cond-> (report/merge-reports acc)
                   (seq skipped) (assoc :report/skipped-files skipped)))))))))

(defn clear-reports!
  "Result<{:dir :removed n}>: every report file (.xml, .tap) under `dir`
   deleted, so a runner's attempt is never read together with a previous
   one's. Only report files go; nothing else in `dir` is touched. An absent
   `dir` removes nothing. Use it on a directory hive-compose owns."
  [dir]
  (r/try-effect* :report/unclearable
    (let [files (filter report-file? (file-seq (io/file dir)))]
      (doseq [f files] (io/delete-file f true))
      {:dir (str dir) :removed (count files)})))
