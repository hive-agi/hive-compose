(ns hive-compose.boundary.report
  "Test report files: the JDK XML parser, hardened, and the reading of a
   runner's report directory. The folding into a Report is pure, in
   `hive-compose.promote.report`; here XML text becomes a plain element tree
   {:tag :attrs :content :text} and files become text.

   The parser forbids DOCTYPE declarations outright, so neither external
   entities (XXE) nor entity expansion (billion laughs) can happen, and loads
   no external DTD or schema."
  (:require [clojure.java.io :as io]
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
                  (element->tree (.getDocumentElement (.parse builder (InputSource. (StringReader. s)))))))]
      (if (r/err? res)
        (report/malformed :xml/unparseable :message (:message res))
        res))))

(defn parse-junit
  "Result<Report> from JUnit XML text."
  [s]
  (r/let-ok [tree (xml-tree s)]
    (report/from-junit-tree tree)))

(defn parse
  "Result<Report> from report text of either format, detected from its
   content. Never throws."
  [s]
  (try
    (case (report/detect-format s)
      :junit (parse-junit s)
      :tap (report/parse-tap s)
      (report/malformed :format/unknown))
    (catch StackOverflowError _
      (report/malformed :report/too-deep))))

(defn report-file?
  "True for a file a runner leaves its report in: .xml or .tap."
  [f]
  (let [f (io/file f)]
    (and (.isFile f) (re-find #"(?i)\.(xml|tap)$" (.getName f)))))

(defn read-report
  "Result<Report> of the report file at `path`, the file named in an error."
  [path]
  (r/let-ok [text (r/try-effect* :report/unreadable (slurp (io/file path)))]
    (r/map-err (parse text) #(assoc % :file (str path)))))

(defn read-reports
  "Result<Report> merging every report file under directory `dir`, sorted by
   path. A file that cannot be read fails the whole; a directory holding no
   report is `:report/none`."
  [dir]
  (let [files (sort-by str (filter report-file? (file-seq (io/file dir))))]
    (if (empty? files)
      (r/err :report/none {:dir (str dir)})
      (loop [files files acc []]
        (if-let [f (first files)]
          (let [res (read-report f)]
            (if (r/err? res) res (recur (rest files) (conj acc (:ok res)))))
          (r/ok (report/merge-reports acc)))))))
