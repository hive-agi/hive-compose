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
           (java.nio.file CopyOption FileVisitOption Files LinkOption Path StandardCopyOption)
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

(declare report-files)

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
   whole, except that :auto skips XML whose root is not JUnit: each skip is
   {:file path :reason keyword}, the keyword its read answered, so a caller
   can say why the file was left out rather than only that it was. A
   directory holding no report is `:report/none`, its :skipped-files the same
   {:file :reason} rows, so a run that skipped every file still knows why."
  ([dir] (read-reports dir :auto))
  ([dir format] (read-reports dir format nil))
  ([dir format since-ms]
   (r/let-ok [files (r/try-effect* :report/unreadable
                     (vec (filter #(or (nil? since-ms)
                                       (>= (.lastModified (io/file %)) (- since-ms mtime-slack-ms)))
                                  (report-files dir))))]
     (loop [files files acc [] skipped []]
       (if-let [f (first files)]
         (let [res (read-report f format)]
           (cond
             (r/ok? res) (recur (rest files) (conj acc (:ok res)) skipped)
             (and (= :auto format) (foreign-xml? res))
             (recur (rest files) acc (conj skipped {:file (str f) :reason (:reason res)}))
             :else res))
         (if (empty? acc)
           (r/err :report/none {:dir (str dir) :skipped-files skipped
                                :hint (if (seq skipped)
                                        "every report file was skipped; each :reason says why"
                                        "no .xml or .tap report file in the directory")})
           (r/ok (cond-> (report/merge-reports acc)
                   (seq skipped) (assoc :report/skipped-files skipped)))))))))

(def ^:private no-follow (into-array LinkOption [LinkOption/NOFOLLOW_LINKS]))

(defn report-files
  "The report files (.xml, .tap) under directory `dir`, sorted by path,
   walked WITHOUT following links: a symlinked directory is not entered and
   a symlinked file is not a report, so nothing outside `dir` is ever read,
   moved or touched through it. An absent `dir` holds none; a nil or blank
   one throws (never read as the working directory)."
  [dir]
  (when (str/blank? (str dir))
    (throw (IllegalArgumentException. "a report dir is required")))
  (let [root (.toPath (io/file (str dir)))]
    (if-not (Files/isDirectory root no-follow)
      []
      (with-open [s (Files/walk root (make-array FileVisitOption 0))]
        (->> (iterator-seq (.iterator s))
             (filter (fn [^Path p] (and (Files/isRegularFile p no-follow)
                                        (re-find #"(?i)\.(xml|tap)$" (str (.getFileName p))))))
             (map #(.toFile ^Path %))
             (sort-by str)
             vec)))))

(defn- norm ^Path [x]
  (.normalize (.toAbsolutePath (.toPath (io/file (str x))))))

(defn- existing-real
  "The real path of the nearest existing ancestor-or-self of `p` strictly
   below `stop`, nil when none exists there."
  [^Path p ^Path stop]
  (loop [^Path q p]
    (cond
      (or (nil? q) (.startsWith stop q)) nil
      (Files/exists q (make-array LinkOption 0)) (try (.toRealPath q (make-array LinkOption 0))
                                                      (catch java.io.IOException _unresolvable nil))
      :else (recur (.getParent q)))))

(defn safe-reports-dir
  "Result<path>: `dir`, a suite's declared host reports dir, canonical, when
   it lies strictly inside the suite's own directory `root`. Refused as
   :compose/unsafe-reports-dir, naming the :reason, when it is `root` itself
   (:is-root), an ancestor of it (:ancestor), outside it (:outside), or when
   a link in its existing part resolves to any of those (:link-escapes):
   the pipeline moves every report file out of that dir before each cell,
   so it must never be a project or a home."
  [root dir]
  (let [refuse (fn [why rp dp]
                 (r/err :compose/unsafe-reports-dir
                        {:dir (str dp) :root (str rp) :reason why
                         :message (str "a suite's reports dir must lie strictly inside its :suite/dir ("
                                       (name why) "): " dp)}))]
    (if (or (str/blank? (str root)) (str/blank? (str dir)))
      (refuse :outside (str root) (str dir))
      (let [rp (norm root) dp (norm dir)]
        (cond
          (= rp dp) (refuse :is-root rp dp)
          (.startsWith rp dp) (refuse :ancestor rp dp)
          (not (.startsWith dp rp)) (refuse :outside rp dp)
          :else
          (let [rr (try (.toRealPath rp (make-array LinkOption 0)) (catch java.io.IOException _absent nil))
                rd (existing-real dp rp)]
            (if (and rr rd (or (.startsWith rr rd) (not (.startsWith rd rr))))
              (refuse :link-escapes rp dp)
              (r/ok (str dp)))))))))

(defn- move-all!
  "{:moved n :left [path]}: each of `files` (under `from`) moved into `to`,
   named by its path relative to `from` with `/` made `__`, so nested files
   never collide."
  [from to files]
  (let [root (.toPath (io/file (str from)))]
    (when (seq files) (.mkdirs (io/file (str to))))
    (reduce (fn [acc ^java.io.File f]
              (let [rel (str/replace (str (.relativize root (.toPath f))) "/" "__")
                    ok? (try (Files/move (.toPath f) (.toPath (io/file (str to) rel))
                                         (into-array CopyOption [StandardCopyOption/REPLACE_EXISTING]))
                             true
                             (catch Exception _unmovable false))]
                (if ok? (update acc :moved inc) (update acc :left conj (str f)))))
            {:moved 0 :left []} files)))

(defn stash-reports!
  "Result<{:dir :to :stashed n}>: every report file (.xml, .tap) under `dir`
   MOVED into directory `stash` (never deleted), so a runner's attempt is
   never read together with a previous one's. Only report files go, links are
   not followed (see `report-files`), nothing else in `dir` is touched. An
   absent `dir` moves nothing. A file that cannot be moved (another uid's, a
   read-only mount) is :report/unclearable naming every leftover: the
   attempt must not run on top of it."
  [dir stash]
  (r/let-ok [files (r/try-effect* :report/unclearable (report-files dir))]
    (let [{:keys [moved left]} (move-all! dir stash files)]
      (if (seq left)
        (r/err :report/unclearable {:dir (str dir) :leftovers left
                                    :message (str (count left) " report file(s) under " dir " cannot be moved away")})
        (r/ok {:dir (str dir) :to (str stash) :stashed moved})))))

(defn collect-reports!
  "Result<{:from :to :moved n}>: every report file under `from` (a suite's
   declared reports dir) moved into directory `to` (the cell's own), named
   by its path relative to `from` with `/` made `__`, so nested files never
   collide. Links are not followed. An absent `from` moves nothing."
  [from to]
  (r/let-ok [files (r/try-effect* :report/uncollectable (report-files from))]
    (let [{:keys [moved left]} (move-all! from to files)]
      (if (seq left)
        (r/err :report/uncollectable {:from (str from) :leftovers left
                                      :message (str (count left) " report file(s) under " from " cannot be moved")})
        (r/ok {:from (str from) :to (str to) :moved moved})))))
