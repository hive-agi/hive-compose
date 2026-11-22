(ns hive-compose.promote.report
  "Pure: test reports folded into one shape, whatever wrote them.

   A suite runner, in any language, leaves JUnit XML or TAP behind. Both fold
   into a Report:

     {:report/format :junit | :tap | :mixed
      :report/cases  [{:case/id :case/name :case/classname :case/status
                       :case/time-s :case/message :case/output}]
      :report/totals {:passed n :failed n :error n :skipped n}}

   JUnit arrives here as a plain element tree, {:tag :attrs :content :text}
   (`hive-compose.boundary.report` owns the XML parser and the files); TAP as
   text. Every entry point answers a hive-dsl Result: what cannot be read is
   `:report/malformed`."
  (:require [clojure.string :as str]
            [hive-dsl.result :as r]
            [malli.core :as m]))

;; SPDX-License-Identifier: MIT

;; ---------------------------------------------------------------------------
;; Shape
;; ---------------------------------------------------------------------------

(def statuses
  "Every status a case can end in, in reporting order."
  [:passed :failed :error :skipped])

(def Status (into [:enum] statuses))

(def Case
  [:map
   [:case/id :string]
   [:case/name :string]
   [:case/classname [:maybe :string]]
   [:case/status Status]
   [:case/time-s [:maybe :double]]
   [:case/message [:maybe :string]]
   [:case/output [:maybe :string]]])

(def Totals
  [:map
   [:passed nat-int?]
   [:failed nat-int?]
   [:error nat-int?]
   [:skipped nat-int?]])

(def Report
  "Kept local: hive-compose.schema belongs to the stack value objects."
  [:map
   [:report/format [:enum :junit :tap :mixed]]
   [:report/cases [:vector Case]]
   [:report/totals Totals]
   [:report/bail-out {:optional true} :string]
   [:report/skipped-files {:optional true} [:vector :string]]])

(def valid-report? (m/validator Report))

(defn totals
  "{status count} over `cases`, every status present."
  [cases]
  (merge (zipmap statuses (repeat 0))
         (frequencies (map :case/status cases))))

(defn ->report
  "A Report of `format` holding `cases`, its totals counted from them."
  [format cases]
  {:report/format format
   :report/cases (vec cases)
   :report/totals (totals cases)})

(defn- blank->nil [s]
  (when-not (str/blank? s) s))

(defn malformed
  "The Result of a report that cannot be read, `reason` saying why."
  [reason & {:as data}]
  (r/err :report/malformed (assoc data :reason reason)))

(defn- tap-case [id status message]
  {:case/id id :case/name id :case/classname nil :case/status status
   :case/time-s nil :case/message message :case/output nil})

;; ---------------------------------------------------------------------------
;; JUnit: element tree -> Report
;; ---------------------------------------------------------------------------

(defn junit-case-id
  "`<suite>/<classname>.<name>`, leaving out the parts that are missing, and
   the classname when it repeats the name (jest-junit writes both the same)."
  [suite classname nm]
  (let [leaf (str/join "." (keep blank->nil [(when (not= classname nm) classname) nm]))]
    (str/join "/" (keep blank->nil [suite leaf]))))

;; Possessive throughout: no quantifier gives back what it took, so a long run
;; of digits is read once (`\d+\.?\d*` was quadratic on a dotless run).
(def ^:private seconds-re #"[+-]?+(?:\d++(?:\.\d*+)?+|\.\d++)(?:[eE][+-]?+\d++)?+")

(defn- decimal-comma
  "`s` with its comma read as a decimal separator (`0,123` is `0.123`), nil
   when the comma could be a thousands separator instead (`1,234`: one to
   three digits not starting with 0, then exactly three), which is not
   supported, so no time is guessed. `s` without a comma is `s`."
  [^String s]
  (if-not (str/includes? s ",")
    s
    (when-let [[_ sign whole frac] (re-matches #"([+-]?+)(\d++),(\d++)" s)]
      (when-not (and (re-matches #"[1-9]\d{0,2}" whole) (= 3 (count frac)))
        (str sign whole "." frac)))))

(defn parse-seconds
  "A JUnit time attribute as seconds, nil when absent or unreadable. A comma
   may be the decimal separator (`0,123` is 0.123); thousands separators are
   not supported, so `1,234` and `1,234.5` are nil rather than a guess."
  [s]
  (when-let [s (some-> s str/trim blank->nil decimal-comma)]
    (when (re-matches seconds-re s)
      (let [d (parse-double s)]
        (when (and d (Double/isFinite d)) d)))))

(defn- children [el tag]
  (filter #(= tag (:tag %)) (:content el)))

(defn- trimmed-text [el]
  (some-> (:text el) str/trim blank->nil))

(defn- outcome-message
  "message attribute and body of a <failure>, <error> or <skipped>, joined."
  [el]
  (blank->nil (str/join "\n" (keep blank->nil [(some-> (get-in el [:attrs "message"]) str/trim)
                                                (trimmed-text el)]))))

(defn- outcome
  "[status element] of testcase `tc`: an error wins over a failure, a failure
   over a skip."
  [tc]
  (or (some (fn [[tag status]] (when-let [el (first (children tc tag))] [status el]))
            [["error" :error] ["failure" :failed] ["skipped" :skipped]])
      [:passed nil]))

(defn- junit-case [suite tc]
  (let [attrs (:attrs tc)
        nm (or (blank->nil (get attrs "name")) "")
        classname (blank->nil (get attrs "classname"))
        [status el] (outcome tc)
        output (keep #(some-> (first (children tc %)) trimmed-text) ["system-out" "system-err"])]
    {:case/id (junit-case-id suite classname nm)
     :case/name nm
     :case/classname classname
     :case/status status
     :case/time-s (parse-seconds (get attrs "time"))
     :case/message (some-> el outcome-message)
     :case/output (blank->nil (str/join "\n" output))}))

(defn- suite-cases
  "Cases under `el` in document order; `suite` names the nearest enclosing
   <testsuite> that has a name."
  [suite el]
  (mapcat (fn [child]
            (case (:tag child)
              "testcase" [(junit-case suite child)]
              "testsuite" (suite-cases (or (blank->nil (get-in child [:attrs "name"])) suite) child)
              "testsuites" (suite-cases suite child)
              nil))
          (:content el)))

(defn from-junit-tree
  "Result<Report> from a JUnit element tree rooted at <testsuites> or
   <testsuite>, nesting allowed. Missing attributes are tolerated; any other
   root is `:report/malformed`."
  [tree]
  (case (:tag tree)
    "testsuites" (r/ok (->report :junit (suite-cases nil tree)))
    "testsuite" (r/ok (->report :junit (suite-cases nil {:content [tree]})))
    (malformed :junit/unknown-root :root (:tag tree))))

;; ---------------------------------------------------------------------------
;; TAP 13/14: text -> Report
;; ---------------------------------------------------------------------------

(def max-line
  "Characters of a TAP line that are read; the rest of a longer line is
   dropped, so no line costs more than a bounded, linear scan."
  65536)

(defn- capped-lines
  "The lines of `text`, each cut to its first `max-line` characters."
  [text]
  (map #(if (> (count %) max-line) (subs % 0 max-line) %) (str/split-lines text)))

;; Every regex here is linear on its line. Each quantifier is possessive, so
;; none gives back what it took, and a line's tail is `(?s).*\z`: `.` takes
;; the line terminators a plain `.` and `$` stop at (U+0085, U+2028, U+2029
;; stay inside a line, since `str/split-lines` splits on \n only), so a
;; whitespace run followed by one never backtracks. The `regexes-stay-linear`
;; test holds them to it, through a bounded future.
(def ^:private test-re #"(?s)^(not ok|ok)(?=\s|\z)(?:\s++(\d++)(?=\s|\z))?+\s*+(?:-\s*+)?+(.*)\z")
(def ^:private plan-re #"(?s)^1\.\.(\d{1,9})(?:\s*+#.*)?+\z")
(def ^:private bail-re #"(?s)^Bail out!\s*+(.*)\z")
(def ^:private version-re #"(?i)^TAP version \d++\z")
(def ^:private directive-re #"(?i)#\s*+(skip(?:ped)?|todo)(?=[\s:]|\z)[\s:]*+")

(defn- indent [line]
  (count (re-find #"^ *" line)))

(defn- depth
  "Subtest depth of `line`: TAP 14 indents a subtest by four spaces."
  [line]
  (quot (indent line) 4))

(defn- token [line]
  (let [body (str/trim line)
        d (depth line)]
    (cond
      (re-matches version-re body) {:kind :version :depth d}
      (re-matches bail-re body) {:kind :bail :depth d :reason (second (re-matches bail-re body))}
      (re-matches plan-re body) {:kind :plan :depth d :n (parse-long (second (re-matches plan-re body)))}
      (re-matches test-re body) {:kind :test :depth d :body body}
      (str/starts-with? body "#") {:kind :comment :depth d :text body}
      :else {:kind :other :depth d})))

(defn- yaml-end?
  "True when `line` ends a YAML block opened by a `---` indented `base`: its
   `...`, a line indented less, or a TAP line (an unterminated block never
   swallows the test points after it)."
  [base line]
  (or (= "..." (str/trim line))
      (and (not (str/blank? line))
           (or (< (indent line) base)
               (and (= (indent line) base)
                    (#{:test :plan :bail :version} (:kind (token line))))))))

(defn- lex
  "TAP lines as tokens, each YAML block `---` .. `...` collapsed into one
   :yaml token holding its raw text."
  [lines]
  (loop [lines lines out []]
    (if-let [line (first lines)]
      (if (= "---" (str/trim line))
        (let [[block more] (split-with (complement (partial yaml-end? (indent line))) (rest lines))
              closed? (= "..." (some-> (first more) str/trim))]
          (recur (if closed? (rest more) more)
                 (conj out {:kind :yaml :depth (depth line)
                            :text (str/join "\n" (concat [line] block (when closed? [(first more)])))})))
        (recur (rest lines) (conj out (token line))))
      out)))

(defn- find-directive
  "[index word end] of the `# SKIP` / `# TODO` directive in `text`: the first
   `#` at the start or after whitespace (so never an escaped `\\#`) followed
   by the word skip, skipped or todo standing alone."
  [^String text]
  (let [m (re-matcher directive-re text)
        n (count text)]
    (loop [i (str/index-of text "#")]
      (when i
        (if (and (or (zero? i) (Character/isWhitespace (.charAt text (int (dec i)))))
                 (do (.region m i n) (.lookingAt m)))
          [i (.group m 1) (.end m)]
          (recur (str/index-of text "#" (inc i))))))))

(defn- split-directive
  "[description directive reason] of a test point's text; an escaped `\\#`
   belongs to the description."
  [text]
  (let [unescape #(str/trim (str/replace % "\\#" "#"))]
    (if-let [[i word end] (find-directive text)]
      [(unescape (subs text 0 i))
       (if (str/starts-with? (str/lower-case word) "skip") :skip :todo)
       (blank->nil (str/trim (subs text end)))]
      [(unescape text) nil nil])))

(defn- point-status [ok? directive]
  (case directive
    :skip :skipped
    :todo (if ok? :passed :skipped)
    (if ok? :passed :failed)))

(defn- test-point
  "A test point from its line; `n` is the number an unnumbered point takes."
  [body n]
  (let [[_ ok num text] (re-matches test-re body)
        ok? (= "ok" ok)
        [desc directive reason] (split-directive text)
        num (or (some-> num parse-long) n)
        id (or (blank->nil desc) (str "test " num))]
    {:num num
     :case (assoc (tap-case id (point-status ok? directive)
                            (when directive
                              (str/join " " (keep identity [(str/upper-case (name directive)) reason]))))
                  :case/name id)}))

(defn- yaml-seconds
  "duration_ms (node:test) of a YAML diagnostic block, in seconds."
  [yaml]
  (when-let [ms (some->> yaml (re-find #"(?m)^[ \t]*+duration_ms:[ \t]*+([0-9.]++(?:[eE][+-]?+\d++)?+)[ \t]*+$") second parse-double)]
    (* 0.001 ms)))

(defn- attach
  "The last point of `points` with YAML block or comment `t` folded in: the
   YAML is its message (raw), a comment is a line of its output."
  [points {:keys [kind text]}]
  (let [p (peek points)
        p (case kind
            :yaml (-> p
                      (assoc-in [:case :case/message] text)
                      (update-in [:case :case/time-s] #(or % (yaml-seconds text))))
            :comment (update-in p [:case :case/output] #(if % (str % "\n" text) text)))]
    (conj (pop points) p)))

(def max-missing
  "More tests than this missing from a plan reads as a broken plan line, not
   as that many errors."
  10000)

(defn- implausible-plan? [plan points]
  (and plan (> (- plan (count points)) max-missing)))

(defn- missing
  "An :error case per test number plan `plan` promised that never ran."
  [plan points bail]
  (when plan
    (let [seen (set (map :num points))
          why (if bail (str "Bail out! " bail) (str "not run: missing from plan 1.." plan))]
      (for [i (range 1 (inc plan)) :when (not (seen i))]
        (tap-case (str "test " i) :error why)))))

(declare fold-level)

(defn- with-shortfall
  "Subtest points `pending` and an :error point per test the subtest's plan
   `plan` promised that is not among them, as at the top level, so a subtest
   cut short fails the point that closes it. `budget` (a volatile) is how
   many such points every subtest of the parse may still add together; a
   subtest plan overdrawing it reads as a broken plan line and throws
   ::implausible-plan, which `parse-tap` answers as `:tap/implausible-plan`.
   So the subtests of one parse add at most `max-missing` points between
   them, however many there are and however deep."
  [pending plan budget]
  (let [short (vec (take (inc @budget) (missing plan pending nil)))]
    (when (> (count short) @budget)
      (throw (ex-info "subtest plan implausible" {::implausible-plan plan})))
    (vswap! budget - (count short))
    (into pending (map (fn [c] {:num nil :case c :children []})) short)))

(defn- fold-subtest
  "{:points :plan} of the subtest whose tokens are `pending`, folded at the
   depth of its shallowest test point or plan (stray output, a comment or a
   YAML block indented less never sets it). Tokens holding neither are
   stray output: no points, no plan."
  [pending budget]
  (if-let [ds (seq (keep #(when (#{:test :plan} (:kind %)) (:depth %)) pending))]
    (fold-level pending (apply min ds) budget)
    {:points [] :plan nil}))

(defn- unclosed
  "What a subtest no point closed leaves at its parent's level, when the
   stream ends inside it (truncated, or bailed out): an :error point taking
   a number past every one its `siblings` hold, its points as children (so
   their ids are prefixed and never collide with the parent's), no
   shortfall of its own (the error stands for the whole subtest). A run of
   deeper lines holding no point and no plan is stray output and leaves
   nothing."
  [pending siblings budget]
  (when (seq pending)
    (let [{:keys [points plan]} (fold-subtest pending budget)
          ;; past the highest number any sibling holds, not the last one:
          ;; points may come out of order, and an id must never repeat
          num (inc (reduce max 0 (keep :num siblings)))]
      (when (or (seq points) plan)
        [{:num num
          :case (tap-case (str "test " num) :error "not run: subtest never closed by a test point")
          :children points}]))))

(defn- fold-level
  "Fold the tokens of one level, `d` deep, into {:points [..] :plan n}.
   Deeper tokens are a subtest; TAP 14 prints it before the point that closes
   it, which takes the subtest's points as :children, plus its plan's
   shortfall (see `with-shortfall`). The deeper tokens are gathered until
   that point and folded once, then, at every depth below this one: a blank
   line, stray output or a comment at this depth (or shallower) inside a
   subtest is dropped, never splitting it, so its numbering and its plan
   stay whole. A subtest no point closes is `unclosed`."
  [tokens d budget]
  (loop [tokens tokens points [] plan nil pending [] n 0 open? false]
    (if-let [{:keys [kind] :as t} (first tokens)]
      (cond
        (and (> (:depth t) d) (or (seq pending) (not (#{:yaml :comment} kind))))
        (recur (rest tokens) points plan (conj pending t) n false)

        (= :test kind)
        (let [children (if (seq pending)
                         (let [sub (fold-subtest pending budget)]
                           (with-shortfall (:points sub) (:plan sub) budget))
                         [])
              p (assoc (test-point (:body t) (inc n)) :children children)]
          (recur (rest tokens) (conj points p) plan [] (:num p) true))

        (= :plan kind) (recur (rest tokens) points (:n t) pending n false)

        (and open? (= :yaml kind))
        (recur (rest tokens) (attach points t) plan pending n true)

        (and open? (= :comment kind) (<= (:depth t) d)
             (not (re-find #"(?i)^#\s*subtest\b" (:text t))))
        (recur (rest tokens) (attach points t) plan pending n true)

        :else (recur (rest tokens) points plan pending n false))
      {:points (into points (unclosed pending points budget)) :plan plan})))

(def max-id-chars
  "Characters the case ids of one TAP parse may hold together. A subtest's
   ids repeat its parent's description, so a long description over many
   leaves would grow the Report far past its input: past this it reads as
   a hostile report, `:tap/implausible-ids`."
  (* 32 1024 1024))

(defn- charge-ids
  "`cases` as a vector, throwing ::implausible-ids once their ids together
   pass `max-id-chars`. Realizes them one at a time, so no more than the
   budget (plus one id, itself bounded by the input) is ever held."
  [cases]
  (let [total (volatile! 0)]
    (mapv (fn [c]
            (when (> (vswap! total + (count (:case/id c))) max-id-chars)
              (throw (ex-info "case ids implausible" {::implausible-ids true})))
            c)
          cases)))

(defn- prefix-id [parent child]
  (update child :case assoc :case/id (str (get-in parent [:case :case/id]) "/" (get-in child [:case :case/id]))))

(defn- leaves
  "Points standing for cases: a point with subtests stands for its leaves,
   and for itself too when it failed while none of them did (a hook failing)."
  [points]
  (mapcat (fn [p]
            (let [kids (map #(prefix-id p %) (leaves (:children p)))
                  bad? #(#{:failed :error} (get-in % [:case :case/status]))]
              (cond
                (empty? kids) [p]
                (and (bad? p) (not-any? bad? kids)) (concat kids [p])
                :else kids)))
          points))

(defn parse-tap
  "Result<Report> from TAP 13/14 text: a plan `1..N` before or after the
   tests, `ok`/`not ok` points with `# SKIP`/`# TODO` directives, a YAML
   diagnostic block kept raw as its point's message, trailing `#` comments as
   its output, subtests flattened into `parent/child` ids. A test the plan
   promised and that never ran counts as :error, a subtest's plan included
   (`parent/test N`); so does a subtest no test point closes (`test N`,
   N past every number its level used), and `Bail out!` without a plan.
   Text with no plan, no test point and no bail-out, a plan promising more
   than `max-missing` tests beyond those that ran, or case ids holding more
   than `max-id-chars` characters together, is `:report/malformed`."
  [text]
  (if-not (string? text)
    (malformed :tap/not-text)
    (try
      (let [lines (capped-lines (str/replace (str/replace-first text #"^\uFEFF" "") #"\r\n?" "\n"))
            tokens (lex lines)
            [run [bail]] (split-with #(not= :bail (:kind %)) tokens)
            bail (some-> bail :reason str/trim)
            {:keys [points plan]} (fold-level (vec run) 0 (volatile! max-missing))]
        (cond
          (and (nil? plan) (empty? points) (nil? bail)) (malformed :tap/no-tests)
          (implausible-plan? plan points) (malformed :tap/implausible-plan :plan plan)
          :else
          (let [cases (charge-ids
                       (concat (map :case (leaves points))
                               (missing plan points bail)
                               (when (and bail (nil? plan))
                                 [(tap-case "Bail out!" :error (str/trim (str "Bail out! " bail)))])))]
            (r/ok (cond-> (->report :tap cases)
                    bail (assoc :report/bail-out bail))))))
      (catch clojure.lang.ExceptionInfo e
        (let [data (ex-data e)]
          (cond
            (::implausible-plan data) (malformed :tap/implausible-plan :plan (::implausible-plan data))
            (::implausible-ids data) (malformed :tap/implausible-ids :max-id-chars max-id-chars)
            :else (throw e))))
      (catch StackOverflowError _
        (malformed :tap/too-deep)))))

;; ---------------------------------------------------------------------------
;; Format, merge, query
;; ---------------------------------------------------------------------------

(defn detect-format
  "The format of report `content`: :junit for XML, :tap when a line is a TAP
   version, plan or test point, nil otherwise. Each line is read as
   `parse-tap` reads it, cut to `max-line` characters, so no line costs more
   than a bounded, linear scan."
  [content]
  (when (string? content)
    (let [s (str/triml (str/replace-first content "\uFEFF" ""))]
      (cond
        (str/starts-with? s "<") :junit
        (some #(let [l (str/trim %)]
                 (or (re-matches version-re l) (re-matches plan-re l) (re-matches test-re l)))
              (capped-lines s))
        :tap))))

(defn merge-reports
  "One Report folding `reports`, the several files of one run: cases in
   order, totals recounted, format :mixed when theirs differ."
  [reports]
  (let [formats (distinct (map :report/format reports))
        bail (blank->nil (str/join "; " (keep :report/bail-out reports)))]
    (cond-> (->report (if (= 1 (count formats)) (first formats) :mixed)
                      (mapcat :report/cases reports))
      bail (assoc :report/bail-out bail))))

(defn failed-cases
  "The cases of `report` that failed or errored."
  [report]
  (filterv #(#{:failed :error} (:case/status %)) (:report/cases report)))

(defn passed?
  "True when `report` holds no failed or errored case."
  [report]
  (empty? (failed-cases report)))
