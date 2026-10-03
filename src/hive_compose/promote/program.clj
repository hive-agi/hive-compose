(ns hive-compose.promote.program
  "Pure: a Program is a host process a section needs beside its containers (a
   shadow-cljs watch, a JVM with an nREPL). What starts it and where its nREPL
   listens are derived from the build files of its directory (`facts`), unless
   the Program says so itself.

   Facts are the parsed build files, keyed by file name:

     {\"shadow-cljs.edn\" {..} \"deps.edn\" {..} \"bb.edn\" {..} \"project.clj\" true}

   A Clojure kind always starts with its nREPL: shadow-cljs opens its own, a
   deps.edn project runs its `:nrepl` alias or a plain nrepl.cmdline, lein and
   babashka run their headless REPL servers."
  (:require [clojure.string :as str]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def nrepl-deps
  "The -Sdeps a deps.edn project without an nREPL alias is started with."
  "{:deps {nrepl/nrepl {:mvn/version \"1.7.0\"} cider/cider-nrepl {:mvn/version \"0.62.2\"}}}")

(def kinds
  "Build file -> kind, in the order a directory holding several is read."
  [["shadow-cljs.edn" :shadow]
   ["deps.edn" :deps]
   ["project.clj" :lein]
   ["bb.edn" :bb]])

(defn kind
  "The kind of the directory `facts` describes, :plain when it has no Clojure
   build file."
  [facts]
  (or (some (fn [[file k]] (when (get facts file) k)) kinds) :plain))

(defn port-value
  "A port out of an EDN value: the int itself, or the int default of a tagged
   reader form such as `#shadow/env [\"PORT\" :default 7903]`."
  [v]
  (cond
    (int? v) v
    (tagged-literal? v) (let [form (:form v)]
                          (when (sequential? form)
                            (or (some (fn [[k x]] (when (and (= :default k) (int? x)) x))
                                      (partition 2 1 form))
                                (first (filter int? form)))))
    :else nil))

(defn- opt-port
  "The int after --port or -p in command line `opts`."
  [opts]
  (some (fn [[flag v]]
          (when (contains? #{"--port" "-p"} flag)
            (cond (int? v) v
                  (string? v) (parse-long v))))
        (partition 2 1 opts)))

(defn- shadow [facts _port]
  (let [edn (get facts "shadow-cljs.edn")
        builds (sort (map name (keys (:builds edn))))]
    {:program/command (into ["npx" "shadow-cljs" "watch"] builds)
     :program/nrepl-port (port-value (get-in edn [:nrepl :port]))}))

(defn- deps [facts port]
  (let [alias (get-in facts ["deps.edn" :aliases :nrepl])
        declared (opt-port (:main-opts alias))]
    (if (and alias (or (nil? port) (= port declared)))
      {:program/command ["clojure" "-M:nrepl"]
       :program/nrepl-port declared}
      {:program/command (cond-> ["clojure" "-Sdeps" nrepl-deps "-M" "-m" "nrepl.cmdline"
                                 "--middleware" "[cider.nrepl/cider-middleware]"]
                          port (into ["--port" (str port)]))
       :program/nrepl-port port})))

(defn- lein [_facts port]
  {:program/command (cond-> ["lein" "repl" ":headless"] port (into [":port" (str port)]))
   :program/nrepl-port port})

(defn- bb [_facts port]
  (let [port (or port 1667)]
    {:program/command ["bb" "nrepl-server" (str port)]
     :program/nrepl-port port}))

(defn detect
  "{:program/kind :program/command :program/nrepl-port} a directory with `facts`
   starts with, as kind `k` (default: what the facts say) and listening on
   `port` when the kind lets the caller choose. A :plain directory has no
   command."
  ([facts] (detect facts nil nil))
  ([facts k port]
   (let [k (or k (kind facts))]
     (assoc (case k
              :shadow (shadow facts port)
              :deps (deps facts port)
              :lein (lein facts port)
              :bb (bb facts port)
              {})
            :program/kind k))))

(defn resolved
  "Result<Program> ready to start: `program` completed from `facts`. A command
   or nREPL port the Program declares wins over the detected one."
  [program facts]
  (let [found (detect facts (:program/kind program) (:program/nrepl-port program))
        command (or (:program/command program) (:program/command found))
        port (or (:program/nrepl-port program) (:program/nrepl-port found))]
    (if (nil? command)
      (r/err :compose/program-needs-command
             {:program (:program/id program) :dir (:program/dir program)
              :hint "no Clojure build file found; declare :program/command"})
      (r/ok (cond-> (assoc program
                           :program/kind (:program/kind found)
                           :program/command command)
              port (assoc :program/nrepl-port port))))))

(defn wanted
  "Programs of `programs` a section running `services` starts: those paired
   with one of the services through :program/with, and those paired with none."
  [programs services]
  (let [services (set services)]
    (filterv (fn [p] (or (empty? (:program/with p)) (some services (:program/with p))))
             programs)))

(defn running-in
  "{program-id Running} of every program the active entries of `project` hold,
   entry `except` left out."
  [active project except]
  (into {}
        (comp (remove (fn [[k _]] (= k except)))
              (filter (fn [[_ a]] (= project (:project a))))
              (mapcat (fn [[_ a]] (:programs a))))
        active))

(defn release-programs
  "{program-id Running} active entry `id` can stop: its programs that no other
   active entry of its project holds and that are not in `keep`."
  [active id keep]
  (let [{:keys [project programs]} (get active id)
        held (running-in active project id)]
    (into {} (remove (fn [[pid _]] (or (contains? held pid) (contains? keep pid)))) programs)))

(defn command-line
  "The shell words of `command`: an argv as is, a string through `sh -c`."
  [command]
  (if (string? command) ["sh" "-c" command] (vec command)))

(defn quoted
  "`s` as one POSIX shell word."
  [s]
  (str "'" (str/replace (str s) "'" "'\\''") "'"))

(defn detached-script
  "The `sh -c` script that replaces the shell with `command` as the leader of a
   new session, its output appended to `log`. The spawned pid is therefore the
   program's and its process group's."
  [command log]
  (str "exec setsid "
       (str/join " " (map quoted (command-line command)))
       " >> " (quoted log) " 2>&1 < /dev/null"))

(defn log-file
  "Where the output of `program` of compose project `project` goes under `log-dir`."
  [log-dir project program]
  (str log-dir "/" project "-" (:program/id program) ".log"))
