(ns hive-compose.promote.override
  "Pure: the generated override that lets a private copy of a compose project
   (a stack's private instance, an isolated section run) run beside the
   shared one (FUN-D).

   No YAML is parsed here: the input is the MERGED model compose itself
   answers (`docker compose config --format json`, string keys as JSON gives
   them), the output an override DOCUMENT as data, which `emit` writes as a
   small deterministic YAML text appended as the last `-f`. Compose merge
   tags do the work (docker compose >= 2.24, see `tags-supported?`):

     container_name: !reset null       a fixed name would clash
     ports: !reset []                  no host binding (:strip)
     ports: !override [..]             exactly these bindings
     build: !reset null + image + pull_policy: never
                                       reuse the image the shared project built
     volumes: {<key>: {name: <new>}}   a per-run data volume

   External volumes and bind mounts are never touched; asking to freshen an
   external volume is an error. See schema/OverrideSpec and PortPolicy."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [hive-compose.schema :as schema]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

;; ---------------------------------------------------------------------------
;; tags and versions

(defn tagged
  "A YAML value carrying compose merge tag `tag` (\"!reset\", \"!override\"):
   {:tag t :value v}, plain data so a document persists as EDN. A compose
   map has string keys, so no compose value is mistaken for one."
  [tag v]
  {:tag tag :value v})

(defn tagged? [v] (and (map? v) (= #{:tag :value} (set (keys v)))))

(def reset-null (tagged "!reset" nil))
(def reset-list (tagged "!reset" []))

(def min-tags-version
  "The first docker compose version that reads !reset and !override."
  [2 24 0])

(defn version-numbers
  "[major minor patch] of a compose version string (\"v2.24.1\",
   \"2.24.0-desktop.1\", \"5.7.1\"), nil when it holds none."
  [s]
  (when-let [[_ a b c] (re-find #"(\d+)\.(\d+)(?:\.(\d+))?" (str s))]
    [(parse-long a) (parse-long b) (if c (parse-long c) 0)]))

(defn tags-supported?
  "True when compose `version` reads the merge tags the override uses."
  [version]
  (boolean (when-let [v (version-numbers version)]
             (>= (compare v min-tags-version) 0))))

;; ---------------------------------------------------------------------------
;; model reads

(defn- services [model] (get model "services" {}))

(defn host-port
  "The fixed host port of a compose port entry as a long (\"5432\", 5432),
   nil when none, 0 or a range."
  [p]
  (let [v (get p "published")]
    (cond (int? v) (when (pos? v) (long v))
          (and (string? v) (re-matches #"\d+" v)) (let [n (parse-long v)] (when (pos? n) n))
          :else nil)))

(defn port-key
  "How a container port is named in port maps: \"5432\" for tcp, \"53/udp\"
   for any other protocol."
  [target protocol]
  (if (or (nil? protocol) (= "tcp" protocol)) (str target) (str target "/" protocol)))

(defn published
  "{service {port-key host-port}}: the fixed host ports the model publishes."
  [model]
  (into (sorted-map)
        (for [[svc sm] (services model)
              :let [ps (into (sorted-map)
                             (keep (fn [p] (when-let [h (host-port p)]
                                             [(port-key (get p "target") (get p "protocol")) h])))
                             (get sm "ports"))]
              :when (seq ps)]
          [svc ps])))

(defn planned-ports
  "{service {port-key host-port}} a copy publishes under PortPolicy
   `policy`, known before `up`; nil for :ephemeral (docker picks)."
  [model policy]
  (let [pub (published model)]
    (cond
      (= :ephemeral policy) nil
      (or (nil? policy) (= :strip policy)) {}
      (= :keep policy) pub
      (:offset policy) (into (sorted-map) (for [[s m] pub]
                                            [s (into (sorted-map) (for [[k h] m] [k (+ h (:offset policy))]))]))
      (:publish policy) (into (sorted-map) (for [[s m] (:publish policy)
                                                 :let [m (into (sorted-map) (for [[c h] m :when (pos? h)] [(str c) h]))]
                                                 :when (seq m)]
                                             [s m]))
      :else {})))

(defn named-containers
  "Sorted services of `model` that set a container_name."
  [model]
  (vec (sort (for [[svc sm] (services model) :when (some? (get sm "container_name"))] svc))))

(defn external-volume? [model k] (true? (get-in model ["volumes" k "external"])))

(defn explicit-volumes
  "Sorted keys of the non-external top-level volumes whose name is not the
   one compose derives (`<project>_<key>`): every copy of the project would
   SHARE them whatever its -p."
  [model]
  (let [project (get model "name")]
    (vec (sort (for [[k v] (get model "volumes" {})
                     :when (not (true? (get v "external")))
                     :when (and (get v "name") (not= (get v "name") (str project "_" k)))]
                 k)))))

(defn build-only
  "Sorted services that `build:` with no `image:` (compose would build them
   anew under a new project name)."
  [model]
  (vec (sort (for [[svc sm] (services model)
                   :when (and (some? (get sm "build")) (str/blank? (str (get sm "image"))))]
               svc))))

(defn fresh-name
  "The per-run name of volume `key` of compose project `project` in `run`:
   `<project>_<key>_<run>`. Deterministic, distinct per run."
  [project key run]
  (str project "_" key "_" run))

(defn pinned-image
  "The image compose built for `service` of compose project `project`."
  [project service]
  (str project "-" service))

;; ---------------------------------------------------------------------------
;; the document

(defn- invalid [data] (r/err :compose/invalid-override data))

(defn- port-entry
  "A compose long-syntax port entry."
  [target host-ip protocol host]
  (cond-> {"target" target "protocol" (or protocol "tcp")}
    host-ip (assoc "host_ip" host-ip)
    (some? host) (assoc "published" (str host))))

(defn- ephemeral-ports [ports]
  (vec (distinct (for [p ports] (port-entry (get p "target") "127.0.0.1" (get p "protocol") nil)))))

(defn- shifted
  "Result<[entry]>: `ports` of `svc` with every fixed host port moved by `n`,
   the others kept as they are."
  [svc ports n]
  (reduce (fn [acc p]
            (let [h (host-port p)
                  moved (when h (+ h n))]
              (cond
                (nil? h) (r/map-ok acc #(conj % (port-entry (get p "target") (get p "host_ip") (get p "protocol")
                                                            (get p "published"))))
                (<= 1 moved 65535) (r/map-ok acc #(conj % (port-entry (get p "target") (get p "host_ip")
                                                                      (get p "protocol") moved)))
                :else (reduced (invalid {:service svc :port h :offset n
                                         :hint "the offset moves this host port out of 1..65535"})))))
          (r/ok [])
          ports))

(defn- ports-entries
  "Result<{service ports-value}> of PortPolicy `policy` over `model`."
  [model policy]
  (let [with-ports (into (sorted-map) (filter (comp seq #(get % "ports") val)) (services model))]
    (cond
      (= :keep policy) (r/ok {})
      (= :strip policy) (r/ok (into (sorted-map) (map (fn [[s _]] [s reset-list])) with-ports))
      (= :ephemeral policy) (r/ok (into (sorted-map)
                                        (map (fn [[s sm]] [s (tagged "!override" (ephemeral-ports (get sm "ports")))]))
                                        with-ports))
      (:offset policy)
      (reduce (fn [acc [s sm]]
                (if-not (some host-port (get sm "ports"))
                  acc
                  (let [x (shifted s (get sm "ports") (:offset policy))]
                    (if (r/err? x) (reduced x) (r/map-ok acc #(assoc % s (tagged "!override" (:ok x))))))))
              (r/ok (sorted-map))
              with-ports)
      (:publish policy)
      (let [pub (:publish policy)]
        (if-let [unknown (seq (remove (services model) (keys pub)))]
          (invalid {:services (vec (sort unknown)) :hint "ports publish names services the model does not define"})
          (r/ok (into (sorted-map)
                      (concat
                       (for [[s _] with-ports :when (not (contains? pub s))] [s reset-list])
                       (for [[s m] pub]
                         ;; m6: a key "53/udp" publishes a udp port
                         [s (tagged "!override" (vec (for [[c h] (sort-by (comp str key) m)
                                                           :let [[t proto] (str/split (str c) #"/" 2)]]
                                                       (port-entry (parse-long t) "127.0.0.1" (or proto "tcp")
                                                                   (when (pos? h) h)))))]))))))
      :else (invalid {:ports policy}))))

(defn- name-resets
  "Result<[service]> whose container_name is reset."
  [model names]
  (let [setting (set (named-containers model))]
    (cond
      (or (nil? names) (= :all names)) (r/ok (vec (sort setting)))
      (= :none names) (r/ok [])
      :else (if-let [unknown (seq (remove (services model) names))]
              (invalid {:services (vec (sort unknown)) :hint "names lists services the model does not define"})
              (r/ok (vec (sort (filter setting names))))))))

(defn- volume-renames
  "Result<{key {\"name\" new}}> of the fresh volumes asked for."
  [model fresh run]
  (let [vols (get model "volumes" {})
        project (get model "name")
        ks (if (= :all fresh)
             (sort (remove #(external-volume? model %) (keys vols)))
             (distinct fresh))]
    (cond
      (empty? ks) (r/ok {})
      (str/blank? (str project)) (invalid {:hint "the model names no compose project (\"name\")"})
      :else
      (if-let [unknown (seq (remove #(contains? vols %) ks))]
        (invalid {:volumes (vec (sort unknown)) :hint "fresh volumes names volumes the model does not declare"})
        (if-let [ext (seq (filter #(external-volume? model %) ks))]
          (r/err :compose/fresh-external-volume
                 {:volumes (vec (sort ext))
                  :hint "an external volume is never renamed nor recreated by hive-compose; drop it from the fresh volumes"})
          (r/ok (into (sorted-map) (map (fn [k] [k {"name" (fresh-name project k run)}])) ks)))))))

(defn- shared-volumes
  "Result<{key {\"external\" true \"name\" <base>_<key>}}>: the volumes `ks`
   a copy borrows from shared project `base` (build caches), never created
   nor removed by the copy. A key the model does not declare is refused."
  [model ks base]
  (cond
    (empty? ks) (r/ok {})
    (nil? base) (invalid {:hint "shared volumes need :override/images-from (the shared project)"})
    :else
    (if-let [unknown (seq (remove #(contains? (get model "volumes" {}) %) ks))]
      (invalid {:volumes (vec (sort unknown)) :hint "run shared volumes names volumes the model does not declare"})
      ;; an already external volume is shared as the file declares it
      ;; B3: compose refuses `external` beside driver/driver_opts/labels, so
      ;; those are reset; the name is the one the SHARED project uses: its
      ;; explicit `name:`, else the one compose derives, <base>_<key> (the
      ;; model is read under the copy's -p, where a derived name shows as
      ;; <copy>_<key>)
      (r/ok (into (sorted-map)
                  (for [k (distinct ks)
                        :let [v (get-in model ["volumes" k])
                              nm (get v "name")
                              derived? (or (nil? nm) (= nm (str (get model "name") "_" k)))]
                        :when (not (true? (get v "external")))]
                    [k {"external" true
                        "name" (if derived? (str base "_" k) nm)
                        "driver" reset-null "driver_opts" reset-null "labels" reset-null}]))))))

(defn- networks-entries
  "Result<{key {\"name\" new}}> (M5): every non-external top-level network
   of `model` with an explicit name gets the copy's own name `<project>_<key>`
   (a fixed name would join the copy to the shared project's network). A
   fixed subnet, a static address or an external network cannot be copied:
   invalid-override naming the network."
  [model]
  (let [project (get model "name")
        nets (get model "networks" {})
        static (for [[svc sm] (sort-by key (services model))
                     :let [ns (get sm "networks")]
                     :when (map? ns)
                     [n cfg] ns
                     :when (or (get cfg "ipv4_address") (get cfg "ipv6_address"))]
                 {:network n :service svc})]
    (cond
      (seq static)
      (invalid {:networks (vec (distinct (map :network static))) :static-addresses (vec static)
                :hint "a static ipv4_address/ipv6_address would clash with the shared project's; an isolated copy cannot run it"})
      (some (fn [[_ v]] (true? (get v "external"))) nets)
      (invalid {:networks (vec (sort (for [[k v] nets :when (true? (get v "external"))] k)))
                :hint "an external network would join the copy to the shared project's services"})
      (some (fn [[_ v]] (some #(get % "subnet") (get-in v ["ipam" "config"]))) nets)
      (invalid {:networks (vec (sort (for [[k v] nets :when (some #(get % "subnet") (get-in v ["ipam" "config"]))] k)))
                :hint "a fixed ipam subnet would overlap the shared project's network"})
      :else
      (r/ok (into (sorted-map)
                  (for [[k v] nets
                        :let [nm (get v "name")]
                        :when (and nm (not= nm (str project "_" k)))]
                    [k {"name" (str project "_" k)}]))))))

(defn override-doc
  "Result<doc>: the override document of OverrideSpec `spec` over compose
   model `model`, {} when it changes nothing. It names only services and
   volumes the model defines and never an external volume. Pure."
  [model spec]
  (if-let [problems (schema/explain schema/OverrideSpec spec)]
    (invalid {:problems problems})
    (r/let-ok [names (name-resets model (:override/names spec))
               ports (ports-entries model (get spec :override/ports :strip))
               vols (volume-renames model (:override/fresh-volumes spec) (:override/run spec))]
      (let [pinned (:override/images spec)
            ;; B2: every service :override/images names is pinned by id and
            ;; never built nor pulled; without a map, the build-only services
            ;; take the shared project's tag
            images (when-let [p (:override/images-from spec)]
                     (into (sorted-map)
                           (concat
                            (when-not (seq pinned)
                              (for [s (build-only model)]
                                [s {"build" reset-null "image" (pinned-image p s) "pull_policy" "never"}]))
                            (for [[s id] pinned :when (contains? (services model) s)]
                              [s (cond-> {"image" id "pull_policy" "never"}
                                   (some? (get-in model ["services" s "build"])) (assoc "build" reset-null))]))))
            svcs (merge-with merge
                             (into (sorted-map) (map (fn [s] [s {"container_name" reset-null}])) names)
                             (into (sorted-map) (map (fn [[s v]] [s {"ports" v}])) ports)
                             images)]
        (r/let-ok [shared (shared-volumes model (:override/shared-volumes spec) (:override/images-from spec))]
          (let [labels (:override/volume-labels spec)
                labelled (when (seq labels)
                           (into {} (for [[k v] (get model "volumes" {})
                                          :when (not (true? (get v "external")))
                                          :when (not (contains? shared k))]
                                      [k {"labels" (into (sorted-map) labels)}])))
                vols (merge-with merge labelled vols shared)]
            (r/let-ok [nets (if (:override/images-from spec) (networks-entries model) (r/ok {}))]
              (r/ok (cond-> {}
                      (seq svcs) (assoc "services" (into (sorted-map) svcs))
                      (seq vols) (assoc "volumes" (into (sorted-map) vols))
                      (seq nets) (assoc "networks" nets))))))))))

(defn describe
  "One line per service and volume override document `doc` changes, for
   `compose plan`, up answers and status."
  [doc]
  (vec (concat
        (for [[s m] (sort-by key (get doc "services"))]
          (str s ": "
               (str/join ", "
                         (concat (when (contains? m "container_name") ["container_name reset"])
                                 (when-let [p (get m "ports")]
                                   [(if (= reset-list p)
                                      "host ports stripped"
                                      (str "ports " (str/join " " (for [e (:value p)]
                                                                    (str (get e "host_ip" "0.0.0.0") ":"
                                                                         (get e "published" "*") "->"
                                                                         (get e "target") "/" (get e "protocol"))))))])
                                 (when-let [i (get m "image")] [(str "image " i " (no build, never pulled)")])))))
        (for [[k v] (sort-by key (get doc "volumes"))]
          (str "volume " k " -> " (get v "name"))))))

(defn spec-of
  "OverrideSpec of options as a call or a preset gives them (:run :names
   :ports :fresh-volumes :images-from), absent ones left out."
  [{:keys [run names ports fresh-volumes images-from images shared-volumes volume-labels]}]
  (cond-> {}
    (seq volume-labels) (assoc :override/volume-labels (into (sorted-map) volume-labels))
    (seq images) (assoc :override/images (into (sorted-map) images))
    (seq shared-volumes) (assoc :override/shared-volumes (vec shared-volumes))
    run (assoc :override/run run)
    (some? names) (assoc :override/names names)
    (some? ports) (assoc :override/ports ports)
    (or (= :all fresh-volumes) (seq fresh-volumes)) (assoc :override/fresh-volumes fresh-volumes)
    images-from (assoc :override/images-from images-from)))

;; ---------------------------------------------------------------------------
;; the emitter: exactly maps, lists, strings, integers, booleans, null and the
;; two tags; block style, keys sorted, every string double-quoted (a JSON
;; string literal is a valid YAML double-quoted scalar).

(defn- scalar [v]
  (cond (nil? v) "null"
        (string? v) (json/write-str v :escape-slash false)
        (keyword? v) (json/write-str (name v) :escape-slash false)
        (boolean? v) (str v)
        (integer? v) (str v)
        :else (throw (ex-info "override emitter: unsupported value" {:value v}))))

(defn- pad [n] (apply str (repeat n " ")))

(declare block)

(defn- after-key
  "What follows `key:` (or `-`) for value `v` nested at indent `ind`."
  [v ind]
  (let [[tag x] (if (tagged? v) [(:tag v) (:value v)] [nil v])
        lead (if tag (str " " tag) "")]
    (cond
      (and (coll? x) (seq x)) (str lead (block x (+ ind 2)))
      (and (map? x) (empty? x)) (str lead " {}")
      (and (coll? x) (empty? x)) (str lead " []")
      :else (str lead " " (scalar x)))))

(defn- block
  "Non-empty map or list `v` in block style at indent `ind`, each line led by
   a newline."
  [v ind]
  (if (map? v)
    (apply str (for [k (sort (map str (keys v)))]
                 (str "\n" (pad ind) (scalar k) ":" (after-key (get v k) ind))))
    (apply str (for [x v] (str "\n" (pad ind) "-" (after-key x ind))))))

(defn emit
  "Deterministic YAML text of override document `doc`; \"{}\\n\" when empty."
  [doc]
  (if (empty? doc)
    "{}\n"
    (str (subs (block doc 0) 1) "\n")))
