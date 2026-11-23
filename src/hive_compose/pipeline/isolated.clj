(ns hive-compose.pipeline.isolated
  "Isolated runs of sections (FUN-D) over the ops context; the engine must
   satisfy `hive-compose.port/IIsolatedRuns`. See `hive-compose.promote.isolated`.

     up!       a section joins run `run` (or a new one): the merged model is
               read, the build-only images pinned by id, the override written,
               the run admitted against host memory (hc-16a admission), then
               the section goes through `pipeline.ops/up-profile!`, the one
               up entry point, as `<id>@<run>` under `<project>-<run>`.
     down-run! every section of a run down, then its compose project (its
               networks); with `volumes?` its volumes too.
     reap!     after the section reaper: a run whose last section the reaper
               just took down is ended and its volumes removed.
     status    runs, ports, images, config digest, volumes.

   Volume removal is the single exception to the rule that hive-compose never
   removes a volume (Pedro, 2026-11-23): `remove-volumes!` here is its only
   caller path, and it removes only what `isolated/removable` keeps."
  (:require [clojure.string :as str]
            [hive-compose.boundary.files :as files]
            [hive-compose.pipeline.ops :as ops]
            [hive-compose.pipeline.stacks :as stacks]
            [hive-compose.port :as port]
            [hive-compose.promote.env :as env]
            [hive-compose.promote.isolated :as iso]
            [hive-compose.promote.override :as override]
            [hive-compose.promote.profile :as profile]
            [hive-compose.promote.resources :as resources]
            [hive-compose.schema :as schema]
            [hive-dsl.result :as r])
  (:import (java.security MessageDigest SecureRandom)))

;; SPDX-License-Identifier: MIT

(defn- persist! [{:keys [state save!]}]
  (when save! (save! (select-keys @state [:active :current :stacks :preexisting :runs]))))

(defn- runs [ctx] (:runs @(:state ctx) {}))

(defn- engine [ctx]
  (let [e (:engine ctx)]
    (if (satisfies? port/IIsolatedRuns e)
      (r/ok e)
      (r/err :compose/isolated-unsupported {:hint "the engine cannot run isolated sections (no IIsolatedRuns)"}))))

(defn- could-not-run [run data]
  (r/err :compose/could-not-run (merge {:run run} data)))

(defn- random-hex []
  (let [b (byte-array 3)]
    (.nextBytes (SecureRandom.) b)
    (apply str (map #(format "%02x" (bit-and % 0xff)) b))))

(defn- sha256 [s]
  (let [d (.digest (MessageDigest/getInstance "SHA-256") (.getBytes (str s) "UTF-8"))]
    (str "sha256:" (apply str (map #(format "%02x" (bit-and % 0xff)) d)))))

(defn config-digest
  "sha256 of the run-independent resolved config (`iso/digest-config`)."
  [model base run]
  (sha256 (pr-str (iso/digest-config model base run))))

(defn- state-file [ctx]
  (or (get-in ctx [:settings :compose/state-file]) (str (System/getProperty "java.io.tmpdir") "/hive-compose/state.edn")))

(defn- record-run! [ctx run f]
  (swap! (:state ctx) update-in [:runs run] f)
  (persist! ctx))

;; ---------------------------------------------------------------------------
;; planning a section's join to a run (no side effect)

(defn- pick-run
  "Result<{:run :entry}>: the run `run` names (an existing one must belong to
   `base`, not be ended, and keep its port policy), else a new id."
  [ctx base {:keys [run ports]}]
  (let [rid (or run (iso/run-id base ((or (:rand-hex ctx) random-hex))))
        e (get (runs ctx) rid)]
    (cond
      (not (iso/valid-run? rid)) (r/err :compose/invalid-run {:run rid :problems (schema/explain schema/RunId rid)})
      (nil? e) (r/ok {:run rid :entry nil})
      (not= base (:base-project e)) (r/err :compose/invalid-param {:param "run" :run rid
                                                                  :hint (str "run " rid " is a run of " (:base-project e))})
      (:ended-at e) (r/err :compose/invalid-param {:param "run" :run rid :hint "the run has ended; start a new one"})
      (and ports (not= ports (:ports e))) (r/err :compose/invalid-param {:param "ports" :run rid :was (:ports e)
                                                                         :hint "a run keeps the port policy of its first up"})
      :else (r/ok {:run rid :entry e}))))

(defn- shared-volumes
  "Result<[key]>: the cache volumes the run borrows: the preset's
   :profile/run-shared-volumes, narrowed by the call's list (never widened)."
  [p asked]
  (let [allowed (vec (:profile/run-shared-volumes p))]
    (cond
      (nil? asked) (r/ok allowed)
      (seq (remove (set allowed) asked))
      (r/err :compose/invalid-param {:param "shared_volumes" :not-in-preset (vec (sort (remove (set allowed) asked)))
                                     :hint "a call may only narrow :profile/run-shared-volumes of the preset"})
      :else (r/ok (vec (distinct asked))))))

(defn- with-files
  "Result<p> naming its compose files: an extra -f (the override) replaces
   compose's default lookup, so the default file is named explicitly."
  [p]
  (if (seq (:profile/files p))
    (r/ok p)
    (if-let [f (files/first-existing (:profile/dir p) iso/default-files)]
      (r/ok (assoc p :profile/files [f]))
      (r/err :compose/no-compose-file {:dir (:profile/dir p) :looked-for iso/default-files}))))

(defn- mb [b] (when b (quot b (* 1024 1024))))

(defn- admission
  "{:budget :verdict :limits}: starting `services` (those of the run not yet
   running) of `model`, judged by the stack admission (hc-16a) against every
   live run's memory and the host's MemAvailable minus :compose/host-reserve.
   A service with no mem_limit counts at :compose/default-resources :mem."
  [ctx model cp services]
  (let [settings (:settings ctx)
        d (:compose/default-resources settings)
        dflt (when (map? d) (resources/parse-mem (:mem d)))
        budget (iso/budget cp (iso/demand model services dflt resources/parse-mem) 1)
        lims (resources/limits settings nil (stacks/mem-available ctx))]
    {:budget budget :limits lims
     :verdict (resources/admit budget lims #{} (stacks/others-mem ctx budget))}))

(defn- would-stop
  "Ids of the live sections and stack runs whose memory a refused run would
   need back, idlest first (a hint; nothing is stopped)."
  [ctx]
  (let [{:keys [active stacks]} @(:state ctx)]
    (vec (concat (map key (sort-by (comp :last-touch val) active))
                 (map #(str "stack:" (key %)) (sort-by (comp :last-touch val) stacks))))))

(defn plan-join
  "Result<plan>: everything section `p` (a preset or section of the shared
   project, call options applied) needs to join an isolated run, computed
   with reads only: run id, isolated profile, override document and text,
   pinned images, planned ports, run env and the admission verdict."
  [ctx p opts]
  (let [base (profile/project p)]
    (r/let-ok [e (engine ctx)
               _ (if-let [bad (seq (iso/reserved-collisions (:profile/env p)))]
                   (r/err :compose/invalid-param {:param "env" :reserved (vec bad)
                                                  :hint "an isolated run sets these itself; rename them"})
                   (r/ok nil))
               {rid :run entry :entry} (pick-run ctx base opts)
               p (with-files p)
               ;; per run, like its port policy: fixed at its first up
               shared (if entry (r/ok (vec (:shared-volumes entry))) (shared-volumes p (:shared-volumes opts)))
               version (port/-version e)
               _ (if (override/tags-supported? version)
                   (r/ok nil)
                   (could-not-run rid {:reason :compose-too-old :version version
                                       :hint "docker compose >= 2.24 reads !reset/!override"}))
               ;; read under the RUN's project name: a volume compose names itself
               ;; then shows as <base>-<run>_<key>, so any other name is explicit
               ;; (and would be shared with the base project) and gets renamed
               model (let [m (port/-model e (-> p (dissoc :profile/override)
                                                 (assoc :profile/project (iso/compose-project base rid)
                                                        :profile/base-project base)))]
                       (if (r/err? m) (could-not-run rid {:reason :config-invalid :detail (select-keys m [:error :stderr])}) m))
               _ (if (seq (remove #(contains? (get model "volumes" {}) %) shared))
                   (r/err :compose/invalid-param {:param "shared_volumes"
                                                  :undeclared (vec (remove #(contains? (get model "volumes" {}) %) shared))
                                                  :hint "a run shared volume must be one the compose file declares"})
                   (r/ok nil))
               tags (r/ok (iso/build-images model base))
               ids (port/-image-ids e (vals tags))
               pinned (let [x (iso/pin-images tags (:images entry) (into {} (for [[_ t] tags :when (get ids t)] [t (get ids t)])))]
                        (if (:error x) (could-not-run rid (:error x)) (r/ok (:ok x))))
               policy (r/ok (or (:ports entry) (:ports opts) :strip))
               doc (override/override-doc model (iso/spec model base rid {:ports policy :images pinned
                                                                          :shared-volumes shared}))]
      (let [cp (iso/compose-project base rid)
            path (iso/override-path (state-file ctx) cp)
            planned (override/planned-ports model policy)
            run-env (iso/run-env base rid policy planned)
            ip (-> (iso/isolate p rid path (:profile/files p))
                   (update :profile/env merge run-env))]
        (r/let-ok [closure (port/-closure e (dissoc ip :profile/override))
                   running (r/ok (if entry (let [x (port/-running e ip)] (if (r/ok? x) (:ok x) #{})) #{}))]
          (let [{:keys [budget verdict limits]} (admission ctx model cp (remove (set running) closure))]
            (r/ok {:run rid :new? (nil? entry) :compose-project cp :profile ip :doc doc
                   :config-digest (config-digest {"config" model "override" doc} base rid)
                   :text (override/emit doc) :path path :images pinned :shared-volumes shared
                   :ports policy :planned-ports planned :env run-env :model model
                   :admission (cond-> {:verdict (if (r/ok? verdict) :ok :refused)
                                       :need-mb (mb (:budget/total-mem budget))
                                       :free-mb (mb (:host limits))
                                       :available-mb (mb (:available limits))
                                       :reserve-mb (mb (:reserve limits))}
                                (r/err? verdict) (assoc :refusal (dissoc verdict :ok)
                                                        :would-have-to-stop (would-stop ctx)))
                   :verdict verdict})))))))

;; ---------------------------------------------------------------------------
;; up

(defn- classify-failure
  "The could-not-run error of a failed up of run `rid`."
  [rid res]
  (could-not-run rid (merge (iso/classify (:stderr res))
                            {:detail (select-keys res [:error :exit :stderr])})))

(defn up!
  "Result: section `p` (call options applied) up inside isolated run
   `opts` {:run id? :ports policy? :shared-volumes [..]?}, through
   `ops/up-profile!`. A new run is recorded only once it is admitted; the
   override is written before the up. A failed up is :compose/could-not-run
   with the failing service and the reason."
  [ctx p opts]
  (r/let-ok [pl (plan-join ctx p opts)
             _ (:verdict pl)
             e (engine ctx)
             _ (files/write-text! (:path pl) (:text pl))]
    (let [rid (:run pl)
          base (profile/project p)]
      (when (:new? pl)
        (record-run! ctx rid (constantly (assoc (iso/run-entry base rid (:ports pl) ((:now ctx)))
                                                :shared-volumes (:shared-volumes pl)
                                                :dir (get-in pl [:profile :profile/dir])
                                                :files (get-in pl [:profile :profile/files])
                                                :override (:path pl)
                                                :env (env/rendered (get-in pl [:profile :profile/env]))))))
      (record-run! ctx rid #(-> %
                                (assoc :images (merge (:images %) (:images pl)))
                                (assoc-in [:digests (:profile/id p)]
                                          (config-digest {"config" (:model pl) "override" (:doc pl)} base rid))))
      (let [res (ops/up-profile! ctx (:profile pl))]
        (if (r/err? res)
          (classify-failure rid res)
          (let [facts (port/-run-facts e (:profile pl))]
            (r/ok (cond-> (assoc (:ok res) :run rid :compose-project (:compose-project pl)
                                 :override (override/describe (:doc pl)) :env-run (:env pl))
                    (r/ok? facts) (assoc :ports (:ports (:ok facts)))))))))))

(defn plan
  "Result: what `up!` of `p` in run `opts` would do, with no side effect: the
   override, ports, pinned images and the admission number (need, free,
   verdict, and on refusal which sections would have to stop)."
  [ctx p opts]
  (r/map-ok (plan-join ctx p opts)
            (fn [pl] (-> (select-keys pl [:run :new? :compose-project :path :images :shared-volumes
                                          :ports :planned-ports :env :admission :config-digest])
                         (assoc :section (get-in pl [:profile :profile/id])
                                :override (override/describe (:doc pl))
                                :override-text (:text pl))))))

;; ---------------------------------------------------------------------------
;; volumes: the single exception (see the ns doc)

(defn removable-volumes
  "Result<[{:name :labels}]>: the volumes `remove-volumes!` may take for run
   `rid`, read from docker now (`iso/removable` over its labelled volumes)."
  [ctx rid]
  (r/let-ok [e (engine ctx)]
    (let [entry (get (runs ctx) rid)]
      (if-not entry
        (r/ok [])
        (r/map-ok (port/-run-volumes e (:compose-project entry))
                  #(iso/removable (runs ctx) rid %))))))

(defn- remove-volumes!
  "Result<{:removed :kept}>: the volumes of ended run `rid` removed, by exact
   name, never forced. Only for a run whose containers are gone."
  [ctx rid]
  (r/let-ok [e (engine ctx)
             vols (removable-volumes ctx rid)]
    (if (empty? vols)
      (r/ok {:removed [] :kept []})
      (port/-remove-volumes! e (mapv :name vols)))))

;; ---------------------------------------------------------------------------
;; down and reap

(defn- end-run!
  "Mark run `rid` ended (by `how`), its project's leftovers (networks,
   orphans) removed with `compose down` (never -v); with `volumes?` its
   volumes removed. Answers the report."
  [ctx rid how volumes?]
  (let [entry (get (runs ctx) rid)
        e (:engine ctx)
        ;; the run's own networks and orphans: `compose -p <project>-<run> down
        ;; --remove-orphans` (never -v), from the files the run ran with
        leftovers (if (and (satisfies? port/IStackEngine e) (:dir entry))
                    (port/-down-project! e {:project (:compose-project entry) :dir (:dir entry)
                                            :files (:files entry) :env (:env entry {}) :all? true})
                    (r/ok nil))
        _ (record-run! ctx rid #(assoc % :ended-at ((:now ctx)) :ended-by how))
        vols (when volumes? (remove-volumes! ctx rid))]
    (cond-> {:run rid :ended? true}
      (r/err? leftovers) (assoc :leftovers-error (select-keys leftovers [:error :stderr]))
      (and vols (r/ok? vols)) (assoc :volumes (:ok vols))
      (and vols (r/err? vols)) (assoc :volumes-error (select-keys vols [:error :hint]))
      (not volumes?) (assoc :volumes-kept (let [x (removable-volumes ctx rid)] (if (r/ok? x) (mapv :name (:ok x)) []))))))

(defn down-run!
  "Result<{:run :sections [..] :volumes ..}>: every section of run `rid`
   taken down (containers removed, `action` :down), the run ended; its
   volumes removed only with `volumes?` (default kept: a failed run's data is
   evidence)."
  [ctx rid volumes?]
  (cond
    (not (get (runs ctx) rid))
    (r/err :compose/unknown-run {:run rid :known (vec (sort (keys (runs ctx))))})
    ;; an ended run keeps its volumes until asked: down run=<id> volumes=true
    (get-in (runs ctx) [rid :ended-at])
    (if volumes?
      (r/map-ok (remove-volumes! ctx rid) (fn [v] {:run rid :ended? true :volumes v}))
      (r/ok {:run rid :ended? true :volumes-kept (let [x (removable-volumes ctx rid)] (if (r/ok? x) (mapv :name (:ok x)) []))}))
    :else
    (let [ids (iso/run-sections (:active @(:state ctx)) rid)
          downs (mapv (fn [id] (let [x (r/bind (ops/lookup ctx id) #(ops/down-profile! ctx % :down))]
                                 (cond-> {:section id :ok? (r/ok? x)} (r/err? x) (assoc :error x))))
                      ids)]
      (if (some (complement :ok?) downs)
        (r/err :compose/stack-down-incomplete {:run rid :sections downs
                                               :hint "the run stays recorded; down run=<id> again once fixed"})
        (r/ok (merge {:sections downs} (end-run! ctx rid :down volumes?)))))))

(defn after-reap!
  "After a section reaper pass (`reaped` rows): every isolated run left with
   no section is ended and its volumes removed (Pedro: auto-remove when the
   run is reaped). A run still holding a section (a live claim keeps it) is
   never touched. Answers one row per ended run."
  [ctx reaped]
  (if-not (satisfies? port/IIsolatedRuns (:engine ctx))
    []
    (let [rids (iso/ended-runs (runs ctx) (:active @(:state ctx))
                               (keep #(some-> (:profile/id %) iso/split-id second)
                                     (filter :ok? reaped)))]
      (mapv #(end-run! ctx % :reaper true) rids))))

;; ---------------------------------------------------------------------------
;; status

(defn- run-row
  [ctx e rid entry]
  (let [live? (not (:ended-at entry))
        facts (when (and e live?)
                (let [x (port/-run-facts e {:profile/id rid :profile/project (:compose-project entry)
                                            :profile/dir (or (:dir entry) ".")})]
                  (when (r/ok? x) (:ok x))))
        vols (when e (let [x (removable-volumes ctx rid)] (when (r/ok? x) (mapv :name (:ok x)))))
        shared (vec (for [k (:shared-volumes entry)] (str (:base-project entry) "_" k)))]
    (cond-> (-> (select-keys entry [:run :base-project :compose-project :ports :started-at :ended-at :ended-by
                                    :dir :files])
                (assoc :sections (iso/run-sections (:active @(:state ctx)) rid)
                       :pinned-images (:images entry {})
                       :config-digest (when (seq (:digests entry)) (sha256 (pr-str (into (sorted-map) (:digests entry)))))
                       :config-digests (into (sorted-map) (:digests entry))
                       :volumes {:created (or vols []) :shared shared}))
      facts (assoc :ports (:ports facts) :images (:images facts))
      (:env entry) (assoc :env-run (into (sorted-map) (filter (fn [[k _]] (re-matches iso/reserved-var-re k))) (:env entry))))))

(defn status
  "{:runs [row] :stale-run-volumes [{:run :name}]}: every isolated run the
   state records, live and ended. A live run's row: sections, :ports
   {service {container-port host-port}} and :images {service image-id} read
   from its running containers, :pinned-images, :config-digest (sha256 of
   the run-independent resolved config: run id, project name and published
   host ports normalised away, so two runs of one checkout and env agree),
   :volumes {:created [..] :shared [..]}. Ended runs' volumes still present
   are listed under :stale-run-volumes."
  [ctx]
  (let [e (when (satisfies? port/IIsolatedRuns (:engine ctx)) (:engine ctx))
        rows (vec (for [[rid entry] (sort-by key (runs ctx))] (run-row ctx e rid entry)))]
    {:runs rows
     :stale-run-volumes (vec (for [row rows :when (:ended-at row) n (get-in row [:volumes :created])]
                               {:run (:run row) :name n}))}))
