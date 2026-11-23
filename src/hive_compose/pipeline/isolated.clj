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
      ;; a run the reaper ended and marked for volume removal is revived by
      ;; `up run=<id>` within the grace (the mark is cancelled, the data kept)
      (and (:ended-at e) (not (:volumes-due-at e)))
      (r/err :compose/invalid-param {:param "run" :run rid :hint "the run has ended; start a new one"})
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
    ;; M8: compose's own default lookup reads the base file AND its override
    ;; partner (compose.override.yaml / docker-compose.override.yml)
    (if-let [f (files/first-existing (:profile/dir p) iso/default-files)]
      (r/ok (assoc p :profile/files (into [f] (keep #(files/first-existing (:profile/dir p) [%])
                                                    [(iso/override-partner f)]))))
      (r/err :compose/no-compose-file {:dir (:profile/dir p) :looked-for iso/default-files}))))

(defn- mb [b] (when b (quot b (* 1024 1024))))

(defn- project-free
  "Result<nil>: ok when docker knows NOTHING labelled with compose project
   `cp` (containers, volumes, networks), else could-not-run :project-exists
   naming what was found. A run must own its project before it starts; an
   engine that cannot list containers and networks by label is refused
   (ownership cannot be proved)."
  [ctx e cp rid]
  (if-not (satisfies? port/IStackEngine e)
    (could-not-run rid {:reason :project-unverifiable :project cp
                        :hint "the engine cannot list a project's containers and networks"})
    (let [lbl {iso/project-label cp}]
      (r/let-ok [cs (port/-running-labelled e lbl)
                 vs (port/-run-volumes e cp)
                 ns (port/-networks-labelled e lbl)]
        (if (and (empty? cs) (empty? vs) (empty? ns))
          (r/ok nil)
          (could-not-run rid {:reason :project-exists :project cp
                              :found {:containers (count cs) :volumes (count vs) :networks (count ns)}
                              :hint "a compose project of that name already exists and is not this run's; pick another run id"}))))))

(defn- ports-free
  "Result<nil> (M7): the host ports run `rid` plans (`planned` {svc {port
   host}}) must not be planned by another live run, nor have a listener on
   this host now (IProgramRunner/-listening?, when the context has a
   runner). Else invalid-param naming the clashes."
  [ctx e rid planned]
  (let [mine (into {} (for [[s m] planned [k h] m] [h [s k]]))
        theirs (into {} (for [[orid oe] (runs ctx)
                              :when (and (not= orid rid) (not (:ended-at oe)))
                              [s m] (:planned-ports oe) [k h] m]
                          [h {:run orid :service s :port k}]))
        rn (:runner ctx)
        clash (vec (sort-by :host
                            (for [[h [s k]] mine
                                  :let [o (get theirs h)
                                        busy? (and (nil? o) rn (satisfies? port/IProgramRunner rn) (port/-listening? rn h))]
                                  :when (or o busy?)]
                              (cond-> {:host h :service s :port k}
                                o (assoc :run (:run o))
                                busy? (assoc :listening? true)))))]
    (if (seq clash)
      (r/err :compose/invalid-param {:param "ports" :run rid :clashes clash
                                     :hint "another live run plans these host ports, or something listens on them: use another offset"})
      (r/ok nil))))

(defn- refuse-external
  "Result<nil>: an external volume a service of the run mounts is refused
   unless the preset names it in :profile/run-shared-volumes (M4)."
  [rid model shared]
  (let [ext (remove (set shared) (:external (iso/mounts model)))]
    (if (seq ext)
      (r/err :compose/fresh-external-volume
             {:run rid :volumes (vec ext)
              :hint "an isolated run would share this external volume read-write with the shared project; name it in :profile/run-shared-volumes to share it on purpose"})
      (r/ok nil))))

(defn- admission
  "{:budget :verdict :limits}: starting `services` (those of the run not yet
   running) of `model`, judged by the stack admission (hc-16a) against every
   live run's memory and the host's MemAvailable minus :compose/host-reserve.
   A service with no mem_limit counts at :compose/default-resources :mem."
  [ctx model cp services declared-mb]
  (let [settings (:settings ctx)
        ;; M2: a service with no memory figure (no mem_limit in its compose
        ;; file) counts at :compose/run-default-service-mem-mb (default 128)
        dflt (* 1024 1024 (or (:compose/run-default-service-mem-mb settings) 128))
        assumed (if declared-mb
                  []
                  (vec (sort (remove (set (keys (iso/demand model services nil resources/parse-mem))) services))))
        ;; (B) a section's declared budget for its whole closure wins
        budget (if declared-mb
                 (iso/budget cp {} (* 1024 1024 declared-mb))
                 (iso/budget cp (iso/demand model services dflt resources/parse-mem) 1))
        lims (resources/limits settings nil (stacks/mem-available ctx))
        ;; every OTHER live isolated run counts, beside the stack runs
        runs-mem (reduce + 0 (for [[_ e] (runs ctx)
                                   :when (and (not (:ended-at e)) (not= cp (:compose-project e)))]
                               (get-in e [:budget :budget/total-mem] 0)))]
    {:budget budget :limits lims :assumed assumed
     :verdict (resources/admit budget lims #{} (+ runs-mem (stacks/others-mem ctx budget)))}))

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
               ;; B1: a NEW run must own its compose project: nothing may
               ;; already carry the project label (another checkout, a dashed
               ;; base, a stack's private project, a hand-run -p)
               _ (if entry (r/ok nil) (project-free ctx e (iso/compose-project base rid) rid))
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
               _ (refuse-external rid model shared)
               policy (r/ok (or (:ports entry) (:ports opts) :strip))
               ;; G3: the digest is taken over the model read WITHOUT the run's
               ;; own variables (HIVE_RUN, HIVE_PORT_..): their values differ per
               ;; run by design and would make two runs of one checkout disagree
               model0 (r/ok model)
               ;; M3: the run env needs the planned ports, which need the model;
               ;; read again WITH the run env, so every interpolation (HIVE_RUN,
               ;; HIVE_PORT_..) the up will see is in the model the override,
               ;; the ports and the digest are computed from
               model (let [env (iso/run-env base rid policy (override/planned-ports model policy))
                           m (port/-model e (-> p (dissoc :profile/override)
                                                (assoc :profile/project (iso/compose-project base rid)
                                                       :profile/base-project base)
                                                (update :profile/env merge env)))]
                       (if (r/err? m) (could-not-run rid {:reason :config-invalid :detail (select-keys m [:error :stderr])}) m))
               planned (r/ok (override/planned-ports model policy))
               run-env (r/ok (iso/run-env base rid policy planned))
               ;; a run's own containers already listen on its ports when a
               ;; second section joins: only a NEW run is probed
               _ (if entry (r/ok nil) (ports-free ctx e rid planned))
               cp (r/ok (iso/compose-project base rid))
               path (r/ok (iso/override-path (state-file ctx) cp))
               ip0 (r/ok (-> (iso/isolate p rid path (:profile/files p))
                             (update :profile/env merge run-env)))
               ;; B1: pin only what the section's closure runs (plus what the
               ;; run recorded); B2: every image of it, by id
               closure (port/-closure e (dissoc ip0 :profile/override))
               tags (r/ok (merge (select-keys (iso/closure-images model base (keys (:images entry)))
                                              (keys (:images entry)))
                                 (iso/closure-images model base closure)))
               ids (port/-image-ids e (vals tags))
               pinned (let [x (iso/pin-images tags (:images entry) (into {} (for [[_ t] tags :when (get ids t)] [t (get ids t)])))]
                        (if (:error x) (could-not-run rid (:error x)) (r/ok (:ok x))))
               doc (override/override-doc model (iso/spec model base rid {:ports policy :images pinned
                                                                          :shared-volumes shared}))]
      (let [ip ip0]
        (r/let-ok [running (r/ok (if entry (let [x (port/-running e ip)] (if (r/ok? x) (:ok x) #{})) #{}))]
          (let [{:keys [budget verdict limits assumed]} (admission ctx model cp (remove (set running) closure)
                                                                          (:profile/run-mem-mb p))]
            (r/ok {:run rid :new? (nil? entry) :compose-project cp :profile ip :doc doc
                   :config-digest (config-digest {"config" model0 "override" doc} base rid)
                   :digest-model model0
                   :text (override/emit doc) :path path :images pinned :shared-volumes shared
                   :ports policy :planned-ports planned :env run-env :model model
                   :volumes (let [m (iso/mounts model)]
                              {:created (vec (sort (for [[k v] (get model "volumes" {})
                                                         :when (not (true? (get v "external")))
                                                         :when (not (some #{k} shared))]
                                                     k)))
                               :shared (vec shared)
                               :external (:external m)})
                   :binds (:binds (iso/mounts model))
                   :budget budget
                   :admission (cond-> {:verdict (if (r/ok? verdict) :ok :refused)
                                       :assumed-services (count assumed)
                                       :assumed-mb-each (or (get-in ctx [:settings :compose/run-default-service-mem-mb]) 128)
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

(declare end-run! grace-ms)

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
                                                :planned-ports (:planned-ports pl)
                                                :external (get-in pl [:volumes :external])
                                                :binds (:binds pl)
                                                :env (env/rendered (get-in pl [:profile :profile/env]))))))
      (record-run! ctx rid #(-> %
                                (dissoc :ended-at :ended-by :volumes-due-at :claim-gone?)
                                ;; m1: key-wise, so a merge of two instances keeps both
                                (update :images (fnil merge {}) (:images pl))
                                (update :budget (fn [b] (merge-with (fn [x y] (if (number? x) (+ x y) (merge x y)))
                                                                    b (select-keys (:budget pl) [:budget/total-mem :budget/instances]))))
                                (assoc-in [:digests (:profile/id p)]
                                          (config-digest {"config" (:digest-model pl) "override" (:doc pl)} base rid))))
      (let [res (ops/up-profile! ctx (:profile pl))]
        (if (r/err? res)
          (let [err (classify-failure rid res)]
            ;; M1: a NEW run that could not come up is ended at once: its
            ;; started containers removed, its volumes kept and marked as for
            ;; any ended run (the grace lets a human read them)
            (if (and (:new? pl) (empty? (iso/run-sections (:active @(:state ctx)) rid)))
              (let [row (end-run! ctx rid :could-not-run false)
                    due (+ ((:now ctx)) (grace-ms (:settings ctx)))]
                (when (:ended-at (get (runs ctx) rid))
                  (record-run! ctx rid #(assoc % :volumes-due-at due)))
                (assoc err :run-ended (select-keys row [:ended? :leftovers-error])))
              err))
          (let [facts (port/-run-facts e (:profile pl))
                _ (record-run! ctx rid #(assoc % :had-section? true))]
            (r/ok (cond-> (assoc (:ok res) :run rid :compose-project (:compose-project pl)
                                 :override (override/describe (:doc pl)) :env-run (:env pl)
                                 ;; (A) the pinned ids, the same map status answers
                                 :images (get-in (runs ctx) [rid :images] {}))
                    (r/ok? facts) (assoc :ports (:ports (:ok facts)))))))))))

(defn plan
  "Result: what `up!` of `p` in run `opts` would do, with no side effect: the
   override, ports, pinned images and the admission number (need, free,
   verdict, and on refusal which sections would have to stop)."
  [ctx p opts]
  (r/map-ok (plan-join ctx p opts)
            (fn [pl] (-> (select-keys pl [:run :new? :compose-project :path :images :shared-volumes
                                          :ports :planned-ports :env :admission :config-digest :volumes :binds])
                         (assoc :section (get-in pl [:profile :profile/id])
                                :files (get-in pl [:profile :profile/files])
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

(defn- project-containers
  "Result<[name]>: every container (running or not) of run `rid`'s project."
  [ctx e rid]
  (if (satisfies? port/IStackEngine e)
    (r/map-ok (port/-running-labelled e {iso/project-label (get-in (runs ctx) [rid :compose-project])})
              #(mapv :name %))
    (r/ok [])))

(defn- remove-volumes!
  "Result<{:removed :kept :not-removable}>: the volumes of ENDED run `rid`
   removed, by exact name, never forced (M3): refused while the run still
   has a section in the state or a container of its project exists. A
   volume labelled with the project but not provably the run's is listed
   under :not-removable, never removed."
  [ctx rid]
  (r/let-ok [e (engine ctx)
             cs (project-containers ctx e rid)]
    (let [secs (iso/run-sections (:active @(:state ctx)) rid)]
      (cond
        (seq secs) (r/err :compose/run-still-active {:run rid :sections secs
                                                     :hint "take the run's sections down first"})
        (seq cs) (r/err :compose/run-still-active {:run rid :containers cs
                                                   :hint "containers of the run's project still exist"})
        :else
        (r/let-ok [all (port/-run-volumes e (get-in (runs ctx) [rid :compose-project]))]
          (let [vols (iso/removable (runs ctx) rid all)
                spared (vec (sort (remove (set (map :name vols)) (map :name all))))]
            (if (empty? vols)
              (r/ok {:removed [] :kept [] :not-removable spared})
              (r/map-ok (port/-remove-volumes! e (mapv :name vols))
                        #(assoc % :not-removable spared)))))))))

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
        ;; m9: a failed `down` does not end the run: a later down retries it
        _ (when (r/ok? leftovers) (record-run! ctx rid #(assoc % :ended-at ((:now ctx)) :ended-by how)))
        ;; m1: a failed `down` may leave containers: never remove then
        volumes? (and volumes? (r/ok? leftovers))
        vols (when volumes? (remove-volumes! ctx rid))]
    (cond-> {:run rid :ended? (r/ok? leftovers)}
      (r/err? leftovers) (assoc :leftovers-error (select-keys leftovers [:error :stderr])
                                :volumes-kept-why :leftovers-error)
      (and vols (r/ok? vols)) (assoc :volumes (:ok vols))
      (and vols (r/err? vols)) (assoc :volumes-error (select-keys vols [:error :hint]))
      (not volumes?) (assoc :volumes-kept (let [x (removable-volumes ctx rid)] (if (r/ok? x) (mapv :name (:ok x)) []))))))

(defn- other-claims
  "[{:section :agents}] of the sections of run `rid` with a live claim by an
   agent other than `agent` (a claim is live while the state holds it: the
   usage pass drops the ended ones)."
  [ctx rid agent]
  (vec (for [id (iso/run-sections (:active @(:state ctx)) rid)
             :let [cs (remove #(= agent %) (sort (keys (get-in @(:state ctx) [:active id :claims]))))]
             :when (seq cs)]
         {:section id :agents (vec cs)})))

(defn- ended-volumes
  "Result: run `rid` is ended; remove its volumes when `volumes?`, else list them."
  [ctx rid volumes?]
  (if volumes?
    (r/map-ok (remove-volumes! ctx rid) (fn [v] {:run rid :ended? true :volumes v}))
    (r/ok {:run rid :ended? true
           :volumes-kept (let [x (removable-volumes ctx rid)] (if (r/ok? x) (mapv :name (:ok x)) []))})))

(defn down-run!
  "Result<{:run :sections [..] :volumes ..}>: every section of run `rid`
   taken down (containers removed), the run ended; its volumes removed only
   with `volumes?` (default kept: a failed run's data is evidence). With
   `volumes?` it is refused while an agent other than `:agent` holds a live
   claim on a section of the run (M2), unless `:allow-shared?`. An ended run
   with `volumes?` removes them now (cancelling any pending reaper mark)."
  ([ctx rid volumes?] (down-run! ctx rid volumes? {}))
  ([ctx rid volumes? {:keys [agent allow-shared?]}]
   (let [held (other-claims ctx rid agent)]
     (cond
       (not (get (runs ctx) rid))
       (r/err :compose/unknown-run {:run rid :known (vec (sort (keys (runs ctx))))})

       (and volumes? (seq held) (not allow-shared?))
       (r/err :compose/held-by-another-section
              {:run rid :holders held
               :message (str "agents hold live claims on sections of run " rid ": "
                             (str/join "; " (for [{:keys [section agents]} held]
                                              (str section " (" (str/join ", " agents) ")"))))
               :hint (str "ask the claiming agent first, or pass allow_shared=true to remove the run's volumes anyway"
                          (when (nil? agent) "; this call names no agent (agent/agent_id), so every claim counts as another's"))})

       (get-in (runs ctx) [rid :ended-at])
       (do (record-run! ctx rid #(dissoc % :volumes-due-at))
           (ended-volumes ctx rid volumes?))

       :else
       (let [ids (iso/run-sections (:active @(:state ctx)) rid)
             downs (mapv (fn [id] (let [x (r/bind (ops/lookup ctx id) #(ops/down-profile! ctx % :down))]
                                    (cond-> {:section id :ok? (r/ok? x)} (r/err? x) (assoc :error x))))
                         ids)]
         (if (some (complement :ok?) downs)
           (r/err :compose/stack-down-incomplete {:run rid :sections downs
                                                  :hint "the run stays recorded; down run=<id> again once fixed"})
           (r/ok (merge {:sections downs} (end-run! ctx rid :down volumes?)))))))))

(defn grace-ms
  "How long the reaper waits between ending a run and removing its volumes:
   :compose/run-volumes-grace-minutes (default 60)."
  [settings]
  (* 60000 (or (:compose/run-volumes-grace-minutes settings) 60)))

(defn doubtful-why
  "Why usage pass `observed` cannot be trusted to judge claims, nil when it
   can: :clock-jump when the reaper's gap since its previous pass exceeded
   the bound (an NTP step, a suspend and resume: every claim looks expired),
   :doubtful-roster when the swarm is absent or lists no agent. A doubtful
   pass marks nothing and removes nothing. nil `observed` (no usage pass, a
   direct call) is trusted."
  [observed]
  (cond
    (nil? observed) nil
    (:clock-jump? observed) :clock-jump
    (or (not= :ok (:swarm observed)) (zero? (:agents observed 0))) :doubtful-roster
    :else nil))

(defn clock-jump?
  "True when the reaper's pass at `now` follows its previous one (`prev-at`,
   nil on the first pass) by more than the bound:
   :compose/reaper-max-gap-minutes, else max(3 x the tick, 10 minutes)."
  [settings prev-at now]
  (let [bound (if-let [m (:compose/reaper-max-gap-minutes settings)]
                (* 60000 m)
                (max (* 3 1000 (or (:compose/tick-seconds settings) 60)) (* 10 60000)))]
    (boolean (and prev-at (> (- now prev-at) bound)))))

(defn after-reap!
  "After a section reaper pass (`reaped` rows, `doubtful?` true when the
   pass judged claims on an absent or empty swarm roster). Step 1 (M1): a
   run whose last section this pass reaped is ended, its containers gone,
   and its volumes only MARKED for removal at now + grace; never marked when
   the reap is doubtful or a claim of the run ended as :gone (the volumes are
   then kept and listed). Step 2, on a LATER pass: `sweep-due!` removes them.
   Answers one row per run ended."
  ([ctx reaped] (after-reap! ctx reaped nil))
  ([ctx reaped observed]
   (if-not (satisfies? port/IIsolatedRuns (:engine ctx))
     []
     (let [rids (iso/ended-runs (runs ctx) (:active @(:state ctx))
                                (keep #(some-> (:profile/id %) iso/split-id second)
                                      (filter :ok? reaped)))
           ;; claims judged against an absent or empty roster, or across a
           ;; clock jump, prove nothing (R2-M2)
           doubt (doubtful-why observed)
           gone (into #{} (keep #(when (= :gone (:reason %)) (some-> (:section %) iso/split-id second)))
                      (:expired observed))
           gone? (fn [rid] (or (contains? gone rid) (true? (get-in (runs ctx) [rid :claim-gone?]))))]
       (mapv (fn [rid]
               (let [row (end-run! ctx rid :reaper false)]
                 (if (or doubt (gone? rid) (:leftovers-error row))
                   (assoc row :volumes-kept-why (cond doubt doubt
                                                      (gone? rid) :claim-gone
                                                      :else :leftovers-error))
                   (let [due (+ ((:now ctx)) (grace-ms (:settings ctx)))]
                     (record-run! ctx rid #(assoc % :volumes-due-at due))
                     (assoc row :volumes-due-at due)))))
             rids)))))

(defn note-observed!
  "Remember on each live isolated run that one of its claims ended as :gone
   in usage pass `observed` (an agent the roster no longer lists): such a
   run's volumes are never marked for removal by the reaper (M1)."
  [ctx observed]
  (doseq [rid (distinct (keep #(when (= :gone (:reason %)) (some-> (:section %) iso/split-id second))
                              (:expired observed)))
          :when (get (runs ctx) rid)]
    (record-run! ctx rid #(assoc % :claim-gone? true))))

(defn cancel-run-due!
  "Cancel a pending volume removal of run `rid`; true when one was pending.
   Reached by run id, so it works when the run has no section left (R2-M1:
   a marked run's sections are all gone)."
  [ctx rid]
  (if (get-in (runs ctx) [rid :volumes-due-at])
    (do (record-run! ctx rid #(dissoc % :volumes-due-at)) true)
    false))

(defn cancel-due!
  "Cancel a pending volume removal of the run of isolated section `sid`
   (`up run=<id>`, a claim or a touch on one of its sections)."
  [ctx sid]
  (when-let [[_ rid] (iso/split-id sid)]
    (cancel-run-due! ctx rid)))

(defn end-if-empty!
  "N1: after an explicit down or stop of isolated section `sid`, end its run
   when no section is left, with its volumes KEPT and UNMARKED: only `down
   run=<id> volumes=true` removes them. Answers the end row, or nil."
  [ctx sid]
  (when-let [[_ rid] (iso/split-id sid)]
    (when (and (get (runs ctx) rid) (not (get-in (runs ctx) [rid :ended-at]))
               (empty? (iso/run-sections (:active @(:state ctx)) rid)))
      (let [row (end-run! ctx rid :down false)]
        (record-run! ctx rid #(dissoc % :volumes-due-at))
        row))))

(defn end-orphans!
  "M1: end every isolated run never ended that holds no section, once it
   started more than the default TTL ago (an up that failed half way, a
   hive-mcp that died during it). Its containers go. Its volumes are marked
   for removal only when the run NEVER had a section (nothing anyone could
   have claimed); a run that had one (R2-B1: a section lost some other way)
   is ended with its volumes KEPT and UNMARKED. Nothing at all on a doubtful
   pass (`observed`: an absent or empty roster, a clock jump). Answers a row
   per run."
  ([ctx] (end-orphans! ctx nil))
  ([ctx observed]
   (if (or (not (satisfies? port/IIsolatedRuns (:engine ctx))) (and observed (doubtful-why observed)))
     []
     (let [now ((:now ctx))
           ttl (* 60000 (or (get-in ctx [:settings :compose/default-ttl-minutes]) 60))]
       (vec (for [rid (iso/orphaned-runs (runs ctx) (:active @(:state ctx)) now ttl)]
              (let [had? (get-in (runs ctx) [rid :had-section?])
                    row (end-run! ctx rid :orphaned false)]
                (when (and (not had?) (:ended-at (get (runs ctx) rid)))
                  (record-run! ctx rid #(assoc % :volumes-due-at (+ now (grace-ms (:settings ctx))))))
                (cond-> (assoc row :reason :no-section)
                  had? (assoc :volumes-kept-why :had-section)))))))))

(defn sweep-due!
  "Step 2 of the reaper's volume removal (M1): remove the volumes of every
   run whose mark has come due, only while it is still ended, no section of
   it is active, it took no claim since, and no container of its project
   exists (`remove-volumes!` checks the last two). Nothing on a doubtful pass
   (`observed`, see `doubtful-why`: a clock jump, an absent or empty roster).
   Answers a row per run."
  ([ctx] (sweep-due! ctx nil))
  ([ctx observed]
   (if (or (not (satisfies? port/IIsolatedRuns (:engine ctx))) (and observed (doubtful-why observed)))
     []
     (let [now ((:now ctx))]
       (vec (for [[rid e] (sort-by key (runs ctx))
                  :let [due (:volumes-due-at e)]
                  :when (and due (<= due now) (:ended-at e))]
              (let [res (remove-volumes! ctx rid)]
                (when (r/ok? res) (record-run! ctx rid #(dissoc % :volumes-due-at)))
                (cond-> {:run rid}
                  (r/ok? res) (assoc :volumes (:ok res))
                  (r/err? res) (assoc :error (select-keys res [:error :sections :containers]))))))))))

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
                       :volumes {:created (or vols []) :shared shared :external (vec (:external entry))}
                       :binds (vec (:binds entry))))
      (:volumes-due-at entry) (assoc :volumes-due-at (:volumes-due-at entry))
      (:conflict entry) (assoc :conflict (:conflict entry))
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
                               (cond-> {:run (:run row) :name n}
                                 (:volumes-due-at row) (assoc :due-at (:volumes-due-at row)))))}))
