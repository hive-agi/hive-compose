(ns hive-compose.pipeline.build
  "`compose build` (card 20261123112858-6f2c31bc): build a section's images
   with the section's EFFECTIVE compose inputs (the caller resolves the
   subject through `ops/effective-subject`: dir, files, env, remembered call
   env, the holder of a target) and under its compose project name, so the
   tags are the ones the project's containers (and a run pinning images by
   id) expect, instead of `<checkout-dir>-<service>`.

   A build creates, recreates and stops no container: no ownership guard.
   What it does say is which active sections keep running the old image
   (:running-old-image) until someone recreates them.

   One build per compose project at a time: a second call while one runs is
   refused with :compose/build-busy naming what runs and since when (a build
   can take many minutes; the caller retries when it ends). The engine call
   runs under :compose/build-timeout-ms (default 30 minutes)."
  (:require [clojure.string :as str]
            [hive-compose.port :as port]
            [hive-compose.promote.build :as build]
            [hive-compose.promote.profile :as profile]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def output-tail-lines
  "How many lines of a failed build's output its answer carries."
  30)

(defonce ^:private fallback-builds
  ;; a ctx built without :builds (a hand-made test ctx) still serialises
  (atom {}))

(defn- builds [ctx] (or (:builds ctx) fallback-builds))

(defn- acquire!
  "Take compose project `project`'s build slot for `info`. True when
   acquired, false when another build holds it."
  [ctx project info]
  (let [[old _] (swap-vals! (builds ctx) (fn [m] (if (contains? m project) m (assoc m project info))))]
    (not (contains? old project))))

(defn- release! [ctx project]
  (swap! (builds ctx) dissoc project))

(defn running
  "{compose-project {:section :services :since}} of the builds running now."
  [ctx]
  @(builds ctx))

(defn- now [ctx] ((or (:now ctx) #(System/currentTimeMillis))))

(defn- built-rows
  "[{:service :image :id}] of `selected` (BuildFacts `facts`), ids from the
   engine's answer `ids` ({ref id}); an id the engine did not answer is nil."
  [facts selected ids]
  (vec (for [s selected
             :let [img (get-in facts [s :image])]]
         {:service s :image img :id (get ids img)})))

(defn- run-build!
  [ctx p selected facts opts]
  (let [e (:engine ctx)
        project (profile/project p)
        t0 (now ctx)]
    (r/let-ok [_ (let [res (port/-build! e p selected opts)]
                   (if (r/err? res)
                     (r/err :compose/build-failed
                            (assoc (build/failure project selected res output-tail-lines)
                                   :profile (:profile/id p) :dir (:profile/dir p)))
                     res))]
      (let [refs (vec (distinct (map #(get-in facts [% :image]) selected)))
            ids (port/-image-ids e refs)
            active (:active @(:state ctx))
            old (build/running-old-image active project selected)]
        (r/ok (cond-> {:profile (:profile/id p)
                       :project project
                       :dir (:profile/dir p)
                       :built (built-rows facts selected (when (r/ok? ids) (:ok ids)))
                       :running-old-image old
                       :duration-ms (- (now ctx) t0)}
                (r/err? ids) (assoc :image-ids-error (select-keys ids [:error :stderr :exit]))
                (seq old) (assoc :note (str "running containers keep the old image until recreated "
                                            "(restart recreate=true, or up after a config change)"))))))))

(defn build!
  "Result of building the images of section `p` (a Profile with the
   effective compose inputs). `opts`: :services (nil: every service of its
   closure that declares `build:`, see `promote.build/select`), :no-cache?,
   :pull?. Answers {:profile :project :dir :built [{:service :image :id}]
   :running-old-image [{:section :services}] :duration-ms}; nothing to build
   is ok with :built [] and a :note."
  [ctx p {:keys [services no-cache? pull?]}]
  (let [e (:engine ctx)
        project (profile/project p)]
    (if-not (satisfies? port/IImageBuilder e)
      (r/err :compose/build-unsupported {:hint "the compose engine in use cannot build images"})
      (r/let-ok [facts (port/-build-facts e p)
                 selected (build/select facts services)]
        (if (empty? selected)
          (r/ok {:profile (:profile/id p) :project project :dir (:profile/dir p)
                 :built [] :running-old-image [] :duration-ms 0
                 :note "nothing to build: no service of this section declares build:"})
          (let [info {:section (:profile/id p) :services selected :since (now ctx)}]
            (if-not (acquire! ctx project info)
              (let [busy (get (running ctx) project)]
                (r/err :compose/build-busy
                       {:project project :running busy
                        :message (str "a build of compose project " project " already runs ("
                                      (:section busy) ": " (str/join ", " (:services busy))
                                      "); builds of one project run one at a time")
                        :hint "call build again when it has finished"}))
              (try
                (run-build! ctx p selected facts {:no-cache? (boolean no-cache?) :pull? (boolean pull?)})
                (finally (release! ctx project))))))))))
