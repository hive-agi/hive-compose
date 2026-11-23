(ns hive-compose.promote.build
  "Pure law of `compose build` (card 20261123112858-6f2c31bc): which services
   of a section a build builds, the refusals, which active sections keep
   running an image a build replaced, and how a failed build is reported.

   BuildFacts, what the engine answers for the services of a section's
   closure: {service {:build? bool :image ref}}, :build? true when the
   service declares `build:`, :image the reference compose tags the build
   with (its `image:`, else `<compose project>-<service>`)."
  (:require [clojure.string :as str]
            [hive-compose.promote.plan :as plan]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn parse-services
  "The `services` param as a vector of names: \"a,b\" (blanks dropped) or a
   list of names; nil when absent or empty, meaning \"the default\"."
  [v]
  (let [names (cond
                (nil? v) nil
                (string? v) (str/split v #",")
                (sequential? v) (map str v)
                :else [(str v)])
        names (vec (distinct (remove str/blank? (map str/trim names))))]
    (when (seq names) names)))

(defn buildable
  "Sorted services of `facts` that declare `build:`."
  [facts]
  (vec (sort (for [[s {:keys [build?]}] facts :when build?] s))))

(defn select
  "Result<[service]>: what a build of a section whose closure has BuildFacts
   `facts` builds. `requested` nil: every service of the closure that
   declares `build:` (possibly none, an ok []). Else exactly `requested`
   (sorted): a name not in the section is :compose/unknown-names, one that
   declares no `build:` is :compose/invalid-param, each naming it. Pure."
  [facts requested]
  (if (nil? requested)
    (r/ok (buildable facts))
    (let [unknown (vec (sort (remove #(contains? facts %) requested)))
          no-build (vec (sort (filter #(and (contains? facts %) (not (:build? (get facts %)))) requested)))]
      (cond
        (seq unknown)
        (r/err :compose/unknown-names {:param "services" :services unknown
                                       :known (vec (sort (keys facts)))
                                       :message (str "not a service of this section: " (str/join ", " unknown))})
        (seq no-build)
        (r/err :compose/invalid-param {:param "services" :value no-build
                                       :buildable (buildable facts)
                                       :message (str "declares no build: in the compose files: " (str/join ", " no-build))})
        :else (r/ok (vec (sort (distinct requested))))))))

(defn running-old-image
  "[{:section id :services [s]}] of the active entries of compose project
   `project` (`active` {id Active}) whose footprint holds any of `built`:
   their containers keep the image they were created from until recreated.
   By section id. Pure."
  [active project built]
  (let [wanted (set built)]
    (vec (for [[id a] (sort-by key active)
               :when (= project (:project a))
               :let [held (vec (sort (filter wanted (plan/footprint a))))]
               :when (seq held)]
           {:section id :services held}))))

(defn tail-lines
  "The last `n` non-blank lines of `text`."
  [text n]
  (let [ls (vec (remove str/blank? (str/split-lines (str text))))]
    (subvec ls (max 0 (- (count ls) n)))))

(defn failing-service
  "The service of `services` a failed build's output `text` names last
   (BuildKit prefixes a step with `[<service> ..]` and reports `target
   <service>: failed to solve`), the only one when one was built, else nil."
  [services text]
  (let [t (str text)
        hit (fn [s]
              (let [q (java.util.regex.Pattern/quote s)
                    m (re-matcher (re-pattern (str "(?:target |\\[|service \"?)" q "(?:[\\s:\\]\"])")) t)]
                (loop [last-at nil]
                  (if (.find m) (recur (.start m)) last-at))))
        found (keep (fn [s] (when-let [at (hit s)] [at s])) services)]
    (cond
      (seq found) (second (last (sort-by first found)))
      (= 1 (count services)) (first services)
      :else nil)))

(defn failure
  "The data of :compose/build-failed for a build of `services` of compose
   project `project` whose engine call failed with error map `err`: the
   failing service when the output names one, the exit code, and the last
   `n` lines of the build output (a timeout says so)."
  [project services err n]
  (let [text (str (:stderr err) "\n" (:out err))
        timed-out? (= :shell/timeout (:error err))]
    (cond-> {:project project :services (vec services)
             :output-tail (tail-lines text n)
             :message (if timed-out?
                        (str "the build of " (str/join ", " services) " did not finish within "
                             (:timeout-ms err) " ms (:compose/build-timeout-ms)")
                        (str "docker compose build failed"
                             (when-let [s (failing-service services text)] (str " for " s))))}
      (failing-service services text) (assoc :service (failing-service services text))
      (:exit err) (assoc :exit (:exit err))
      timed-out? (assoc :timed-out? true :timeout-ms (:timeout-ms err))
      (not (#{:compose/command-failed :shell/timeout} (:error err))) (assoc :cause (:error err)))))
