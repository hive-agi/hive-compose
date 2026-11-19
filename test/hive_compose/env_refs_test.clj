(ns hive-compose.env-refs-test
  "Env values that are secret references: `{:pass path}` is read from its store
   only when containers or a host program are created, and every surface shows
   it as `pass:<path>`. The store is a stub ISecretBackend; `pass` never runs."
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [hive-addon.protocol :as addon]
            [hive-compose.adapter.cli :as cli]
            [hive-compose.adapter.host :as host]
            [hive-compose.adapter.secrets :as secrets]
            [hive-compose.addon :as sut]
            [hive-compose.port :as port]
            [hive-compose.promote.config :as config]
            [hive-compose.promote.env :as env]
            [hive-compose.schema :as schema]
            [hive-dsl.result :as r]
            [hive-system.secrets.core :as sec]
            [hive-system.secrets.protocols :as proto]))

;; SPDX-License-Identifier: MIT

(def token-path "Funeraria/Whatsapp/token")
(def token-value "stub-value-wa-1f9c")
(def other-path "Funeraria/other")
(def other-value "stub-value-other-77b2")

(def entries {token-path token-value other-path other-value})

(defn- stub-backend
  "An ISecretBackend :pass answering from `entries`, recording every fetch.
   `faults` maps a path to the error the store answers for it."
  ([fetches] (stub-backend fetches {}))
  ([fetches faults]
   (reify proto/ISecretBackend
     (backend-id [_] :pass)
     (fetch [_ k _opts]
       (swap! fetches conj k)
       (cond
         (contains? faults k) (get faults k)
         (contains? entries k) (r/ok (sec/make-secret (get entries k) :pass k))
         :else (r/err :pass/not-found {:key k :stderr (str "Error: " k " is not in the password store.")})))
     (available? [_] (r/ok true)))))

(defn- resolver
  ([fetches] {:pass (stub-backend fetches)})
  ([fetches faults] {:pass (stub-backend fetches faults)}))

(defn- leaks?
  "True when `s` carries a value the store holds."
  [s]
  (boolean (some #(str/includes? (str s) %) (vals entries))))

(def profile {:profile/id "shop/api"
              :profile/dir "/w/shop"
              :profile/services ["api"]
              :profile/env {"WA_TOKEN" {:pass token-path}
                            "SRC" "../../wt/shop"}})

(defn- docker
  "A docker compose exec recording (argv, opts). `up` echoes the WA_TOKEN it was
   given, as a process printing its environment would; `fail-up` makes that
   echo the stderr of a failed up."
  ([calls] (docker calls false))
  ([calls fail-up]
   (fn [argv opts]
     (swap! calls conj [argv opts])
     (let [has (set argv)
           echo (str "WA_TOKEN=" (get-in opts [:env "WA_TOKEN"]))]
       (r/ok (cond
               (has "--profiles") {:exit 0 :stdout ""}
               (has "--services") {:exit 0 :stdout "api\ndb\n"}
               (has "config") {:exit 0 :stdout "{\"services\":{\"api\":{},\"db\":{}}}"}
               (has "ps") {:exit 0 :stdout "[{\"Service\":\"api\",\"State\":\"running\"}]"}
               (has "ls") {:exit 0 :stdout "[]"}
               (has "version") {:exit 0 :stdout "2.29.0"}
               (has "logs") {:exit 0 :stdout echo}
               (and fail-up (has "up")) {:exit 1 :stdout "" :stderr echo}
               :else {:exit 0 :stdout echo}))))))

(defn- env-of
  "The env docker was given on the call whose argv has `verb`."
  [calls verb]
  (some (fn [[argv opts]] (when (some #{verb} argv) (:env opts))) @calls))

;; ---------------------------------------------------------------------------
;; schema and pure renderings

(deftest an-env-value-is-a-string-or-a-pass-reference
  (let [proj {:project/id "dc" :project/dir "/w/dc"}]
    (is (nil? (schema/explain schema/Project
                              (assoc proj :project/env {"CRM_WA_ACCESS_TOKEN" {:pass token-path}
                                                        "SISF_CRM_SRC" "../../wt/crm"}))))
    (testing "a reference names one known scheme and a path"
      (is (some? (schema/explain schema/Project (assoc proj :project/env {"X" {:pass ""}}))))
      (is (some? (schema/explain schema/Project (assoc proj :project/env {"X" {:vault "a/b"}}))))
      (is (some? (schema/explain schema/Project (assoc proj :project/env {"X" {:pass "a" :key "b"}}))))
      (is (some? (schema/explain schema/Project (assoc proj :project/env {"X" 5})))))
    (testing "programs and presets take references too"
      (is (nil? (schema/explain schema/Program {:program/id "crm" :program/dir "/w/crm"
                                                :program/env {"T" {:pass token-path}}})))
      (is (nil? (schema/explain schema/Profile (assoc profile :profile/id "shop")))))
    (testing "the config shape loads, and the reference reaches the section as data"
      (let [cfg {:compose/projects {"dc" {:project/dir "~/dc"
                                          :project/env {"CRM_WA_ACCESS_TOKEN" {:pass token-path}
                                                        "SISF_CRM_SRC" "../../wt/crm"}}}}
            s (config/settings cfg nil "/home/u")]
        (is (r/ok? s))
        (is (= {:pass token-path}
               (get-in (:ok s) [:compose/projects "dc" :project/env "CRM_WA_ACCESS_TOKEN"])))))))

(deftest a-reference-renders-as-scheme-and-path
  (is (env/ref? {:pass token-path}))
  (is (not (env/ref? "pass:x")))
  (is (= "pass:Funeraria/Whatsapp/token" (env/render {:pass token-path})))
  (is (= {"WA_TOKEN" "pass:Funeraria/Whatsapp/token" "SRC" "../../wt/shop"}
         (env/rendered (:profile/env profile))))
  (is (= {:pass "a/b"} (env/from-param {"pass" "a/b"})))
  (is (= "8080" (env/from-param 8080)))
  (testing "scrub puts the rendering where a resolved value was printed"
    (is (= "t=pass:Funeraria/Whatsapp/token SRC=../../wt/shop"
           (env/scrub (:profile/env profile)
                      {"WA_TOKEN" token-value "SRC" "../../wt/shop"}
                      (str "t=" token-value " SRC=../../wt/shop"))))))

;; ---------------------------------------------------------------------------
;; resolution

(deftest resolution-reads-only-references
  (let [fetches (atom [])]
    (testing "plain strings pass through and touch no store"
      (is (= {:ok {"A" "1" "B" "two"}} (secrets/resolve-env (resolver fetches) {"A" "1" "B" "two"})))
      (is (empty? @fetches)))
    (testing "a reference becomes what its entry holds"
      (is (= {:ok {"WA_TOKEN" token-value "SRC" "../../wt/shop"}}
             (secrets/resolve-env (resolver fetches) (:profile/env profile))))
      (is (= [token-path] @fetches)))))

(deftest a-reference-that-cannot-be-read-is-an-error-naming-it
  (let [fetches (atom [])]
    (doseq [[label faults path reason]
            [["missing entry" {} "Funeraria/absent" :pass/not-found]
             ["gpg locked" {token-path (r/err :pass/exec-fail {:key token-path :exit 2
                                                               :stderr "gpg: decryption failed: No secret key"})}
              token-path :pass/exec-fail]
             ["pass absent" {token-path (r/err :pass/exec-fail {:key token-path :cause :shell/exec-failed})}
              token-path :pass/exec-fail]
             ["blank entry" {token-path (r/ok (sec/make-secret "  " :pass token-path))} token-path :pass/empty]]]
      (testing label
        (let [res (secrets/resolve-env (resolver fetches faults) {"A" "1" "WA_TOKEN" {:pass path}})]
          (is (= :compose/secret-unresolved (:error res)))
          (is (= "WA_TOKEN" (:var res)))
          (is (= (str "pass:" path) (:ref res)))
          (is (= reason (:reason res)))
          (is (not (leaks? (pr-str res)))))))
    (testing "an unknown scheme names the ones there are"
      (let [res (secrets/resolve-env (resolver fetches) {"A" {:vault "a/b"}})]
        (is (= :secrets/unknown-backend (:reason res)))
        (is (= "vault:a/b" (:ref res)))
        (is (= ["pass"] (:known res)))))))

(deftest the-pass-adapter-runs-pass-and-keeps-the-first-line
  (let [calls (atom [])
        exec (fn [argv opts] (swap! calls conj [argv opts])
               (r/ok (if (= "Funeraria/absent" (second argv))
                       {:exit 1 :stdout "" :stderr "Error: Funeraria/absent is not in the password store."}
                       {:exit 0 :stdout (str "  " token-value "  \nuser: someone\n") :stderr ""})))
        rs (secrets/make-resolver {:exec exec})]
    (is (= {:ok {"T" token-value}} (secrets/resolve-env rs {"T" {:pass token-path}})))
    (is (= ["pass" token-path] (ffirst @calls)))
    (is (= :pass/not-found (:reason (secrets/resolve-env rs {"T" {:pass "Funeraria/absent"}}))))
    (testing "pass itself failing to run"
      (let [rs (secrets/make-resolver {:exec (fn [_ _] (r/err :shell/exec-failed {:message "No such file"}))})]
        (is (= :pass/exec-fail (:reason (secrets/resolve-env rs {"T" {:pass token-path}}))))))))

;; ---------------------------------------------------------------------------
;; the compose engine

(deftest the-engine-reads-references-only-when-it-creates-containers
  (let [calls (atom [])
        fetches (atom [])
        e (cli/make-engine {:exec (docker calls) :compose/resolver (resolver fetches)})]
    (testing "queries, stop, down, logs and an in-place restart read no store"
      (is (r/ok? (port/-closure e profile)))
      (is (r/ok? (port/-running e profile)))
      (is (r/ok? (port/-listed e profile)))
      (is (r/ok? (port/-stop! e profile ["api"])))
      (is (r/ok? (port/-down! e profile ["api"])))
      (is (r/ok? (port/-restart! e profile ["api"] false)))
      (is (r/ok? (port/-logs e profile ["api"] 10)))
      (is (empty? @fetches))
      (is (every? (fn [[_ opts]] (= "pass:Funeraria/Whatsapp/token" (get-in opts [:env "WA_TOKEN"]))) @calls))
      (is (= "../../wt/shop" (get (env-of calls "stop") "SRC"))))
    (testing "up hands docker the value"
      (reset! calls [])
      (is (r/ok? (port/-up! e profile ["api" "db"])))
      (is (= [token-path] @fetches))
      (is (= {"WA_TOKEN" token-value "SRC" "../../wt/shop"} (env-of calls "up"))))
    (testing "a recreating restart is an up, and reads it too"
      (reset! calls [])
      (is (r/ok? (port/-restart! e profile ["api"] true)))
      (is (= token-value (get (env-of calls "up") "WA_TOKEN"))))
    (testing "an empty service set reads nothing and runs nothing"
      (reset! calls [])
      (reset! fetches [])
      (is (= :no-services (:skipped (:ok (port/-up! e profile [])))))
      (is (empty? @calls))
      (is (empty? @fetches)))))

(deftest an-unreadable-reference-fails-up-before-docker-runs
  (let [calls (atom [])
        e (cli/make-engine {:exec (docker calls) :compose/resolver (resolver (atom []))})
        res (port/-up! e (assoc-in profile [:profile/env "WA_TOKEN"] {:pass "Funeraria/absent"}) ["api"])]
    (is (= :compose/secret-unresolved (:error res)))
    (is (= "pass:Funeraria/absent" (:ref res)))
    (is (= :pass/not-found (:reason res)))
    (is (empty? @calls) "compose never starts with an empty value")))

(deftest what-docker-prints-carries-the-reference-not-the-value
  (let [e (cli/make-engine {:exec (docker (atom [])) :compose/resolver (resolver (atom []))})
        up (port/-up! e profile ["api"])]
    (is (= "WA_TOKEN=pass:Funeraria/Whatsapp/token" (:out (:ok up)))))
  (let [e (cli/make-engine {:exec (docker (atom []) true) :compose/resolver (resolver (atom []))})
        up (port/-up! e profile ["api"])]
    (is (= :compose/command-failed (:error up)))
    (is (= "WA_TOKEN=pass:Funeraria/Whatsapp/token" (:stderr up)))
    (is (not (leaks? (pr-str up))))))

;; ---------------------------------------------------------------------------
;; host programs

(defn- temp-dir []
  (str (java.nio.file.Files/createTempDirectory "hive-compose-env"
                                                (make-array java.nio.file.attribute.FileAttribute 0))))

(deftest a-program-reads-its-references-at-spawn
  (let [spawned (atom [])
        rn (host/make-runner {:spawn (fn [cmd opts] (swap! spawned conj [cmd opts])
                                       (r/ok {:pid 4242 :stdin nil :stdout nil :stderr nil}))
                              :compose/resolver (resolver (atom []))})
        log (str (temp-dir) "/crm.log")
        prog {:program/id "crm" :program/dir "/w/crm" :program/command ["clojure" "-M:nrepl"]
              :program/env {"WA_TOKEN" {:pass token-path} "MODE" "dev"}}]
    (is (= {:ok {:pid 4242}} (port/-start! rn prog log)))
    (is (= {"WA_TOKEN" token-value "MODE" "dev"} (:env (second (first @spawned)))))
    (testing "an unreadable one keeps the program from starting"
      (reset! spawned [])
      (let [res (port/-start! rn (assoc-in prog [:program/env "WA_TOKEN"] {:pass "Funeraria/absent"}) log)]
        (is (= :compose/secret-unresolved (:error res)))
        (is (= "pass:Funeraria/absent" (:ref res)))
        (is (empty? @spawned))))))

;; ---------------------------------------------------------------------------
;; every surface of the tool

(defn- call [a params]
  (let [{:keys [content isError]} ((:handler (first (addon/tools a))) params)
        text (:text (first content))]
    {:error? (boolean isError) :text text :body (json/read-str text :key-fn keyword)}))

(deftest no-surface-shows-a-resolved-value
  (let [dir (temp-dir)
        calls (atom [])
        fetches (atom [])
        engine (cli/make-engine {:exec (docker calls) :compose/resolver (resolver fetches)})
        a (sut/make-addon {:compose/projects {"shop" {:project/dir "/w/shop"
                                                       :project/env {"WA_TOKEN" {:pass token-path}
                                                                     "SRC" "../../wt/shop"}}}
                           :compose/profiles-file (str dir "/absent.edn")
                           :compose/state-file (str dir "/state.edn")
                           :compose/engine engine
                           :compose/initial-delay-ms 3600000})]
    (is (:success? (addon/initialize! a {})))
    (try
      (let [up (call a {"command" "up" "target" "api" "env" {"EXTRA" {"pass" other-path}}})]
        (is (not (:error? up)) (:text up))
        (is (= {"WA_TOKEN" token-value "EXTRA" other-value "SRC" "../../wt/shop"} (env-of calls "up"))
            "docker got the values")
        (is (str/includes? (:text up) "pass:Funeraria/Whatsapp/token"))
        (doseq [params [{"command" "status"}
                        {"command" "targets"}
                        {"command" "programs"}
                        {"command" "projects"}
                        {"command" "ps" "target" "api"}
                        {"command" "logs" "target" "api"}
                        {"command" "restart" "target" "api"}
                        {"command" "restart" "target" "api" "recreate" true}]]
          (let [res (call a params)]
            (is (not (:error? res)) (str params " " (:text res)))
            (is (not (leaks? (:text res))) (str (get params "command") " shows a value"))))
        (is (not (leaks? (:text up))) "up shows a value")
        (is (not (leaks? (slurp (io/file dir "state.edn")))) "the state file holds a value")
        (is (str/includes? (slurp (io/file dir "state.edn")) token-path) "it holds the reference"))
      (testing "an unreadable reference fails the up and names itself"
        (reset! calls [])
        (let [res (call a {"command" "up" "target" "db" "env" {"EXTRA" {"pass" "Funeraria/absent"}}})]
          (is (:error? res))
          (is (= "secret-unresolved" (get-in res [:body :error])))
          (is (= "pass:Funeraria/absent" (get-in res [:body :ref])))
          (is (nil? (env-of calls "up")))))
      (testing "an env param that is neither a string nor a reference is refused"
        (let [res (call a {"command" "up" "target" "api" "env" {"EXTRA" {"vault" "a/b"}}})]
          (is (:error? res))
          (is (= "invalid-param" (get-in res [:body :error])))))
      (finally (addon/shutdown! a)))))
