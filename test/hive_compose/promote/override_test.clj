(ns hive-compose.promote.override-test
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-compose.promote.isolated]
            [hive-compose.promote.override :as ov]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(def model
  "A merged model as `docker compose -p dc config --format json` answers it."
  {"name" "dc"
   "services" {"postgres" {"container_name" "dc-postgres" "image" "postgres:16"
                           "ports" [{"target" 5432 "published" "5434" "protocol" "tcp" "mode" "ingress"}]
                           "volumes" [{"type" "volume" "source" "pgdata" "target" "/var/lib/postgresql/data"}]}
               "firebird-test" {"container_name" "dc-firebird-test" "image" "firebird:3"
                                "ports" [{"target" 3050 "published" "3051" "protocol" "tcp" "host_ip" "127.0.0.1"}]
                                "volumes" [{"type" "volume" "source" "firebird_test_data" "target" "/fb"}
                                           {"type" "bind" "source" "/w/x" "target" "/x"}]}
               "web" {"build" {"context" "/w/web"}
                      "ports" [{"target" 8080 "protocol" "tcp"}]}
               "worker" {"image" "busybox"}}
   "volumes" {"pgdata" {"name" "dc_pgdata"}
              "firebird_test_data" {"name" "dc_firebird_test_data"}
              "ledger" {"name" "ledger-book"}
              "shared" {"name" "shared" "external" true}}})

(defn- ok [x] (is (r/ok? x) (pr-str x)) (:ok x))

(deftest default-spec-resets-names-and-strips-ports
  (let [doc (ok (ov/override-doc model {}))]
    (is (= {"postgres" {"container_name" ov/reset-null "ports" ov/reset-list}
            "firebird-test" {"container_name" ov/reset-null "ports" ov/reset-list}
            "web" {"ports" ov/reset-list}}
           (get doc "services")))
    (is (nil? (get doc "volumes")))
    (is (= (str "\"services\":\n"
                "  \"firebird-test\":\n"
                "    \"container_name\": !reset null\n"
                "    \"ports\": !reset []\n"
                "  \"postgres\":\n"
                "    \"container_name\": !reset null\n"
                "    \"ports\": !reset []\n"
                "  \"web\":\n"
                "    \"ports\": !reset []\n")
           (ov/emit doc)))
    (is (str/includes? (ov/emit (ok (ov/override-doc model {:override/ports :ephemeral})))
                       "  \"postgres\":\n    \"container_name\": !reset null\n    \"ports\": !override\n      -\n        \"host_ip\": \"127.0.0.1\"\n"))))

(deftest port-policies
  (testing ":ephemeral keeps every container port on loopback, host port docker's"
    (is (= (ov/tagged "!override" [{"target" 5432 "protocol" "tcp" "host_ip" "127.0.0.1"}])
           (get-in (ok (ov/override-doc model {:override/ports :ephemeral})) ["services" "postgres" "ports"]))))
  (testing "{:offset n} moves every fixed host port, same address"
    (let [doc (ok (ov/override-doc model {:override/ports {:offset 1000}}))]
      (is (= (ov/tagged "!override" [{"target" 5432 "protocol" "tcp" "published" "6434"}])
             (get-in doc ["services" "postgres" "ports"])))
      (is (= (ov/tagged "!override" [{"target" 3050 "protocol" "tcp" "host_ip" "127.0.0.1" "published" "4051"}])
             (get-in doc ["services" "firebird-test" "ports"])))
      (is (nil? (get-in doc ["services" "web" "ports"])) "no fixed host port, nothing to move")
      (is (= :compose/invalid-override (:error (ov/override-doc model {:override/ports {:offset 65000}}))))))
  (testing "{:publish ..} exactly these, the rest stripped"
    (let [doc (ok (ov/override-doc model {:override/ports {:publish {"postgres" {5432 15432} "worker" {80 0}}}}))]
      (is (= (ov/tagged "!override" [{"target" 5432 "protocol" "tcp" "host_ip" "127.0.0.1" "published" "15432"}])
             (get-in doc ["services" "postgres" "ports"])))
      (is (= (ov/tagged "!override" [{"target" 80 "protocol" "tcp" "host_ip" "127.0.0.1"}])
             (get-in doc ["services" "worker" "ports"])))
      (is (= ov/reset-list (get-in doc ["services" "web" "ports"]))))
    (is (= :compose/invalid-override
           (:error (ov/override-doc model {:override/ports {:publish {"nope" {1 1}}}})))))
  (testing ":keep leaves ports alone"
    (is (nil? (get-in (ok (ov/override-doc model {:override/ports :keep})) ["services" "web"])))))

(deftest fresh-volumes-and-images
  (let [doc (ok (ov/override-doc model {:override/run "r01" :override/fresh-volumes ["firebird_test_data" "ledger"]
                                         :override/names :none :override/ports :keep}))]
    (is (= {"firebird_test_data" {"name" "dc_firebird_test_data_r01"} "ledger" {"name" "dc_ledger_r01"}}
           (get doc "volumes")))
    (is (nil? (get doc "services"))))
  (testing ":all never names an external volume"
    (is (= #{"pgdata" "firebird_test_data" "ledger"}
           (set (keys (get (ok (ov/override-doc model {:override/run "r01" :override/fresh-volumes :all})) "volumes"))))))
  (testing "an external volume is refused, an unknown one too, a missing run too"
    (is (= :compose/fresh-external-volume
           (:error (ov/override-doc model {:override/run "r01" :override/fresh-volumes ["shared"]}))))
    (is (= :compose/invalid-override
           (:error (ov/override-doc model {:override/run "r01" :override/fresh-volumes ["nope"]}))))
    (is (= :compose/invalid-override (:error (ov/override-doc model {:override/fresh-volumes ["ledger"]})))))
  (testing "a build-only service reuses the image the shared project built"
    (is (= {"build" ov/reset-null "image" "dc-web" "pull_policy" "never" "ports" ov/reset-list}
           (get-in (ok (ov/override-doc model {:override/images-from "dc"})) ["services" "web"])))))

(deftest model-reads
  (is (= ["firebird-test" "postgres"] (ov/named-containers model)))
  (is (= {"firebird-test" {"3050" 3051} "postgres" {"5432" 5434}} (ov/published model)))
  (is (= {"firebird-test" {"3050" 4051} "postgres" {"5432" 6434}} (ov/planned-ports model {:offset 1000})))
  (is (nil? (ov/planned-ports model :ephemeral)))
  (is (= {} (ov/planned-ports model :strip)))
  (is (= ["ledger"] (ov/explicit-volumes model)) "dc_<key> is compose's own name; external is not explicit")
  (is (= ["web"] (ov/build-only model))))

(deftest versions
  (is (ov/tags-supported? "2.24.0"))
  (is (ov/tags-supported? "v2.29.1-desktop.1"))
  (is (ov/tags-supported? "5.7.1"))
  (is (not (ov/tags-supported? "2.23.3")))
  (is (not (ov/tags-supported? "1.29.2")))
  (is (not (ov/tags-supported? "garbage"))))

(deftest describe-lines
  (is (= ["firebird-test: container_name reset, host ports stripped"
          "postgres: container_name reset, host ports stripped"
          "web: host ports stripped, image dc-web (no build, never pulled)"]
         (ov/describe (ok (ov/override-doc model {:override/images-from "dc"}))))))

;; ---------------------------------------------------------------------------
;; properties

(def gen-name (gen/fmap #(apply str "s" %) (gen/vector (gen/elements "abcdefgh0123-") 0 6)))

(def gen-port
  (gen/let [t (gen/choose 1 65535)
            h (gen/one-of [(gen/return nil) (gen/fmap str (gen/choose 1 30000))])]
    (cond-> {"target" t "protocol" "tcp"} h (assoc "published" h))))

(def gen-service
  (gen/let [cn (gen/one-of [(gen/return nil) gen-name])
            ports (gen/vector gen-port 0 3)
            build? gen/boolean]
    (cond-> {"image" "x"}
      cn (assoc "container_name" cn)
      (seq ports) (assoc "ports" ports)
      build? (-> (dissoc "image") (assoc "build" {"context" "."})))))

(def gen-model
  (gen/let [svcs (gen/map gen-name gen-service {:max-elements 6})
            vols (gen/map gen-name (gen/elements [{} {"external" true} {"name" "fixed"}]) {:max-elements 4})]
    {"name" "proj" "services" svcs "volumes" vols}))

(def gen-run (gen/fmap #(apply str "r" %) (gen/vector (gen/elements "abc0123456789") 2 10)))

(def gen-spec
  (gen/let [run gen-run
            ports (gen/elements [:strip :ephemeral :keep {:offset 7} {:offset -3}])
            names (gen/elements [:all :none nil])
            fresh (gen/elements [nil :all])
            images (gen/elements [nil "proj"])]
    (ov/spec-of {:run run :ports ports :names names :fresh-volumes fresh :images-from images})))

(defspec plain-model-has-the-empty-override 100
  (prop/for-all [svcs (gen/map gen-name (gen/return {"image" "x" "volumes" [{"type" "bind" "source" "/a" "target" "/b"}]})
                               {:max-elements 5})
                 spec gen-spec]
    (let [doc (ov/override-doc {"name" "proj" "services" svcs} spec)]
      (and (r/ok? doc) (= {} (:ok doc)) (= "{}\n" (ov/emit (:ok doc)))))))

(defspec never-names-a-service-or-volume-absent-from-the-model 200
  (prop/for-all [m gen-model spec gen-spec]
    (let [doc (ov/override-doc m spec)]
      (or (r/err? doc)
          (and (set/subset? (set (keys (get (:ok doc) "services"))) (set (keys (get m "services"))))
               (set/subset? (set (keys (get (:ok doc) "volumes")))
                            (set (remove #(ov/external-volume? m %) (keys (get m "volumes"))))))))))

(defspec fresh-names-are-deterministic-and-unique-per-run 200
  (prop/for-all [k gen-name a gen-run b gen-run]
    (and (= (ov/fresh-name "proj" k a) (ov/fresh-name "proj" k a))
         (= (= a b) (= (ov/fresh-name "proj" k a) (ov/fresh-name "proj" k b))))))

(defspec emitting-is-deterministic-and-order-free 100
  (prop/for-all [m gen-model spec gen-spec]
    (let [doc (ov/override-doc m spec)
          shuffled (update m "services" #(into {} (reverse (seq %))))
          doc2 (ov/override-doc shuffled spec)]
      (or (r/err? doc)
          (and (= (ov/emit (:ok doc)) (ov/emit (:ok doc)))
               (= (ov/emit (:ok doc)) (ov/emit (:ok doc2))))))))

;; The consumer's real port shapes, as `docker compose config --format json`
;; (5.7.1) answers them: a loopback bind, an ephemeral bind already in the
;; file (published "0"), an interpolated one (resolved before we see it,
;; POSTGRES_HOST_PORT=7777 in the call env), and a service with several.
(def shapes
  {"name" "dc"
   "services" {"gw" {"image" "x"
                     "ports" [{"mode" "ingress" "host_ip" "127.0.0.1" "target" 8090 "published" "18090" "protocol" "tcp"}
                              {"mode" "ingress" "host_ip" "127.0.0.1" "target" 9000 "published" "0" "protocol" "tcp"}
                              {"mode" "ingress" "target" 5432 "published" "7777" "protocol" "tcp"}]}
               "envoy" {"image" "x"
                        "ports" [{"target" 10000 "published" "10000" "protocol" "tcp"}
                                 {"target" 10001 "published" "10001" "protocol" "tcp"}
                                 {"target" 9901 "published" "9901" "protocol" "tcp"}]}}})

(deftest real-port-shapes-under-offset
  (let [doc (ok (ov/override-doc shapes {:override/ports {:offset 1000}}))]
    (is (= (ov/tagged "!override"
                      [{"target" 8090 "protocol" "tcp" "host_ip" "127.0.0.1" "published" "19090"}
                       {"target" 9000 "protocol" "tcp" "host_ip" "127.0.0.1" "published" "0"}
                       {"target" 5432 "protocol" "tcp" "published" "8777"}])
           (get-in doc ["services" "gw" "ports"]))
        "loopback keeps its host ip; an ephemeral bind stays ephemeral (0, never 0+N); the resolved value moves")
    (is (= {"gw" {"8090" 19090 "5432" 8777} "envoy" {"10000" 11000 "10001" 11001 "9901" 10901}}
           (ov/planned-ports shapes {:offset 1000}))
        "an ephemeral bind has no host port known before up")
    (is (= #{"HIVE_PORT_GW_8090" "HIVE_PORT_GW_5432" "HIVE_PORT_ENVOY_10000" "HIVE_PORT_ENVOY_10001" "HIVE_PORT_ENVOY_9901"}
           (set (filter #(and (clojure.string/starts-with? % "HIVE_PORT_") (not= % "HIVE_PORT_OFFSET")) (keys (hive-compose.promote.isolated/run-env "dc" "r01" {:offset 1000} (ov/planned-ports shapes {:offset 1000}))))))
        "one variable per published port; none for the ephemeral one")))
