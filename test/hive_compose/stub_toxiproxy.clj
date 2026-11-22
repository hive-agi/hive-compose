(ns hive-compose.stub-toxiproxy
  "An in-memory IToxiproxyClient: proxies and their toxics as a value, every
   call recorded, faults injectable per method."
  (:require [hive-compose.port :as port]
            [hive-dsl.result :as r]))

;; SPDX-License-Identifier: MIT

(defn- fault [world method]
  (when (contains? (:fail @world) method)
    (r/err :compose/toxiproxy-unreachable {:method method})))

(defn- record! [world call] (swap! world update :calls conj call))

(defn- shown [p] (update p "toxics" #(vec (vals (sort %)))))

(defrecord StubToxiproxy [world]
  port/IToxiproxyClient
  (-create-proxy! [_ proxy]
    (record! world [:create (get proxy "name")])
    (or (fault world :create)
        (let [n (get proxy "name")
              existing (get-in @world [:proxies n])]
          (cond
            (nil? existing)
            (do (swap! world assoc-in [:proxies n] (assoc proxy "toxics" {}))
                (r/ok (shown (get-in @world [:proxies n]))))
            (= (select-keys existing ["listen" "upstream"]) (select-keys proxy ["listen" "upstream"]))
            (r/ok (shown existing))
            :else (r/err :compose/toxiproxy-proxy-conflict {:name n})))))
  (-add-toxic! [_ proxy-name toxic]
    (record! world [:add proxy-name (get toxic "name")])
    (or (fault world :add)
        (if (get-in @world [:proxies proxy-name])
          (do (swap! world assoc-in [:proxies proxy-name "toxics" (get toxic "name")] toxic)
              (r/ok toxic))
          (r/err :compose/toxiproxy-failed {:status 404 :message "proxy not found"}))))
  (-remove-toxic! [_ proxy-name toxic-name]
    (record! world [:remove proxy-name toxic-name])
    (or (fault world :remove)
        (let [had? (some? (get-in @world [:proxies proxy-name "toxics" toxic-name]))]
          (swap! world update-in [:proxies proxy-name "toxics"] dissoc toxic-name)
          (r/ok {:removed? had?}))))
  (-reset! [_]
    (record! world [:reset])
    (or (fault world :reset)
        (do (swap! world update :proxies
                   (fn [ps] (into {} (map (fn [[k p]] [k (assoc p "toxics" {} "enabled" true)])) ps)))
            (r/ok {:reset true}))))
  (-list [_]
    (or (fault world :list)
        (r/ok (into (sorted-map) (map (fn [[k p]] [k (shown p)])) (:proxies @world))))))

(defn client
  "A fresh StubToxiproxy."
  []
  (->StubToxiproxy (atom {:proxies {} :calls [] :fail #{}})))

(defn calls [c] (:calls @(:world c)))
(defn toxics [c proxy-name] (get-in @(:world c) [:proxies proxy-name "toxics"]))
(defn fail! [c & methods] (swap! (:world c) update :fail into methods))
