(ns hive-compose.promote.sync
  "Three-way merge of the persisted state {:active {id Active} :current id}.

   Two addon instances can share one state file for a while: a hot remount
   mounts a new instance while a call still runs on the old one. Each instance
   keeps the state it last read or wrote (`base`). On every write it merges its
   own state (`mine`) with what the file holds now (`theirs`):

     - a key only one side changed takes that side's value, a removal included;
     - a key both sides changed to two maps merges them: claims key by key the
       same way, the rest from the side touched last, :last-touch the later;
     - a key one side removed while the other changed it stays, because losing a
       claim or a running section costs more than keeping an entry the reaper
       or the forget pass drops later.

   Pure: no clock, no file.")

;; SPDX-License-Identifier: MIT

(defn- merge3
  "Merge maps by key against `base`. `both` resolves a key that both sides
   changed to two non-nil values."
  [base mine theirs both]
  (reduce (fn [acc k]
            (let [b (get base k) m (get mine k) t (get theirs k)
                  v (cond (= t b) m
                          (= m b) t
                          (nil? m) t
                          (nil? t) m
                          :else (both b m t))]
              (if (nil? v) (dissoc acc k) (assoc acc k v))))
          {}
          (distinct (concat (keys base) (keys mine) (keys theirs)))))

(defn- later-claim [_ m t]
  (if (>= (:seen m 0) (:seen t 0)) m t))

(defn- merge-entry
  "One active entry both sides changed: claims merged key by key, the rest from
   the side touched last."
  [b m t]
  (let [newer (if (>= (:last-touch m 0) (:last-touch t 0)) m t)
        claims (merge3 (:claims b) (:claims m) (:claims t) later-claim)]
    (cond-> (assoc newer :last-touch (max (:last-touch m 0) (:last-touch t 0)))
      (seq claims) (assoc :claims claims)
      (empty? claims) (dissoc :claims))))

(defn merge-state
  "The persisted state from `base` (last synced), `mine` (this instance) and
   `theirs` (the file now). Each is {:active {..} :current id-or-nil}."
  [base mine theirs]
  (let [bc (:current base) mc (:current mine) tc (:current theirs)]
    {:active (merge3 (:active base) (:active mine) (:active theirs) merge-entry)
     :current (if (= tc bc) mc (if (= mc bc) tc mc))}))
