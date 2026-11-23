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

(defn- merge-marks
  "Preexistence marks ({provider #{service}}) by service against `base`: a
   mark in base stays only while both sides keep it (a run that saw it gone
   dropped it, and that removal sticks); a mark not in base stays when
   either side added it. A side without the key (an older file) counts as
   unchanged, so it never removes a mark."
  [base mine theirs]
  (let [b (or base {}) m (or mine b) t (or theirs b)]
    (into {}
          (keep (fn [p]
                  (let [bs (set (get b p)) ms (set (get m p)) ts (set (get t p))
                        v (into (sorted-set)
                                (filter #(if (bs %) (and (ms %) (ts %)) (or (ms %) (ts %))))
                                (concat bs ms ts))]
                    (when (seq v) [p v]))))
          (distinct (concat (keys b) (keys m) (keys t))))))

(defn merge-run
  "One isolated run record both sides changed (M3). The same run (equal
   :base-project and :compose-project): an ended side wins. Two different
   runs under one id: the LIVE side is kept and flagged :conflict (naming the
   other's base and project), which `promote.isolated/removable` refuses, so
   neither side's volumes can be removed until a human resolves it."
  [_ m t]
  (let [same? (and (= (:base-project m) (:base-project t))
                   (= (:compose-project m) (:compose-project t)))]
    (if same?
      (if (:ended-at t) t m)
      (let [[keep other] (if (and (:ended-at m) (not (:ended-at t))) [t m] [m t])]
        (assoc keep :conflict {:base-project (:base-project other)
                               :compose-project (:compose-project other)})))))

(defn merge-state
  "The persisted state from `base` (last synced), `mine` (this instance) and
   `theirs` (the file now). Each is {:active {..} :current id-or-nil}."
  [base mine theirs]
  (let [bc (:current base) mc (:current mine) tc (:current theirs)]
    (cond-> {:active (merge3 (:active base) (:active mine) (:active theirs) merge-entry)
             :current (if (= tc bc) mc (if (= mc bc) tc mc))}
      ;; stack runs (hc-6), merged like active entries; absent when no side has any
      (some :stacks [base mine theirs])
      (assoc :stacks (merge3 (:stacks base) (:stacks mine) (:stacks theirs) merge-entry))
      ;; preexistence marks by service: an addition from either side lands, a
      ;; reconciled removal (a run saw the service gone) sticks
      ;; isolated runs (FUN-D), merged like stack runs
      (some :runs [base mine theirs])
      (assoc :runs (merge3 (:runs base) (:runs mine) (:runs theirs) merge-run))
      (some :preexisting [base mine theirs])
      (assoc :preexisting (merge-marks (:preexisting base) (:preexisting mine) (:preexisting theirs))))))
