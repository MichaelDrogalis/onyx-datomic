(ns onyx.plugin.datomic
  (:require [clojure.core.async :refer [chan >! >!! <!! close! thread timeout alts!! go-loop sliding-buffer]]
            [datomic.api :as d]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.peer.function :as function]
            [onyx.types :as t]
            [onyx.static.default-vals :refer [defaults]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info debug fatal]]))

(defn safe-connect [task-map]
  (if-let [uri (:datomic/uri task-map)]
    (d/connect uri)
    (throw (ex-info ":datomic/uri missing from write-datoms task-map." task-map))))

(defn safe-as-of [task-map conn]
  (if-let [t (:datomic/t task-map)]
    (d/as-of (d/db conn) t)
    (throw (ex-info ":datomic/t missing from write-datoms task-map." task-map))))

(defn safe-datoms-per-segment [task-map]
  (or (:datomic/datoms-per-segment task-map)
      (throw (ex-info ":datomic/datoms-per-segment missing from write-datoms task-map." task-map))))

(defn start-commit-loop! [commit-ch log k]
  (go-loop []
           (when-let [content (<!! commit-ch)]
             (extensions/force-write-chunk log :chunk content k)
             (recur))))

;;;;;;;;;;;;;
;;;;;;;;;;;;;
;; input plugins

(defn unroll-datom
  "Turns a datom into a vector of :eavt+op."
  [db datom]
  [(:e datom)
   (d/ident db (:a datom))
   (:v datom)
   (:tx datom)
   (:added datom)])

(defn datoms-sequence [db task-map]
  (case (:onyx/plugin task-map)
    ::read-datoms
    (let [datoms-components (or (:datomic/datoms-components task-map) [])
          datoms-index (:datomic/datoms-index task-map)]
      (apply d/datoms db datoms-index datoms-components))
    ::read-index-range
    (let [attribute (:datomic/index-attribute task-map)
          range-start (:datomic/index-range-start task-map)
          range-end (:datomic/index-range-end task-map)]
      (d/index-range db attribute range-start range-end))))

(defn close-read-datoms-resources
  [{:keys [datomic/producer-ch datomic/commit-ch datomic/read-ch] :as event} lifecycle]
  (close! read-ch)
  (close! commit-ch)
  (close! producer-ch)
  {})

(defn inject-read-datoms-resources
  [{:keys [onyx.core/task-map onyx.core/log onyx.core/task-id onyx.core/pipeline] :as event} lifecycle]
  (when-not (or (= 1 (:onyx/max-peers task-map))
                (= 1 (:onyx/n-peers task-map)))
    (throw (ex-info "Read datoms tasks must set :onyx/max-peers 1" task-map)))
  (let [_ (extensions/write-chunk log :chunk {:chunk-index -1 :status :incomplete} task-id)
        content (extensions/read-chunk log :chunk task-id)]
    (if (= :complete (:status content))
      (throw (Exception. "Restarted task and it was already complete. This is currently unhandled."))
      (let [ch (:read-ch pipeline)
            start-index (:chunk-index content)
            conn (safe-connect task-map)
            db (safe-as-of task-map conn)
            datoms-per-segment (safe-datoms-per-segment task-map)
            unroll (partial unroll-datom db)
            num-ignored (* start-index datoms-per-segment)
            commit-loop-ch (start-commit-loop! (:commit-ch pipeline) log task-id)
            producer-ch (thread
                          (try
                            (loop [chunk-index (inc start-index)
                                   datoms (seq (drop num-ignored
                                                     (datoms-sequence db task-map)))]
                              (when datoms
                                (let [success? (>!! ch (assoc (t/input (random-uuid)
                                                                       {:datoms (map unroll (take datoms-per-segment datoms))})
                                                              :chunk-index chunk-index))]
                                  (if success?
                                    (recur (inc chunk-index)
                                           (seq (drop datoms-per-segment datoms)))))))
                            (>!! ch (t/input (random-uuid) :done))
                            (catch Exception e
                              (fatal e))))]

        {:datomic/read-ch ch
         :datomic/commit-ch (:commit-ch pipeline)
         :datomic/producer-ch producer-ch
         :datomic/drained? (:drained pipeline)
         :datomic/top-chunk-index (:top-chunk-index pipeline)
         :datomic/top-acked-chunk-index (:top-acked-chunk-index pipeline)
         :datomic/pending-chunk-indices (:pending-chunk-indices pipeline)
         :datomic/pending-messages (:pending-messages pipeline)}))))

(defn highest-acked-chunk [starting-index max-index pending-chunk-indices]
  (loop [max-pending starting-index]
    (if (or (pending-chunk-indices (inc max-pending))
            (= max-index max-pending))
      max-pending
      (recur (inc max-pending)))))

(defn all-done? [messages]
  (empty? (remove #(= :done (:message %))
                  messages)))

(defrecord DatomicInput [log task-id max-pending batch-size batch-timeout
                         pending-messages drained?
                         top-chunk-index top-acked-chunk-index pending-chunk-indices
                         read-ch commit-ch]
  p-ext/Pipeline
  (write-batch
    [this event]
    (function/write-batch event))

  (read-batch
    [_ event]
    (let [pending (count (keys @pending-messages))
          max-segments (min (- max-pending pending) batch-size)
          timeout-ch (timeout batch-timeout)
          batch (->> (range max-segments)
                     (keep (fn [_] (first (alts!! [read-ch timeout-ch] :priority true)))))]
      (doseq [m batch]
        (when-let [chunk-index (:chunk-index m)]
          (swap! top-chunk-index max chunk-index)
          (swap! pending-chunk-indices conj chunk-index))
        (swap! pending-messages assoc (:id m) m))
    (when (and (all-done? (vals @pending-messages))
               (all-done? batch)
               (or (not (empty? @pending-messages))
                   (not (empty? batch))))
      (>!! commit-ch {:status :complete})
      (reset! drained? true))
    {:onyx.core/batch batch}))

  p-ext/PipelineInput

  (ack-segment [_ _ segment-id]
    (let [chunk-index (:chunk-index (@pending-messages segment-id))]
      (swap! pending-chunk-indices disj chunk-index)
      (let [new-top-acked (highest-acked-chunk @top-acked-chunk-index @top-chunk-index @pending-chunk-indices)]
        (>!! commit-ch {:chunk-index new-top-acked :status :incomplete})
        (reset! top-acked-chunk-index new-top-acked))
      (swap! pending-messages dissoc segment-id)))

  (retry-segment
    [_ event segment-id]
    (when-let [msg (get @pending-messages segment-id)]
      (>!! read-ch (assoc msg :id (random-uuid))))
    (swap! pending-messages dissoc segment-id))

  (pending?
    [_ _ segment-id]
    (get @pending-messages segment-id))

  (drained?
    [_ _]
    @drained?))

(defn shared-input-builder [pipeline-data]
  (let [catalog-entry (:onyx.core/task-map pipeline-data)
        max-pending (or (:onyx/max-pending catalog-entry) (:onyx/max-pending defaults))
        batch-size (:onyx/batch-size catalog-entry)
        batch-timeout (or (:onyx/batch-timeout catalog-entry) (:onyx/batch-timeout defaults))
        read-ch (chan (or (:datomic/read-buffer catalog-entry) 1000))
        commit-ch (chan (sliding-buffer 1))]
    (->DatomicInput (:onyx.core/log pipeline-data)
                    (:onyx.core/task-id pipeline-data)
                    max-pending batch-size batch-timeout
                    (atom {})
                    (atom false)
                    (atom -1)
                    (atom -1)
                    (atom #{})
                    read-ch
                    commit-ch)))

(defn read-datoms [pipeline-data]
  (shared-input-builder pipeline-data))

(defn read-index-range [pipeline-data]
  (shared-input-builder pipeline-data))

;;;;;;;;;;;;;
;;;;;;;;;;;;;
;; read log plugin

(defn unroll-log-datom
  "Turns a log datom into a vector of :eavt+op."
  [datom]
  [(:e datom)
   (:a datom)
   (:v datom)
   (:tx datom)
   (:added datom)])

(defn close-read-log-resources
  [{:keys [datomic/producer-ch datomic/commit-ch datomic/read-ch datomic/shutdown-ch] :as event} lifecycle]
  (close! read-ch)
  (close! commit-ch)
  (close! shutdown-ch)
  (<!! producer-ch)
  {})

(defn set-starting-offset! [log task-map checkpoint-key start-tx]
  (if (:checkpoint/force-reset? task-map)
    (extensions/force-write-chunk log :chunk {:largest (or start-tx -1)
                                              :status :incomplete}
                                  checkpoint-key)
    (extensions/write-chunk log :chunk {:largest (or start-tx -1)
                                        :status :incomplete}
                            checkpoint-key)))

(defn validate-within-supplied-bounds [start-tx end-tx checkpoint-tx]
  (when checkpoint-tx
    (when (and start-tx (< checkpoint-tx start-tx))
      (throw (ex-info "Checkpointed transaction is less than :datomic/log-start-tx"
                      {:datomic/log-start-tx start-tx
                       :datomic/log-end-tx end-tx
                       :checkpointed-tx checkpoint-tx})))
    (when (and end-tx (>= checkpoint-tx end-tx))
      (throw (ex-info "Checkpointed transaction is greater than :datomic/log-start-tx"
                      {:datomic/log-start-tx start-tx
                       :datomic/log-end-tx end-tx
                       :checkpointed-tx checkpoint-tx})))))

(defn check-completed [task-map checkpointed]
  (when (and (not (:checkpoint/key task-map))
             (= :complete (:status checkpointed)))
    (throw (Exception. "Restarted task, however it was already completed for this job.
                       This is currently unhandled."))))

(defn inject-read-log-resources
  [{:keys [onyx.core/task-map onyx.core/log onyx.core/task-id onyx.core/pipeline] :as event} lifecycle]
  (when-not (or (= 1 (:onyx/max-peers task-map))
                (= 1 (:onyx/n-peers task-map)))
    (throw (ex-info "Read log tasks must set :onyx/max-peers 1" task-map)))
  (let [start-tx (:datomic/log-start-tx task-map)
        max-tx (:datomic/log-end-tx task-map)
        {:keys [read-ch shutdown-ch commit-ch]} pipeline
        checkpoint-key (or (:checkpoint/key task-map) task-id)
        _ (set-starting-offset! log task-map checkpoint-key start-tx)
        checkpointed (extensions/read-chunk log :chunk checkpoint-key)
        _ (validate-within-supplied-bounds start-tx max-tx (:largest checkpointed))
        _ (check-completed task-map checkpointed)
        read-size (or (:datomic/read-max-chunk-size task-map) 1000)
        batch-timeout (or (:onyx/batch-timeout task-map) (:onyx/batch-timeout defaults))
        initial-backoff 1
        conn (safe-connect task-map)
        commit-loop-ch (start-commit-loop! commit-ch log checkpoint-key)
        producer-ch (thread
                      (try
                        (let [exit (loop [tx-index (inc (:largest checkpointed)) backoff initial-backoff]
                                     ;; relies on the fact that tx-range is lazy, therefore only read-size elements will be realised
                                     ;; always use a nil end-tx so that we don't have to rely on a tx id existing
                                     ;; in order to determine whether we should emit the sentinel (tx ids don't always increment)
                                     (if (first (alts!! [shutdown-ch] :default true))
                                       (if-let [entries (seq
                                                          (take read-size
                                                                (seq
                                                                  (d/tx-range (d/log conn) tx-index nil))))]
                                         (let [last-t (:t (last entries))
                                               next-t (inc last-t)]
                                           (doseq [entry (filter #(or (nil? max-tx)
                                                                      (< (:t %) max-tx))
                                                                 entries)]
                                             (>!! read-ch
                                                  (t/input (random-uuid)
                                                           (update (into {} entry)
                                                                   :data
                                                                   (partial map unroll-log-datom)))))
                                           (if (or (nil? max-tx)
                                                   (< last-t max-tx))
                                             (recur next-t initial-backoff)))
                                         (let [next-backoff (min (* 2 backoff) batch-timeout)]
                                           (Thread/sleep backoff)
                                           (recur tx-index next-backoff)))
                                       :shutdown))]
                          (if-not (= exit :shutdown)
                            (>!! read-ch (t/input (random-uuid) :done))))
                        (catch Exception e
                          (fatal e))))]

    {:datomic/read-ch read-ch
     :datomic/shutdown-ch shutdown-ch
     :datomic/commit-ch commit-ch
     :datomic/producer-ch producer-ch
     :datomic/drained? (:drained pipeline)
     :datomic/pending-messages (:pending-messages pipeline)}))

(defn highest-acked-tx [starting-tx max-tx pending-txes]
  (loop [max-pending starting-tx]
    (if (or (pending-txes (inc max-pending))
            (= max-tx max-pending))
      max-pending
      (recur (inc max-pending)))))

(defrecord DatomicLogInput
  [log task-id max-pending batch-size batch-timeout pending-messages drained?
   top-tx top-acked-tx pending-txes
   read-ch commit-ch shutdown-ch]
  p-ext/Pipeline
  (write-batch
    [this event]
    (function/write-batch event))

  (read-batch
    [_ event]
    (let [pending (count (keys @pending-messages))
          max-segments (min (- max-pending pending) batch-size)
          timeout-ch (timeout batch-timeout)
          batch (if (zero? max-segments) 
                  (<!! timeout-ch)
                  (->> (range max-segments)
                       (keep (fn [_] (first (alts!! [read-ch timeout-ch] :priority true))))))]
      (doseq [m batch]
        (let [message (:message m)]
          (when-not (= message :done)
            (swap! top-tx max (:t message))
            (swap! pending-txes conj (:t message))))
        (swap! pending-messages assoc (:id m) m))
      (when (and (all-done? (vals @pending-messages))
                 (all-done? batch)
                 (zero? (count (.buf read-ch)))
                 (or (not (empty? @pending-messages))
                     (not (empty? batch))))
        (when-not (:checkpoint/key (:onyx.core/task-map event))
          (>!! commit-ch {:status :complete}))
        (reset! drained? true))
      {:onyx.core/batch batch}))

  p-ext/PipelineInput

  (ack-segment [_ _ segment-id]
    (let [tx (:t (:message (@pending-messages segment-id)))]
      (swap! pending-txes disj tx)
      ;; if this transaction is now the lowest unacked tx, then we can update the checkpoint
      (let [new-top-acked (highest-acked-tx @top-acked-tx @top-tx @pending-txes)]
        (>!! commit-ch {:largest new-top-acked :status :incomplete})
        (reset! top-acked-tx new-top-acked))
      (swap! pending-messages dissoc segment-id)))

  (retry-segment
    [_ event segment-id]
    (when-let [msg (get @pending-messages segment-id)]
      (>!! read-ch (assoc msg :id (random-uuid))))
    (swap! pending-messages dissoc segment-id))

  (pending?
    [_ _ segment-id]
    (get @pending-messages segment-id))

  (drained?
    [_ _]
    @drained?))

(defn read-log [pipeline-data]
  (let [catalog-entry (:onyx.core/task-map pipeline-data)
        max-pending (or (:onyx/max-pending catalog-entry) (:onyx/max-pending defaults))
        batch-size (:onyx/batch-size catalog-entry)
        batch-timeout (or (:onyx/batch-timeout catalog-entry) (:onyx/batch-timeout defaults))
        read-ch (chan (or (:datomic/read-buffer catalog-entry) 1000))
        commit-ch (chan (sliding-buffer 1))
        shutdown-ch (chan 1)
        top-tx (atom -1)
        top-acked-tx (atom -1)
        pending-txes (atom #{})]
    (->DatomicLogInput (:onyx.core/log pipeline-data)
                       (:onyx.core/task-id pipeline-data)
                       max-pending batch-size batch-timeout
                       (atom {})
                       (atom false)
                       top-tx
                       top-acked-tx
                       pending-txes
                       read-ch
                       commit-ch
                       shutdown-ch)))

(def read-log-calls
  {:lifecycle/before-task-start inject-read-log-resources
   :lifecycle/after-task-stop close-read-log-resources})

;;;;;;;;;;;;;
;;;;;;;;;;;;;
;; output plugins

(defn inject-write-tx-resources
  [{:keys [onyx.core/pipeline]} lifecycle]
  {:datomic/conn (:conn pipeline)})

(defn inject-write-bulk-tx-resources
  [{:keys [onyx.core/pipeline]} lifecycle]
  {:datomic/conn (:conn pipeline)})

(defrecord DatomicWriteDatoms [conn partition]
  p-ext/Pipeline
  (read-batch
    [_ event]
    (function/read-batch event))

  (write-batch
    [_ event]
    (let [messages (mapcat :leaves (:tree (:onyx.core/results event)))]
      @(d/transact conn
                   (map (fn [msg] (if (and partition (not (sequential? msg)))
                                    (assoc msg :db/id (d/tempid partition))
                                    msg))
                        (map :message messages)))
      {:onyx.core/written? true}))

  (seal-resource
    [_ _]
    {}))

(defn write-datoms [pipeline-data]
  (let [task-map (:onyx.core/task-map pipeline-data)
        conn (safe-connect task-map)
        partition (:datomic/partition task-map)]
    (->DatomicWriteDatoms conn partition)))

(defrecord DatomicWriteBulkDatoms [conn]
  p-ext/Pipeline
  (read-batch
    [_ event]
    (function/read-batch event))

  (write-batch
    [_ event]
    ;; Transact each tx individually to avoid tempid conflicts.
    (doseq [tx (mapcat :leaves (:tree (:onyx.core/results event)))]
      @(d/transact conn (:tx (:message tx))))
    {:onyx.core/written? true})

  (seal-resource
    [_ _]
    {}))

(defn write-bulk-datoms [pipeline-data]
  (let [task-map (:onyx.core/task-map pipeline-data)
        conn (safe-connect task-map)]
    (->DatomicWriteBulkDatoms conn)))

(def read-datoms-calls
  {:lifecycle/before-task-start inject-read-datoms-resources
   :lifecycle/after-task-stop close-read-datoms-resources})

(def read-index-range-calls
  {:lifecycle/before-task-start inject-read-datoms-resources
   :lifecycle/after-task-stop close-read-datoms-resources})


(def write-tx-calls
  {:lifecycle/before-task-start inject-write-tx-resources})

(def write-bulk-tx-calls
  {:lifecycle/before-task-start inject-write-bulk-tx-resources})


;;;;;;;;;
;;; params lifecycles

(defn inject-db [{:keys [onyx.core/params] :as event} {:keys [datomic/basis-t datomic/uri onyx/param?] :as lifecycle}]
  (when-not uri
    (throw (ex-info "Missing :datomic/uri in inject-db-calls lifecycle." lifecycle)))
  (let [conn (d/connect (:datomic/uri lifecycle))
        db (cond-> (d/db conn)
             basis-t (d/as-of basis-t))]
    {:datomic/conn conn
     :datomic/db db
     :onyx.core/params (if param?
                         (conj params db)
                         params)}))

(def inject-db-calls
  {:lifecycle/before-task-start inject-db})

(defn inject-conn [{:keys [onyx.core/params] :as event} {:keys [datomic/uri onyx/param?] :as lifecycle}]
  (when-not uri
    (throw (ex-info "Missing :datomic/uri in inject-conn-calls lifecycle."
                    lifecycle)))
  (let [conn (d/connect uri)]
    {:datomic/conn conn
     :onyx.core/params (if param?
                         (conj params conn)
                         params)}))

(def inject-conn-calls
  {:lifecycle/before-task-start inject-conn})
