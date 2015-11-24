(ns onyx.plugin.input-fault-tolerance-test
  "Tests whether the plugin is fault tolerant. Won't make any progress if it restarts each time"
  (:require [clojure.core.async :refer [chan >!! <!!]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.datomic]
            [onyx.api]
            [midje.sweet :refer :all]
            [datomic.api :as d]))

(def id (java.util.UUID/randomUUID))

(def env-config
  {:zookeeper/address "127.0.0.1:2188"
   :zookeeper/server? true
   :zookeeper.server/port 2188
   :onyx/id id})

(def peer-config
  {:zookeeper/address "127.0.0.1:2188"
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port 40200
   :onyx.messaging/bind-addr "localhost"
   :onyx.messaging/backpressure-strategy :high-restart-latency
   :onyx/id id})

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def db-uri (str "datomic:mem://" (java.util.UUID/randomUUID)))

(def schema
  [{:db/id #db/id [:db.part/db]
    :db/ident :com.mdrogalis/people
    :db.install/_partition :db.part/db}

   {:db/id #db/id [:db.part/db]
    :db/ident :user/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}])

(d/create-database db-uri)

(def conn (d/connect db-uri))

@(d/transact conn schema)

(def people
  [{:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Mike"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Dorrene"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Benti"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Derek"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Kristen"}])

@(d/transact conn people)

(def db (d/db conn))

(def t (d/next-t db))

(def batch-size 20)

(def out-chan (chan 1000))

(def query '[:find ?a :where
             [?e :user/name ?a]
             [(count ?a) ?x]
             [(<= ?x 5)]])

(defn my-test-query [{:keys [datoms] :as segment}]
  {:names (d/q query datoms)})

(def workflow
  [[:read-datoms :query]
   [:query :persist]])

(defn restartable? [e]
  true)

(def catalog
  [{:onyx/name :read-datoms
    :onyx/plugin :onyx.plugin.datomic/read-datoms
    :onyx/type :input
    :onyx/medium :datomic
    :datomic/uri db-uri
    :datomic/t t
    :datomic/partition :com.mdrogalis/people
    :datomic/datoms-index :eavt
    :datomic/datoms-per-segment 1
    :onyx/restart-pred-fn :onyx.plugin.input-fault-tolerance-test/restartable?
    :onyx/max-peers 1
    :onyx/batch-size batch-size
    :onyx/doc "Reads a sequence of datoms from the d/datoms API"}

   {:onyx/name :query
    :onyx/fn :onyx.plugin.input-fault-tolerance-test/my-test-query
    :onyx/type :function
    :onyx/batch-size batch-size
    :onyx/doc "Queries for names of 5 characters or fewer"}

   {:onyx/name :persist
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size 20
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}])

(defn inject-persist-ch [event lifecycle]
  {:core.async/chan out-chan})

(def persist-calls
  {:lifecycle/before-task-start inject-persist-ch})

(def batch-num (atom 0))

(def read-datoms-crash
  {:lifecycle/before-batch (fn [event lifecycle]
                             ; give the peer a bit of time to write
                             ;; the chunks out and ack the batches
                             (when (zero? (mod (swap! batch-num inc) 3))
                               (Thread/sleep 3000) 
                               (throw (ex-info "Restartable" {:restartable? true}))))})

(def lifecycles
  [{:lifecycle/task :read-datoms
    :lifecycle/calls :onyx.plugin.datomic/read-datoms-calls}
   {:lifecycle/task :read-datoms
    :lifecycle/calls :onyx.plugin.input-fault-tolerance-test/read-datoms-crash}
   {:lifecycle/task :persist
    :lifecycle/calls :onyx.plugin.input-fault-tolerance-test/persist-calls}
   {:lifecycle/task :persist
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def v-peers (onyx.api/start-peers 4 peer-group))

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow :lifecycles lifecycles
  :task-scheduler :onyx.task-scheduler/balanced})

(def results (take-segments! out-chan))

(fact (sort (mapcat #(apply concat %) (map :names results)))
      => (sort ["Mike" "Benti" "Derek"]))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
