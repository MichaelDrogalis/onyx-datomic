(ns onyx.plugin.tx-async-output-test
  (:require [midje.sweet :refer :all]
            [datomic.api :as d]
            [clojure.core.async :refer [chan >!! <!! close!]]
            [onyx.plugin.core-async]
            [onyx.plugin.datomic]
            [onyx.api]))

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
    :db/ident :name
    :db/valueType :db.type/string
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   {:db/id #db/id [:db.part/db]
    :db/ident :uuid 
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}
   
   {:db/id #db/id [:db.part/db]
    :db/ident :age
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}])

(d/create-database db-uri)

(def datomic-conn (d/connect db-uri))

(def txes
  [{:tx schema}
   {:tx (map #(assoc % :db/id (d/tempid :db.part/user))
        [{:name "Mike" :age 27 :uuid #uuid "f47ac10b-58cc-4372-a567-0e02b2c3d479"}
         {:name "Dorrene" :age 21}
         {:name "Benti" :age 10}
         {:name "Kristen"}
         {:name "Derek"}])}
   {:tx [{:db/id [:name "Mike"] :age 30}]}
   {:tx [[:db/retract [:name "Dorrene"] :age 21]]}
   {:tx [[:db.fn/cas [:name "Benti"] :age 10 18]]}])

(def in-chan (chan 1000))

(doseq [tx txes]
  (>!! in-chan tx))

(>!! in-chan :done)

(def workflow
  [[:in :identity]
   [:identity :out]])

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size 1000
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :identity
    :onyx/fn :clojure.core/identity
    :onyx/type :function
    :onyx/batch-size 2}
   
   {:onyx/name :out
    :onyx/plugin :onyx.plugin.datomic/write-bulk-datoms-async
    :onyx/type :output
    :onyx/medium :datomic
    :datomic/uri db-uri
    :datomic/partition :com.mdrogalis/people
    :onyx/batch-size 5
    :onyx/doc "Transacts segments to storage"}])

(defn inject-in-ch [event lifecycle]
  {:core.async/chan in-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.tx-async-output-test/in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.datomic/write-bulk-tx-async-calls}])

(def v-peers (onyx.api/start-peers 3 peer-group))

(def job-id
  (:job-id
   (onyx.api/submit-job
    peer-config
    {:catalog catalog :workflow workflow :lifecycles lifecycles
     :task-scheduler :onyx.task-scheduler/balanced})))

(onyx.api/await-job-completion peer-config job-id)

(let [db (d/db datomic-conn)]
  (def results
    (map (comp (juxt :name :age :uuid) (partial d/entity db))
         (apply concat (d/q '[:find ?e :where [?e :name]] db)))))

(fact (set results) =>
      #{["Mike" 30 #uuid "f47ac10b-58cc-4372-a567-0e02b2c3d479"]
        ["Dorrene" nil nil]
        ["Benti" 18 nil]
        ["Kristen" nil nil]
        ["Derek" nil nil]})

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
