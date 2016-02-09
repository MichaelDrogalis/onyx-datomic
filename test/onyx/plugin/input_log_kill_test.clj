(ns onyx.plugin.input-log-kill-test
  (:require [clojure.core.async :refer [chan >!! <!! sliding-buffer]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.datomic]
            [onyx.api]
            [taoensso.timbre :refer [info debug fatal]]
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
   :onyx/id id})

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def db-uri (str "datomic:free://127.0.0.1:4334/" 
                 (java.util.UUID/randomUUID)))

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

(def batch-size 20)

(def out-chan (chan (sliding-buffer 1000)))

(def workflow
  [[:read-log :persist]])

(def catalog
  [{:onyx/name :read-log
    :onyx/plugin :onyx.plugin.datomic/read-log
    :onyx/type :input
    :onyx/medium :datomic
    :datomic/uri db-uri
    :checkpoint/key "global-checkpoint-key"
    :checkpoint/force-reset? false
    :onyx/max-peers 1
    :onyx/batch-size batch-size
    :onyx/doc "Reads a sequence of datoms from the d/tx-range API"}

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

(def lifecycles
  [{:lifecycle/task :read-log
    :lifecycle/calls :onyx.plugin.datomic/read-log-calls}
   {:lifecycle/task :persist
    :lifecycle/calls ::persist-calls}
   {:lifecycle/task :persist
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def v-peers (onyx.api/start-peers 3 peer-group))

(def job-id
  (:job-id (onyx.api/submit-job
             peer-config
             {:catalog catalog :workflow workflow :lifecycles lifecycles
              :task-scheduler :onyx.task-scheduler/balanced})))

(def people2
  [{:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Mike2"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Dorrene2"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Benti2"}])

(def transact-constantly
  (future
    (while (not (Thread/interrupted)) 
      (Thread/sleep 5)
      @(d/transact conn people2))))

(Thread/sleep 10000)

(onyx.api/kill-job peer-config job-id)

(fact (onyx.api/await-job-completion peer-config job-id) => false)

(future-cancel transact-constantly)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
