(ns onyx.plugin.input-log-test
  (:require [clojure.core.async :refer [chan >!! <!!]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.datomic]
            [onyx.api]
            [midje.sweet :refer :all]
            [datomic.api :as d]))

;; TODO: NEED TO ADD A TEST SELECTOR SO THIS TEST ONLY RUNS ON CIRCLE CI

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

(def db-uri 
  (str "datomic:free://127.0.0.1:4334/" (java.util.UUID/randomUUID)))

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
    :datomic/log-end-tx 1002
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

(Thread/sleep 15)

(def people2
  [{:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Mike2"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Dorrene2"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Benti2"}])

@(d/transact conn people2)

(def results (take-segments! out-chan))

(onyx.api/await-job-completion peer-config job-id)

(fact (map (fn [result]
             (if (= result :done)
               :done
               ;; drop tx datom and id
               (-> result
                   (update :data rest)
                   (dissoc :id))))
           results)
      =>
      [{:data '(;[13194139534312 50 #inst "2015-08-19T13:27:59.237-00:00" 13194139534312 true]
                [63 10 :com.mdrogalis/people 13194139534312 true]
                [0 11 63 13194139534312 true]
                [64 10 :user/name 13194139534312 true]
                [64 40 23 13194139534312 true]
                [64 41 35 13194139534312 true]
                [0 13 64 13194139534312 true])
        :t 1000}
       {:data '(;[13194139534313 50 #inst "2015-08-19T13:27:59.256-00:00" 13194139534313 true]
                [277076930200554 64 "Mike" 13194139534313 true]
                [277076930200555 64 "Dorrene" 13194139534313 true]
                [277076930200556 64 "Benti" 13194139534313 true]
                [277076930200557 64 "Derek" 13194139534313 true]
                [277076930200558 64 "Kristen" 13194139534313 true])
        :t 1001}
       :done])

(def people3
  [{:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Mike3"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Dorrene3"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Benti3"}])

@(d/transact conn people3)

(def people4
  [{:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Mike4"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Dorrene4"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Benti4"}])

@(d/transact conn people4)
@(d/transact conn people4)
@(d/transact conn people4)

; ;; re-resetup out-chan and restart from checkpoint
(def out-chan2 (chan 1000))

(defn inject-persist-ch2 [event lifecycle]
  {:core.async/chan out-chan2})

(def persist-calls2
  {:lifecycle/before-task-start inject-persist-ch2})

(def lifecycles2
  [{:lifecycle/task :read-log
    :lifecycle/calls :onyx.plugin.datomic/read-log-calls}
   {:lifecycle/task :persist
    :lifecycle/calls ::persist-calls2}
   {:lifecycle/task :persist
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def catalog2
  [{:onyx/name :read-log
    :onyx/plugin :onyx.plugin.datomic/read-log
    :onyx/type :input
    :onyx/medium :datomic
    :datomic/uri db-uri
    :checkpoint/key "global-checkpoint-key"
    :checkpoint/force-reset? false
    :datomic/log-end-tx 1014
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

(onyx.api/submit-job
 peer-config
 {:catalog catalog2 :workflow workflow :lifecycles lifecycles2
  :task-scheduler :onyx.task-scheduler/balanced})

(def results2 (take-segments! out-chan2))

(fact (map (fn [result]
             (if (= result :done)
               :done
               ;; drop tx datom and id
               (-> result
                   (update :data rest)
                   (dissoc :id))))
           results2)
      => [{:data '([277076930200560 64 "Mike2" 13194139534319 true] 
                  [277076930200561 64 "Dorrene2" 13194139534319 true] 
                  [277076930200562 64 "Benti2" 13194139534319 true]), :t 1007} 
          {:data '([277076930200564 64 "Mike3" 13194139534323 true] 
                  [277076930200565 64 "Dorrene3" 13194139534323 true] 
                  [277076930200566 64 "Benti3" 13194139534323 true]), :t 1011} :done])

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
