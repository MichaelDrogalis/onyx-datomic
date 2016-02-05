(ns onyx.plugin.input-log-test
  (:require [clojure.core.async :refer [chan >!! <!!]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.datomic]
            [onyx.plugin.tasks.datomic :as t]
            [onyx.api]
            [midje.sweet :refer :all]
            [datomic.api :as d]))


(def id (java.util.UUID/randomUUID))

(def env-config
  {:zookeeper/address "127.0.0.1:2188"
   :zookeeper/server? true
   :zookeeper.server/port 2188
   :onyx/tenancy-id id})

(def peer-config
  {:zookeeper/address "127.0.0.1:2188"
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port 40200
   :onyx.messaging/bind-addr "localhost"
   :onyx.messaging/backpressure-strategy :high-restart-latency
   :onyx/tenancy-id id})

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

(def read-log-1 
  (:task (t/read-log :read-log {:datomic/uri db-uri
                                :checkpoint/key "global-checkpoint-key"
                                :checkpoint/force-reset? false
                                :datomic/log-end-tx 1002
                                :onyx/batch-size batch-size})))

(def catalog
  [(:task-map read-log-1)
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
  (into [{:lifecycle/task :persist
          :lifecycle/calls ::persist-calls}
         {:lifecycle/task :persist
          :lifecycle/calls :onyx.plugin.core-async/writer-calls}]
        (:lifecycles read-log-1)))

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

(def people3
  [{:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Mike3"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Dorrene3"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Benti3"}])

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

(def read-log-2
  (:task (t/read-log :read-log {:datomic/uri db-uri
                                :checkpoint/key "global-checkpoint-key"
                                :checkpoint/force-reset? false
                                :datomic/log-end-tx 1014
                                :onyx/batch-size batch-size})))

(def lifecycles2
  (into [{:lifecycle/task :persist
          :lifecycle/calls ::persist-calls2}
         {:lifecycle/task :persist
          :lifecycle/calls :onyx.plugin.core-async/writer-calls}]
        (:lifecycles read-log-2)))

(def catalog2
  [(:task-map read-log-2)
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
