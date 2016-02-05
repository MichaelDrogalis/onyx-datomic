(ns onyx.plugin.input-fault-tolerance-test
  "Tests whether the plugin is fault tolerant. Won't make any progress if it restarts each time"
  (:require [aero.core :refer [read-config]]
            [clojure.test :refer [deftest is]]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.datomic.tasks :refer [read-datomic-datoms]]
            [onyx.plugin
             [core-async :refer [take-segments!]]
             [core-async-tasks :as core-async]
             [datomic]]
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

(def batch-num (atom 0))

(def read-datoms-crash
  {:lifecycle/before-batch (fn [event lifecycle]
                             (when (zero? (mod (swap! batch-num inc) 5))
                               (Thread/sleep 3000)
                               (throw (ex-info "Restartable" {:restartable? true}))))
   :lifecycle/handle-exception (constantly :restart)})

(defn build-job [db-uri t batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job (merge {:workflow [[:read-datoms :query]
                                    [:query :persist]]
                         :catalog [{:onyx/name :query
                                    :onyx/fn ::my-test-query
                                    :onyx/type :function
                                    :onyx/batch-size batch-size
                                    :onyx/doc "Queries for names of 5 characters or fewer"}]
                         :lifecycles [;{:lifecycle/task :read-datoms
                                      ; :lifecycle/calls ::read-datoms-crash}
                                      ]
                         :windows []
                         :triggers []
                         :flow-conditions []
                         :task-scheduler :onyx.task-scheduler/balanced})]
    (-> base-job
        (add-task (read-datomic-datoms :read-datoms
                   (merge {:datomic/uri db-uri
                           :datomic/t t
                           :datomic/datoms-index :eavt
                           :datomic/datoms-per-segment 1
                           :onyx/max-peers 1}
                          batch-settings)))
        (add-task (core-async/output-task :persist batch-settings)))))

(defn ensure-datomic!
  ([db-uri data]
   (d/create-database db-uri)
   @(d/transact
     (d/connect db-uri)
     data)))

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

(deftest datomic-input-fault-tolerance-test
  (let [db-uri (str "datomic:mem://" (java.util.UUID/randomUUID))
        {:keys [env-config peer-config]} (read-config
                                          (clojure.java.io/resource "config.edn")
                                          {:profile :test})
        _ (mapv (partial ensure-datomic! db-uri) [[] schema people])
        t (d/next-t (d/db (d/connect db-uri)))
        job (build-job db-uri t 20 1000)
        {:keys [persist]} (core-async/get-core-async-channels job)]
    (try
      (with-test-env [test-env [4 env-config peer-config]]
        (onyx.test-helper/validate-enough-peers! test-env job)
        (onyx.api/submit-job peer-config job)
        (is (= (sort (mapcat #(apply concat %) (map :names (take-segments! persist))))
               (sort ["Mike" "Benti" "Derek"]))))
      (finally (d/delete-database db-uri)))))
