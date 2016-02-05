(ns onyx.plugin.input-log-kill-test
  (:require [aero.core :refer [read-config]]
            [clojure.test :refer [deftest is testing]]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.datomic.tasks :refer [read-datomic-log]]
            [onyx.plugin
             [core-async :refer [take-segments!]]
             [core-async-tasks :as core-async]
             [datomic]]
            [datomic.api :as d]))


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
   :onyx/tenancy-id id})

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

(def people2
  [{:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Mike2"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Dorrene2"}
   {:db/id (d/tempid :com.mdrogalis/people)
    :user/name "Benti2"}])

(defn transact-constantly [db-uri]
  (future
    (while (not (Thread/interrupted))
      (Thread/sleep 5)
      (ensure-datomic! db-uri people2))))

(deftest ^:ci datomic-input-log-kill-test
  (let [db-uri (str "datomic:free://localhost:4334/" (java.util.UUID/randomUUID))
        {:keys [env-config peer-config]}
        (read-config (clojure.java.io/resource "config.edn") {:profile :test})
        job (build-job db-uri 10 1000)
        {:keys [persist]} (core-async/get-core-async-channels job)
        job-id (atom nil)
        tx-thread (atom nil)]
    (try
      (with-test-env [test-env [4 env-config peer-config]]
        (testing "That we can read the initial transaction log"
          (mapv (partial ensure-datomic! db-uri) [schema people])
          (reset! job-id (:job-id (onyx.api/submit-job peer-config job)))
          (reset! tx-thread (transact-constantly db-uri))
          (Thread/sleep 5000)
          (onyx.api/kill-job peer-config @job-id)
          (is (not (onyx.api/await-job-completion peer-config @job-id)))
          (swap! tx-thread future-cancel)))
      (finally (d/delete-database db-uri)))))
