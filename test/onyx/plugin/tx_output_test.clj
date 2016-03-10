(ns onyx.plugin.tx-output-test
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
   {:tx [[:db.fn/cas [:name "Benti"] :age 10 18]]}
   :done])

(deftest datomic-tx-output-test
  (let [db-uri (str "datomic:mem://" (java.util.UUID/randomUUID))
        {:keys [env-config peer-config]} (read-config
                                          (clojure.java.io/resource "config.edn")
                                          {:profile :test})
        job (build-job db-uri 10 1000)
        {:keys [in]} (core-async/get-core-async-channels job)]
    (try
      (with-test-env [test-env [3 env-config peer-config]]
        (ensure-datomic! db-uri [])
        (pipe (spool txes) in false)
        (onyx.test-helper/validate-enough-peers! test-env job)
        (->> (:job-id (onyx.api/submit-job peer-config job))
             (onyx.api/await-job-completion peer-config))
        (let [db (d/db (d/connect db-uri))]
          (is (= (set (map (comp (juxt :name :age :uuid) (partial d/entity db))
                           (apply concat (d/q '[:find ?e :where [?e :name]] db))))
                 #{["Mike" 30 #uuid "f47ac10b-58cc-4372-a567-0e02b2c3d479"]
                   ["Dorrene" nil nil]
                   ["Benti" 18 nil]
                   ["Kristen" nil nil]
                   ["Derek" nil nil]}))))
      (finally (d/delete-database db-uri)))))
