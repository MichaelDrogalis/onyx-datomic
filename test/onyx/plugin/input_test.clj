(ns onyx.plugin.input-test
  (:require [aero.core :refer [read-config]]
            [clojure.test :refer [deftest is]]
            [onyx.datomic.api :as d]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.plugin datomic
             [core-async :refer [take-segments! get-core-async-channels]]]
            [onyx.tasks
             [datomic :refer [read-datoms]]
             [core-async :as core-async]]))

(def query '[:find ?a :where
             [?e :user/name ?a]
             [(count ?a) ?x]
             [(<= ?x 5)]])

(defn my-test-query [{:keys [datoms] :as segment}]
  {:names (d/q query datoms)})

(defn build-job [db-uri t batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job (merge {:workflow [[:read-datoms :query]
                                    [:query :persist]]
                         :catalog [{:onyx/name :query
                                    :onyx/fn ::my-test-query
                                    :onyx/type :function
                                    :onyx/batch-size batch-size
                                    :onyx/doc "Queries for names of 5 characters or fewer"}]
                         :lifecycles []
                         :windows []
                         :triggers []
                         :flow-conditions []
                         :task-scheduler :onyx.task-scheduler/balanced})]
    (-> base-job
        (add-task (read-datoms :read-datoms
                               (merge {:datomic/uri db-uri
                                       :datomic/t t
                                       :datomic/datoms-index :eavt
                                       :datomic/datoms-per-segment 20
                                       :onyx/max-peers 1}
                                      batch-settings)))
        (add-task (core-async/output :persist batch-settings 100000)))))

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

(deftest datomic-input-test
  (let [db-uri (str "datomic:mem://" (java.util.UUID/randomUUID))
        {:keys [env-config peer-config]} (read-config
                                          (clojure.java.io/resource "config.edn")
                                          {:profile :test})
        _ (mapv (partial ensure-datomic! db-uri) [[] schema people])
        t (d/next-t (d/db (d/connect db-uri)))
        job (build-job db-uri t 10 1000)
        {:keys [persist]} (get-core-async-channels job)]
    (try
      (with-test-env [test-env [3 env-config peer-config]]
        (onyx.test-helper/validate-enough-peers! test-env job)
        (->> job
             (onyx.api/submit-job peer-config)
             :job-id
             (onyx.test-helper/feedback-exception! peer-config))
        (is (= (set (mapcat #(apply concat %) (map :names (take-segments! persist 50))))
               #{"Mike" "Benti" "Derek"})))
      (finally (d/delete-database db-uri)))))
