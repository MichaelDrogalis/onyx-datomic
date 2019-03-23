(ns onyx.plugin.test-datomic
  (:require [onyx.plugin.datomic :as sut]
            [onyx.peer.pipeline-extensions :as p-ext]
            [clojure.test :refer :all]
            [datomic.api :as d]))

;; Test exception handling

(def test-ex1
  (ex-info "Transaction timeout"
           {:db/error :db.error/transaction-timeout}))

(def test-ex2
  (Exception. "Transactor Timeout"
              (ex-info "Transactor Unavailable"
                       {:db/error :db.error/transactor-unavailable})))

(def nested-db-error
  (Exception. "Top Level"
              (Exception. "One Down"
                          (Exception. "And another"
                                      (ex-info "Transactor Unavailable"
                                               {:db/error
                                                :db.error/transactor-unavailable})))))

(def nested-other-error
  (Exception. "Top Level"
              (Exception. "One Down"
                          (Exception. "And another"
                                      (ex-info "Not a DB error"
                                               {:not.db/error :something-else})))))

(def test-ex-other1
  (ex-info "Other Exception"
           {:db/error :db.error/unique-conflict}))

(def test-ex-other2
  (Exception. "Other Exception 2" test-ex-other1))

(deftest test-db-error
  (is (= :db.error/transaction-timeout (sut/db-error test-ex1)))
  (is (= :db.error/transactor-unavailable (sut/db-error test-ex2)))
  (is (= :db.error/transactor-unavailable (sut/db-error nested-db-error)))
  (is (nil? (sut/db-error nested-other-error))))

(deftest test-try-deref
  (is (= ::sut/restartable-ex (sut/try-deref (future (throw test-ex1)))))
  (is (= ::sut/restartable-ex (sut/try-deref (future (throw test-ex2)))))
  (is (thrown-with-msg? Exception
                        #"Unrecoverable transaction"
                        (sut/try-deref (future (throw test-ex-other1)))))
  (is (thrown-with-msg? Exception
                        #"Unrecoverable transaction"
                        (sut/try-deref (future (throw test-ex-other2))))))



;; Test plugin write-batch fns

(defn ensure-datomic!
  ([db-uri data]
   (d/create-database db-uri)
   @(d/transact
     (d/connect db-uri)
     data)))

(defn random-db-uri
  []
  (str "datomic:mem://" (java.util.UUID/randomUUID)))

(defn with-db-conn
  [db-uri data f]
  (let [db (ensure-datomic! db-uri data)
        db-conn (d/connect db-uri)]
    (try (f db-conn)
         (finally (d/delete-database db-uri)))))

(def schema
  [{:db/id #db/id [:db.part/db]
    :db/ident :com.mdrogalis/people
    :db.install/_partition :db.part/db}

   {:db/id #db/id [:db.part/db]
    :db/ident :name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}])

(defn mock-msg
  [message]
  {:onyx.core/results
   {:tree
    [{:root
      {:message message,
       :id #uuid "04937498-9738-fadb-7f77-d8b43813ba8f",
       :offset nil,
       :acker-id #uuid "be180c11-603e-42a4-8e96-cef4a85cd83b",
       :completion-id #uuid "be180c11-603e-42a4-8e96-cef4a85cd83b",
       :ack-val -9060905885493246379,
       :hash-group nil,
       :route nil},
      :leaves
      [{:message message,
        :id #uuid "04937498-9738-fadb-7f77-d8b43813ba8f",
        :offset nil,
        :acker-id #uuid "be180c11-603e-42a4-8e96-cef4a85cd83b",
        :completion-id #uuid "be180c11-603e-42a4-8e96-cef4a85cd83b",
        :ack-val nil,
        :hash-group nil,
        :route nil}]}]}})

(def write-datoms-test-event (mock-msg {:name "mike"}))

(def write-bulk-datoms-event
  (mock-msg {:tx [[:db/add (d/tempid :com.mdrogalis/people) :name "Mike"]]}))

(deftest test-write-datoms
  (let [db-uri (random-db-uri)]
    (with-db-conn
      db-uri
      schema
      (fn [conn]
        (let [w-datoms (sut/map->DatomicWriteDatoms {:conn conn
                                                     :partition :com.mdrogalis/people})
              output (p-ext/write-batch w-datoms write-datoms-test-event)]
          (is (true? (:datomic/written? output)))
          (is (not-empty (:tx-data (:datomic/written output)))))))))

(deftest test-write-datoms-tx-restartable
  (let [db-uri (random-db-uri)]
    (with-db-conn
      db-uri
      schema
      (fn [conn]
        (with-redefs [datomic.api/transact (fn [& _] (future (throw test-ex1)))]
          (let [w-datoms (sut/map->DatomicWriteDatoms {:conn conn
                                                       :partition :com.mdrogalis/people})
                ex (try
                     (p-ext/write-batch w-datoms write-datoms-test-event)
                     (catch Exception ex ex))
                output (ex-data ex)]
            (is (true? (:restartable? output)))
            (is (true? (:datomic-plugin? output)))))))))

(deftest test-write-datoms-tx-not-restartable
  (let [db-uri (random-db-uri)]
    (with-db-conn
      db-uri
      schema
      (fn [conn]
        (with-redefs [datomic.api/transact
                      (fn [& _]
                        (future (throw test-ex-other1)))]
          (let [w-datoms (sut/map->DatomicWriteDatoms
                          {:conn conn
                           :partition :com.mdrogalis/people})]
           
            (is (thrown-with-msg? Exception
                                  #"Unrecoverable transaction"
                                  (p-ext/write-batch w-datoms
                                                     write-datoms-test-event)))))))))

(deftest test-write-bulk-datoms
  (let [db-uri (random-db-uri)]
    (with-db-conn
      db-uri
      schema
      (fn [conn]
        (let [w-datoms (sut/map->DatomicWriteBulkDatoms {:conn conn
                                                     :partition :com.mdrogalis/people})
              output (p-ext/write-batch w-datoms write-bulk-datoms-event)]
          (is (true? (:datomic/written? output)))
          (is (not-empty (:tx-data (first (:datomic/written output))))))))))

(deftest test-write-bulk-datoms-tx-restartable
  (let [db-uri (random-db-uri)]
    (with-db-conn
      db-uri
      schema
      (fn [conn]
        (with-redefs [datomic.api/transact (fn [& _] (future (throw test-ex1)))]
          (let [w-datoms (sut/map->DatomicWriteBulkDatoms {:conn conn
                                                           :partition :com.mdrogalis/people})
                ex (try
                     (p-ext/write-batch w-datoms write-bulk-datoms-event)
                     (catch Exception ex ex))
                output (ex-data ex)]
            (is (true? (:restartable? output)))
            (is (true? (:datomic-plugin? output)))))))))

(deftest test-write-datoms-not-restartable
  (let [db-uri (random-db-uri)]
    (with-db-conn
      db-uri
      schema
      (fn [conn]
        (with-redefs [datomic.api/transact
                      (fn [& _]
                        (future (throw test-ex-other1)))]
          (let [w-datoms (sut/map->DatomicWriteBulkDatoms
                          {:conn conn
                           :partition :com.mdrogalis/people})]
           
            (is (thrown-with-msg? Exception
                                  #"Unrecoverable transaction"
                                  (p-ext/write-batch w-datoms
                                                     write-bulk-datoms-event)))))))))

(deftest test-write-bulk-datoms-async
  (let [db-uri (random-db-uri)]
    (with-db-conn
      db-uri
      schema
      (fn [conn]
        (let [w-datoms (sut/map->DatomicWriteBulkDatomsAsync {:conn conn
                                                              :partition :com.mdrogalis/people})
              output (p-ext/write-batch w-datoms write-bulk-datoms-event)]
          (is (true? (:datomic/written? output)))
          (is (not-empty (:tx-data (first (:datomic/written output))))))))))

(deftest test-write-bulk-datoms-async-tx-restartable
  (let [db-uri (random-db-uri)]
    (with-db-conn
      db-uri
      schema
      (fn [conn]
        (with-redefs [datomic.api/transact-async (fn [& _] (future (throw test-ex1)))]
          (let [w-datoms (sut/map->DatomicWriteBulkDatomsAsync {:conn conn
                                                                :partition :com.mdrogalis/people})
                ex (try
                     (p-ext/write-batch w-datoms write-bulk-datoms-event)
                     (catch Exception ex ex))
                output (ex-data ex)]
            (is (true? (:restartable? output)))
            (is (true? (:datomic-plugin? output)))))))))

(deftest test-write-bulk-datoms-not-restartable
  (let [db-uri (random-db-uri)]
    (with-db-conn
      db-uri
      schema
      (fn [conn]
        (with-redefs [datomic.api/transact-async
                      (fn [& _]
                        (future (throw test-ex-other1)))]
          (let [w-datoms (sut/map->DatomicWriteBulkDatomsAsync
                          {:conn conn
                           :partition :com.mdrogalis/people})]
           
            (is (thrown-with-msg? Exception
                                  #"Unrecoverable transaction"
                                  (p-ext/write-batch w-datoms
                                                     write-bulk-datoms-event)))))))))

