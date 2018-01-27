(ns onyx.datomic.protocols)

(defprotocol DatomicHelpers
  (safe-connect [this task-map] "Return datomic connection.")
  (safe-as-of [this task-map conn] "Returns the value of the database as of some time-point.")
  (instance-of-datomic-function? [this v] "Checks if the value is an instance of datomic.function.Function"))

(defprotocol DatomicFns
  (as-of [this] "datomic as-of fn")
  (connect [this] "datomic connect fn")
  (create-database [this] "datomic create-database fn")
  (datoms [this] "datomic datom fn")
  (db [this] "datomic db fn")
  (delete-database [this] "datomic delete-database fn")
  (entity [this] "datomic entity fn")
  (ident [this] "datomic ident fn")
  (index-range [this] "datomic index-range fn")
  (log [this] "datomic log fn")
  (next-t [this] "datomic next-t fn")
  (q [this] "datomic q fn")
  (tempid [this] "datomic tempid fn")
  (transact [this] "datomic transact fn")
  (transact-async [this] "datomic transact-async fn")
  (tx-range [this] "datomic tx-range fn"))
