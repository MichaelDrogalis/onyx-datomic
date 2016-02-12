(ns onyx.plugin.tasks.datomic
  (:require [schema.core :as s]
            [onyx.schema :as os]
            [taoensso.timbre :refer [info debug fatal]]))

;;;;;;;;;;;;;;
;;;;;;;;;;;;;;
;; task schemas

(def UserTaskMapKey
  (os/build-allowed-key-ns :datomic))

(def DatomicReadLogTaskMap
  (s/->Both [os/TaskMap 
             {:datomic/uri s/Str
              (s/optional-key :datomic/log-start-tx) s/Int
              (s/optional-key :datomic/log-end-tx) s/Int
              (s/optional-key :checkpoint/key) s/Str
              :checkpoint/force-reset? s/Bool
              (s/optional-key :onyx/max-peers) (s/enum 1)
              (s/optional-key :onyx/n-peers) (s/enum 1)
              UserTaskMapKey s/Any}]))

(def DatomicReadDatomsTaskMap
  (s/->Both [os/TaskMap 
             {:datomic/uri s/Str
              :datomic/t s/Int
              :datomic/datoms-index s/Keyword
              :datomic/datoms-per-segment s/Int
              (s/optional-key :datomic/datoms-components) [s/Any]
              (s/optional-key :onyx/max-peers) (s/enum 1)
              (s/optional-key :onyx/n-peers) (s/enum 1)
              UserTaskMapKey s/Any}]))


(def DatomicReadIndexRangeTaskMap
  (s/->Both [os/TaskMap 
             {:datomic/uri s/Str
              :datomic/t s/Int
              :datomic/index-attribute s/Any
              :datomic/index-range-start s/Any
              :datomic/index-range-end s/Any
              :datomic/datoms-per-segment s/Int
              (s/optional-key :onyx/max-peers) (s/enum 1)
              (s/optional-key :onyx/n-peers) (s/enum 1)
              UserTaskMapKey s/Any}]))

(def DatomicWriteDatomsTaskMap
  (s/->Both [os/TaskMap 
             {:datomic/uri s/Str
              (s/optional-key :datomic/partition) (s/either s/Int s/Keyword)
              (s/optional-key :onyx/max-peers) (s/enum 1)
              (s/optional-key :onyx/n-peers) (s/enum 1)
              UserTaskMapKey s/Any}]))

(s/defn read-log
  [task-name :- s/Keyword opts]
  {:task {:task-map (merge {:onyx/name task-name
                            :onyx/plugin :onyx.plugin.datomic/read-log
                            :onyx/type :input
                            :onyx/medium :datomic
                            :onyx/max-peers 1
                            :onyx/doc "Reads a sequence of datoms from the d/log API"}
                           opts)
          :lifecycles [{:lifecycle/task task-name
                        :lifecycle/calls :onyx.plugin.datomic/read-log-calls}]}
   :schema {:task-map (merge os/TaskMap DatomicReadLogTaskMap)
            :lifecycles [os/Lifecycle]}})

(s/defn read-datoms
  [task-name :- s/Keyword opts]
  {:task {:task-map (merge {:onyx/name task-name
                            :onyx/plugin :onyx.plugin.datomic/read-datoms
                            :onyx/type :input
                            :onyx/medium :datomic
                            :datomic/datoms-per-segment 20
                            :onyx/max-peers 1
                            :onyx/doc "Reads a sequence of datoms from the d/log API"}
                           opts)
          :lifecycles [{:lifecycle/task task-name
                        :lifecycle/calls :onyx.plugin.datomic/read-datoms-calls}]}
   :schema {:task-map (merge os/TaskMap DatomicReadDatomsTaskMap)
            :lifecycles [os/Lifecycle]}})

(s/defn read-index-range
  [task-name :- s/Keyword opts]
  {:task {:task-map (merge {:onyx/name task-name
                            :onyx/plugin :onyx.plugin.datomic/read-index-range
                            :onyx/type :input
                            :onyx/medium :datomic
                            :datomic/datoms-per-segment 20
                            :onyx/max-peers 1
                            :onyx/doc "Reads a sequence of datoms from the d/log API"}
                           opts)
          :lifecycles [{:lifecycle/task task-name
                        :lifecycle/calls :onyx.plugin.datomic/read-index-range-calls}]}
   :schema {:task-map (merge os/TaskMap DatomicReadIndexRangeTaskMap)
            :lifecycles [os/Lifecycle]}})

(s/defn write-datoms
  [task-name :- s/Keyword opts]
  {:task {:task-map (merge {:onyx/name task-name
                            :onyx/plugin :onyx.plugin.datomic/write-datoms
                            :onyx/type :output
                            :onyx/medium :datomic
                            :onyx/doc "Transacts segments to storage"}
                           opts)
          :lifecycles [{:lifecycle/task task-name
                        :lifecycle/calls :onyx.plugin.datomic/write-tx-calls}]}
   :schema {:task-map (merge os/TaskMap DatomicWriteDatomsTaskMap)
            :lifecycles [os/Lifecycle]}})

(s/defn write-bulk-tx-datoms
  [task-name :- s/Keyword opts]
  {:task {:task-map (merge {:onyx/name task-name
                            :onyx/plugin :onyx.plugin.datomic/write-bulk-datoms
                            :onyx/type :output
                            :onyx/medium :datomic
                            :onyx/doc "Transacts segments to storage"}
                           opts)
          :lifecycles [{:lifecycle/task task-name
                        :lifecycle/calls :onyx.plugin.datomic/write-bulk-tx-calls}]}
   :schema {:task-map (merge os/TaskMap DatomicWriteDatomsTaskMap)
            :lifecycles [os/Lifecycle]}})
