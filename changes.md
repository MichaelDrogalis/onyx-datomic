#### 0.8.3.1
* written tx data is now included in the event map under :datomic/written

#### 0.7.2.0
* Add a read-log input plugin to read the datomic log.

#### 0.7.0.2
* commit-tx :datomic/partition is now optional.
* commit-tx will now allow transaction data in vector as well as map form
* improved validation of task-map data

#### 0.7.0.1
* New index-range catalog type
* read-datoms can now supply components to datoms via :datomic/datoms-components

