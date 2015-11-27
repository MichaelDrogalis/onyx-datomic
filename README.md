## onyx-datomic

Onyx plugin providing read and write facilities for batch processing a Datomic database.

#### Installation

In your project file:

```clojure
[org.onyxplatform/onyx-datomic "0.8.2.4"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.datomic])
```

#### Functions

##### read-datoms

Reads datoms out a Datomic database via `datomic.api/datoms`.

Catalog entry:

```clojure
{:onyx/name :read-datoms
 :onyx/plugin :onyx.plugin.datomic/read-datoms
 :onyx/type :input
 :onyx/medium :datomic
 :datomic/uri db-uri
 :datomic/t t
 :datomic/datoms-index :eavt
 :datomic/datoms-components []
 :datomic/datoms-per-segment 20
 :onyx/max-peers 1
 :onyx/batch-size batch-size
 :onyx/doc "Reads a sequence of datoms from the d/datoms API"}
```

`:datomic/datoms-components` may be used to filter by a datomic index. See the
[Clojure Cookbook](https://github.com/clojure-cookbook/clojure-cookbook/blob/master/06_databases/6-15_traversing-indices.asciidoc) for examples.

Lifecycle entry:

```clojure
{:lifecycle/task :read-datoms
 :lifecycle/calls :onyx.plugin.datomic/read-datoms-calls}
```

###### Attributes

| key                          | type      | description
|------------------------------|-----------|------------
|`:datomic/uri`                | `string`  | The URI of the datomic database to connect to
|`:datomic/t`                  | `integer` | The t-value of the database to read from
|`:datomic/datoms-index`       | `keyword` | datomic index to use in datomic.api/datoms call
|`:datomic/datoms-components`  | `keyword` | components of the datomic index to use (see datomic.api/datoms documentation)
|`:datomic/datoms-per-segment` | `integer` | The number of datoms to compress into a single segment
|`:datomic/read-buffer`        | `integer` | The number of segments to buffer after partitioning, default is `1000`


##### read-index-range

Reads datoms from an indexed attribute via `datomic.api/index-range`.

Catalog entry:

```clojure
{:onyx/name :read-index-datoms
 :onyx/plugin :onyx.plugin.datomic/read-index-range
 :onyx/type :input
 :onyx/medium :datomic
 :datomic/uri db-uri
 :datomic/t t
 :datomic/index-attribute :your-indexed-attribute
 :datomic/index-range-start <<INDEX_START_VALUE>>
 :datomic/index-range-end <<INDEX_END_VALUE>>
 :datomic/datoms-per-segment 20
 :onyx/max-peers 1
 :onyx/batch-size batch-size
 :onyx/doc "Reads a range of datoms from the d/index-range API"}
```

Lifecycle entry:

```clojure
{:lifecycle/task :read-index-datoms
 :lifecycle/calls :onyx.plugin.datomic/read-index-range-calls}
```

###### Attributes

| key                          | type      | description
|------------------------------|-----------|------------
|`:datomic/uri`                | `string`  | The URI of the datomic database to connect to
|`:datomic/t`                  | `integer` | The t-value of the database to read from
|`:datomic/index-attribute`    | `keyword` | datomic indexed attribute
|`:datomic/index-range-start`  | `any`     | inclusive start value for the index range
|`:datomic/index-range-end`    | `any`     | exclusive end value for the index range
|`:datomic/datoms-per-segment` | `integer` | The number of datoms to compress into a single segment
|`:datomic/read-buffer`        | `integer` | The number of segments to buffer after partitioning, default is `1000`

##### read-log

Reads the transaction log via repeated chunked calls of d/tx-range. Continues
to read transactions until `:datomic/log-end-tx` is reached, or forever if
`:datomic/log-end-tx` is nil.

Catalog entry:

```clojure
{:onyx/name :read-log
 :onyx/plugin :onyx.plugin.datomic/read-log
 :onyx/type :input
 :onyx/medium :datomic
 :datomic/uri db-uri
 :datomic/log-start-tx <<OPTIONAL_TX_START_INDEX>>
 :datomic/log-end-tx <<OPTIONAL_TX_END_INDEX>>
 :checkpoint/force-reset? true
 :onyx/max-peers 1
 :onyx/batch-size batch-size
 :onyx/doc "Reads a sequence of datoms from the d/log API"}
```

Lifecycle entry:

```clojure
{:lifecycle/task :read-log
 :lifecycle/calls :onyx.plugin.datomic/read-log-calls}
```

Task will emit a sentinel `:done` when it reaches the tx log-end-tx
(exclusive).

Segments will be read in the form `{:t tx-id :data [[e a v t added] [e a v t added]]}`.

Log read checkpointing is per job - i.e. if a virtual peer crashes, and a new
one is allocated to the task, the new virtual peer will restart reading the log
at the highest acked point. If a new job is started, this checkpoint
information will not be used. In order to persist checkpoint information
between jobs, add `:checkpoint/key "somekey"` to the task-map. This will
persist checkpoint information for cluster (on a given :onyx/id) under the key,
ensuring that any new jobs restart at the checkpoint. This is useful if the
cluster needs to be restarted, or a job is killed and a new one is created in
its place.

###### Attributes

| key                          | type      | description
|------------------------------|-----------|------------
|`:datomic/uri`                | `string`  | The URI of the datomic database to connect to
|`:datomic/log-start-tx`       | `integer` | optional starting tx (inclusive) for log read
|`:datomic/log-end-tx`         | `integer` | optional ending tx (exclusive) for log read. Sentinel will emitted when this tx is passed.
|`:checkpoint/force-reset?`    | `boolean` | whether or not checkpointing should be re-initialised from log-start-tx, or 0 in the case of nil
|`:checkpoint/key`             | `any`     | optional global (for a given onyx/id) key under which to store the checkpoint information. By default the task-id for the job will be used, in which case checkpointing will only be resumed when a virtual peer crashes, and not when a new job is started.
|`:datomic/read-buffer`        | `integer` | The number of segments to buffer after partitioning, default is `1000`

##### commit-tx

Writes new entity maps to datomic. Will automatically assign tempid's for the partition
if a value for :datomic/partition is supplied and datomic transaction data is in map form.

Catalog entry:

```clojure
{:onyx/name :write-datoms
 :onyx/plugin :onyx.plugin.datomic/write-datoms
 :onyx/type :output
 :onyx/medium :datomic
 :datomic/uri db-uri
 :datomic/partition :my.database/optional-partition-name
 :onyx/batch-size batch-size
 :onyx/doc "Transacts segments to storage"}
```

Lifecycle entry:

```clojure
{:lifecycle/task :write-datoms
 :lifecycle/calls :onyx.plugin.datomic/write-tx-calls}
```

###### Attributes

| key                          | type      | description
|------------------------------|-----------|------------
|`:datomic/uri`                | `string`  | The URI of the datomic database to connect to
|`:datomic/partition`          | `keyword` | Optional keyword. When supplied, :db/id tempids are added using this partition.

##### commit-bulk-tx

Writes transactions via the `:tx` segment key to a Datomic database. The value of `:tx` should be as if it were ready for `(d/transact uri tx)`. This lets you perform retractions and arbitrary db functions.

Catalog entry:

```clojure
{:onyx/name :write-bulk-datoms
 :onyx/plugin :onyx.plugin.datomic/write-bulk-datoms
 :onyx/type :output
 :onyx/medium :datomic
 :datomic/uri db-uri
 :datomic/partition :my.database/partition
 :onyx/batch-size batch-size
 :onyx/doc "Transacts segments to storage"}
```

An example value of `:tx` would look like the following:

```clojure
(require '[datomic.api :as d])

{:tx [[:db/add (d/tempid :db.part/user) :db/doc "Hello world"]]}
```

Lifecycle entry:

```clojure
{:lifecycle/task :write-bulk-datoms
 :lifecycle/calls :onyx.plugin.datomic/write-bulk-tx-calls}
```

###### Attributes

| key                          | type      | description
|------------------------------|-----------|------------
|`:datomic/uri`                | `string`  | The URI of the datomic database to connect to
|`:datomic/partition`          | `keyword` | Optional keyword. When supplied, :db/id tempids are added using this partition.

##### Datomic Params Injection via Lifecycles

The datomic params lifecycles inject datomic dbs or conns into `:onyx/fn` params.

###### inject-conn

Injects a datomic conn into the event map. Will also inject as an :onyx/fn param if :onyx/param? is true.

```clojure
{:lifecycle/task :use-conn-task
 :lifecycle/calls :onyx.plugin.datomic/inject-conn-calls
 :datomic/uri db-uri
 :onyx/param? true
 :lifecycle/doc "Initialises datomic conn as a :onyx.core/param"}
```

###### inject-db

Injects a datomic db into the event map. Will also inject as an :onyx/fn param if :onyx/param? is true.

`:datomic/basis-t` is optional, and if supplied it calls datomic.api/as-of on the db using `:datomic/basis-t`.

```clojure
{:lifecycle/task :use-db-task
 :lifecycle/calls :onyx.plugin.datomic/inject-db-calls
 :datomic/uri db-uri
 :datomic/basis-t optional-basis-t
 :onyx/param? true
 :lifecycle/doc "Initialises datomic db as a :onyx.core/param"}
```

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2015 Michael Drogalis

Distributed under the Eclipse Public License, the same as Clojure.
