# metatransaction

## What is a metatransaction?

A metatransaction is a way to link datomic transactions in a single logical transaction. Metatransactions have simple symantics, link a datomic transaction to a metatransaction and commit a metatransaction. The library also supplies a [db filter](https://support.cognitect.com/entries/25976096-Filtering-Databases) to remove transactions that have not had their metatransaction committed. 

## Why use metatransactions?

Metatransactions allow you to craft smaller transactions and handle streaming data while maintaining the semantics of a transaction. 

## Usage

Simple example where a user can submit any number of jobs, one at a time to the server and once all the jobs are sent, a end message is sent to the server. Here we can use metatransactions to logically transact our jobs and only acknowledge them once they are committed.:

```Clojure

(require '[datomic.api :as d]
         '[metatransaction.core :as mt])

(def conn (d/connect uri))

(mt/setup-metatransaction conn)

(def job1 (d/squuid)) ; We suggest using squuids to improve indexing

(d/transact conn [[:metatransaction/include-in job1]
                  {:db/id (d/tempid :db.part/user)
                   :job/id 1
                   :job/uuid job1}])
(d/transact conn [[:metatransaction/commit job1]])

(def job2 (d/squuid))

(d/transact conn [[:metatransaction/include-in job2]
                  {:db/id (d/tempid :db.part/user)
                   :job/id 1
                   :job/uuid job2}])
(d/transact conn [[:metatransaction/include-in job2]
                  {:db/id (d/tempid :db.part/user)
                   :job/id 2
                   :job/uuid job2}])

(d/q '[:find ?job-id ?job-uuid
       :where 
          [?e :job/id ?job-id]
          [?e :job/uuid ?job-uuid]]
        (d/db conn))
;; Will print #{[1 job1] [1 job2] [2 job2]}



(d/q '[:find ?job-id ?job-uuid
        :where
          [?e :job/id ?job-id]
          [?e :job/uuid ?job-uuid]]
       (mt/db conn)) 

; Will print #{[1 job1]}
```

## License

&copy; Two Sigma Open Source, LLC
