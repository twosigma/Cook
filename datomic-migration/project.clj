;;
;; Copyright (c) Two Sigma Open Source, LLC
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;  http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
;;
(defproject com.twosigma/datomic-filter-log "0.1.0-SNAPSHOT"
  :profiles {:uberjar {:aot [twosigma.datomic.filter-log]
                       :main twosigma.datomic.filter-log}}
  :aot [twosigma.datomic.filter-log]
  :main twosigma.datomic.filter-log
  :dependencies
  [[ch.qos.logback/logback-classic "1.1.2"]
   [com.basho.riak/riak-client "1.1.0"
    :exclusions
    [ com.google.protobuf/protobuf-java]]
   [com.datomic/datomic-pro "0.9.4956"]
   [factual/clj-leveldb "0.1.1"]
   [joda-time "2.7"]
   [org.apache.curator/curator-framework "2.6.0"]
   [org.clojure/clojure "1.7.0-alpha5"]
   [org.clojure/core.async "0.1.346.0-17112a-alpha"]
   [org.clojure/tools.cli "0.3.1"]
   [org.clojure/tools.logging "0.3.1"]
   [org.clojure/tools.namespace "0.2.9"]
   [org.slf4j/jcl-over-slf4j "1.7.10"]
   [org.slf4j/jul-to-slf4j "1.7.10"]
   [org.slf4j/log4j-over-slf4j "1.7.10"]
   [riemann-clojure-client "0.3.2"]
   [org.slf4j/slf4j-api "1.7.10"]]
  :exclusions
  [commons-logging
   joda-time
   log4j
   org.apache.logging.log4j/log4j
   org.slf4j/simple
   org.slf4j/slf4j-api
   org.slf4j/slf4j-jcl
   org.slf4j/slf4j-nop
   org.slf4j/slf4j-log4j12
   org.slf4j/slf4j-log4j13]
    :jvm-opts ^:replace ["-Xmx20g" "-server"]

  )
