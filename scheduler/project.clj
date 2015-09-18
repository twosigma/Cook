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
(defproject cook "0.1.0-SNAPSHOT"
  :description "This launches jobs on a Mesos cluster with fair sharing and preemption"
  :license {:name "Apache License, Version 2.0"}
  :dependencies [[org.clojure/clojure "1.6.0"]

                 ;;Data marshalling
                 [org.clojure/data.codec "0.1.0"]
                 [cheshire "5.3.1"]
                 [byte-streams "0.1.4"]
                 [org.clojure/data.json "0.2.2"]
                 [com.taoensso/nippy "2.8.0"
                  :exclusions [org.clojure/tools.reader]]
                 [circleci/clj-yaml "0.5.4"]

                 ;;Utility
                 [amalloy/ring-buffer "1.1"]
                 [lonocloud/synthread "1.0.4"]
                 [org.clojure/tools.namespace "0.2.4"]
                 [org.clojure/core.cache "0.6.3"]
                 [org.clojure/core.memoize "0.5.6"]
                 [clj-time "0.9.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [prismatic/schema "0.2.1"
                  :exclusions [potemkin]]
                 [clojure-miniprofiler "0.4.0"]
                 [jarohen/chime "0.1.6"]
                 [org.clojure/data.priority-map "0.0.5"]
                 [swiss-arrows "1.0.0"]
                 [riddley "0.1.10"]

                 ;;Logging
                 [org.clojure/tools.logging "0.2.6"]
                 [clj-logging-config "1.9.10"
                  :exclusions [log4j]]
                 [org.slf4j/slf4j-log4j12 "1.7.12"]
                 [com.draines/postal "1.11.0"
                  :exclusions [commons-codec]]
                 [prismatic/plumbing "0.1.1"]
                 [instaparse "1.4.0"]
                 [org.codehaus.jsr166-mirror/jsr166y "1.7.0"]
                 [clj-pid "0.1.1"]
                 [jarohen/chime "0.1.6"]

                 ;;Networking
                 [clj-http "2.0.0"]
                 [io.netty/netty "3.10.1.Final"]
                 [dgrnbrg/metrics-clojure "2.6.1"
                  :exclusions [io.netty/netty
                               org.clojure/clojure]]
                 [cc.qbits/jet "0.5.4"]

                 ;;External system integrations
                 [me.raynes/conch "0.5.2"]
                 [clj-mesos "0.22.2"]
                 [com.google.protobuf/protobuf-java "2.5.0"] ; used by clj-mesos
                 [org.clojure/tools.nrepl "0.2.3"]

                 ;;Ring
                 [ring/ring-core "1.4.0"]
                 [ring/ring-devel "1.4.0"]
                 [compojure "1.4.0"]
                 [hiccup "1.0.5"]
                 [ring/ring-json "0.2.0"]
                 [ring-edn "0.1.0"]
                 [com.duelinmarkers/ring-request-logging "0.2.0"]
                 [liberator "0.13"]

                 ;;Databases
                 [com.datomic/datomic-free "0.9.5206"
                  :exclusions [org.slf4j/slf4j-api
                               com.fasterxml.jackson.core/jackson-core
                               org.slf4j/jcl-over-slf4j
                               org.slf4j/jul-to-slf4j
                               org.slf4j/log4j-over-slf4j
                               org.slf4j/slf4j-nop
                               joda-time]]
                 [org.apache.curator/curator-framework "2.7.1"
                  :exclusions [io.netty/netty]]
                 [org.apache.curator/curator-recipes "2.7.1"
                  :exclusions [org.slf4j/slf4j-log4j12
                               org.slf4j/log4j
                               log4j
                               ]]
                 [org.apache.curator/curator-test "2.7.1"]

                 ;; incanter
                 ;[incanter "1.5.4"]
  ]

  :repositories {"maven2" {:url "http://files.couchbase.com/maven2/"}
                 "sonatype-oss-public" "https://oss.sonatype.org/content/groups/public/"}

  :filespecs [{:type :fn
               :fn (fn [p]
                     {:type :bytes :path "git-log"
                      :bytes (.trim (:out (clojure.java.shell/sh
                                            "git" "rev-parse" "HEAD")))})}]

  :profiles
  {:uberjar
   {:aot [cook.components]}

   :dev
   {:dependencies [[clj-http-fake "1.0.1"]
                   [org.clojure/test.check "0.6.1"]]
    :jvm-opts ["-Xms2G"
               "-XX:-OmitStackTraceInFastThrow"
               "-Xmx2G"]
    :source-paths []}}

  :test-selectors {:default (complement :integration)
                   :integration :integration
                   :all (constantly true)}

  :main cook.components

  :jvm-opts ["-Dpython.cachedir.skip=true"
             "-XX:MaxPermSize=500M"
             ;"-Dsun.security.jgss.native=true"
             ;"-Dsun.security.jgss.lib=/opt/mitkrb5/lib/libgssapi_krb5.so"
             ;"-Djavax.security.auth.useSubjectCredsOnly=false"
             "-verbose:gc"
             "-XX:+PrintGCDetails"
             "-Xloggc:gclog"
             "-XX:+UseGCLogFileRotation"
             "-XX:NumberOfGCLogFiles=20"
             "-Dcom.sun.management.jmxremote.port=5555"
             "-Dcom.sun.management.jmxremote.authenticate=false"
             "-Dcom.sun.management.jmxremote.ssl=false"
             "-XX:GCLogFileSize=128M"
             "-XX:+PrintGCDateStamps"
             "-XX:+HeapDumpOnOutOfMemoryError"
             ])
