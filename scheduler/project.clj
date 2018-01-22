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
(defproject cook "1.9.1-SNAPSHOT"
  :description "This launches jobs on a Mesos cluster with fair sharing and preemption"
  :license {:name "Apache License, Version 2.0"}
  :dependencies [[org.clojure/clojure "1.8.0"]

                 ;;Data marshalling
                 [org.clojure/data.codec "0.1.0"]
                 [byte-streams "0.1.4"]
                 [org.clojure/data.json "0.2.2"]
                 [com.taoensso/nippy "2.8.0"
                  :exclusions [org.clojure/tools.reader]]
                 [circleci/clj-yaml "0.5.5"]
                 [camel-snake-kebab "0.4.0"]
                 [com.rpl/specter "1.0.1"]

                 ;;Utility
                 [amalloy/ring-buffer "1.1"]
                 [listora/ring-congestion "0.1.2"]
                 [lonocloud/synthread "1.0.4"]
                 [org.clojure/tools.namespace "0.2.4"]
                 [org.clojure/core.cache "0.6.4"]
                 [org.clojure/core.memoize "0.5.8"]
                 [clj-time "0.9.0"]
                 [org.clojure/core.async "0.3.442" :exclusions [org.clojure/tools.reader]]
                 [org.clojure/tools.cli "0.3.5"]
                 [prismatic/schema "1.1.3"]
                 [clojure-miniprofiler "0.4.0"]
                 [jarohen/chime "0.1.6"]
                 [org.clojure/data.priority-map "0.0.5"]
                 [swiss-arrows "1.0.0"]
                 [riddley "0.1.10"]

                 ;;Logging
                 [org.clojure/tools.logging "0.2.6"]
                 [clj-logging-config "1.9.10"
                  :exclusions [log4j]]
                 [com.draines/postal "1.11.0"
                  :exclusions [commons-codec]]
                 [prismatic/plumbing "0.5.3"]
                 [log4j "1.2.17"]
                 [instaparse "1.4.0"]
                 [org.codehaus.jsr166-mirror/jsr166y "1.7.0"]
                 [clj-pid "0.1.1"]
                 [jarohen/chime "0.1.6"]

                 ;;Networking
                 [clj-http "2.0.0"]
                 [io.netty/netty "3.10.1.Final"]
                 [cc.qbits/jet "0.6.4" :exclusions [org.eclipse.jetty/jetty-io
                                                    org.eclipse.jetty/jetty-security
                                                    org.eclipse.jetty/jetty-server
                                                    org.eclipse.jetty/jetty-http
                                                    cheshire]]
                 [org.eclipse.jetty/jetty-server "9.2.6.v20141205"]
                 [org.eclipse.jetty/jetty-security "9.2.6.v20141205"]


                 ;;Metrics
                 [metrics-clojure "2.6.1"
                  :exclusions [io.netty/netty org.clojure/clojure]]
                 [metrics-clojure-ring "2.3.0" :exclusions [com.codahale.metrics/metrics-core
                                                            org.clojure/clojure io.netty/netty]]
                 [metrics-clojure-jvm "2.6.1"]
                 [io.dropwizard.metrics/metrics-graphite "3.1.2"]
                 [com.aphyr/metrics3-riemann-reporter "0.4.0"]
                 [riemann-clojure-client "0.4.1"]

                 ;;External system integrations
                 [me.raynes/conch "0.5.2"]
                 [org.clojure/tools.nrepl "0.2.3"]

                 ;;Ring
                 [ring/ring-core "1.4.0"]
                 [ring/ring-devel "1.4.0" :exclusions [org.clojure/tools.namespace]]
                 [compojure "1.4.0"]
                 [metosin/compojure-api "1.1.8"]
                 [hiccup "1.0.5"]
                 [ring/ring-json "0.2.0"]
                 [ring-edn "0.1.0"]
                 [com.duelinmarkers/ring-request-logging "0.2.0"]
                 [liberator "0.15.0"]

                 ;;Databases
                 [org.apache.curator/curator-framework "2.7.1"
                  :exclusions [io.netty/netty]]
                 [org.apache.curator/curator-recipes "2.7.1"
                  :exclusions [org.slf4j/slf4j-log4j12
                               org.slf4j/log4j
                               log4j]]
                 [org.apache.curator/curator-test "2.7.1"]]

  :repositories {"maven2" {:url "https://files.couchbase.com/maven2/"}
                 "sonatype-oss-public" "https://oss.sonatype.org/content/groups/public/"}

  :filespecs [{:type :fn
               :fn (fn [_]
                     {:type :bytes
                      :path "git-log"
                      :bytes (.trim (:out (clojure.java.shell/sh
                                            "git" "rev-parse" "HEAD")))})}
              {:type :fn
               :fn (fn [{:keys [version]}]
                     {:type :bytes
                      :path "version"
                      :bytes version})}]

  :java-source-paths ["java"]

  :profiles
  {:bar
   {:dependencies [[cheshire "5.3.1"]
                   [com.datomic/datomic-free "0.9.5206"
                    :exclusions [org.slf4j/slf4j-api
                                 com.fasterxml.jackson.core/jackson-core
                                 org.slf4j/jcl-over-slf4j
                                 org.slf4j/jul-to-slf4j
                                 org.slf4j/log4j-over-slf4j
                                 org.slf4j/slf4j-nop
                                 joda-time]]
                   [com.netflix.fenzo/fenzo-core "0.10.0"
                    :exclusions [org.apache.mesos/mesos
                                 com.fasterxml.jackson.core/jackson-core
                                 org.slf4j/slf4j-api
                                 org.slf4j/slf4j-simple]]
                   [org.slf4j/slf4j-log4j12 "1.7.12"]
                   [wyegelwe/mesomatic "1.0.1-r0-SNAPSHOT"]]}

   :uberjar
   {:aot [cook.components]}

   :dev
   {:dependencies [[clj-http-fake "1.0.1"]
                   [criterium "0.4.4"]
                   [org.mockito/mockito-core "1.10.19"]
                   [twosigma/cook-jobclient "0.1.2-SNAPSHOT"]
                   [org.clojure/test.check "0.6.1"]
                   [log4j/log4j "1.2.17" :exclusions [javax.mail/mail
                                                      javax.jms/jms
                                                      com.sun.jdmk/jmxtools
                                                      com.sun.jmx/jmxri]]
                   [ring/ring-jetty-adapter "1.5.0"]]
    :jvm-opts ["-Xms2G"
               "-XX:-OmitStackTraceInFastThrow"
               "-Xmx2G"
               "-Dcom.sun.management.jmxremote.authenticate=false"
               "-Dcom.sun.management.jmxremote.ssl=false"]
    :resource-paths ["test-resources"]
    :source-paths []}

   :test-console
   {:jvm-opts ["-Dcook.test.logging.console"]}

   :docker
   ; avoid calling javac in docker
   ; (.java sources are only used for unit test support)
   {:java-source-paths ^:replace []}}

  :aliases {"deps" ["with-profile" "+bar" "deps"]
            "jar" ["with-profile" "+bar" "jar"]
            "run" ["with-profile" "+bar" "run"]
            "test" ["with-profile" "+bar" "test"]
            "uberjar" ["with-profile" "+bar" "uberjar"]}

  :plugins [[lein-print "0.1.0"]]

  :test-selectors {:all (constantly true)
                   :all-but-benchmark (complement :benchmark)
                   :benchmark :benchmark
                   :default (complement #(or (:integration %) (:benchmark %)))
                   :integration :integration}

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
             "-XX:GCLogFileSize=128M"
             "-XX:+PrintGCDateStamps"
             "-XX:+HeapDumpOnOutOfMemoryError"])
