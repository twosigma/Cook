;; This project file exists to make it easier to develop the the job client 
;; and cook scheduler together. It should be kept in sync with the pom.xml

(defproject twosigma/cook-jobclient "0.1.1-SNAPSHOT"
  :description "A Java API for connecting to the Cook scheduler, submitting, and monitoring jobs."
  :license {:name "Apache License, Version 2.0"}
  :dependencies [[com.google.guava/guava "14.0.1"]
                 [org.json/json "20140107"]
                 [org.apache.httpcomponents/httpclient "4.3.2"]
                 [log4j/log4j "1.2.17"]
                 [junit/junit "4.12"]
                 [org.jmockit/jmockit "1.18"]
                 [commons-io/commons-io "2.4"]]
  :java-source-paths ["src/main/java"]
  :javac-options ["-target" "1.7" "-source" "1.7"])
