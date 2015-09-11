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

(ns cook-agent.common.runner
  (:require [clj-time.core :as time]
            [clojure.java.io :as io]
            [clojure.java.shell]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async]
            [cook-agent.retry :as retry]
            [me.raynes.conch.low-level :as sh]
            [cook-agent.util :as util]))

(defn- mkfifo
  "Makes a new, randomly named fifo.

   Returns the name of the fifo."
  []
  (let [tmppipe (-> (clojure.java.shell/sh "mktemp" "-u") :out .trim)
        fifo (clojure.java.shell/sh "mkfifo" "-m" "600" tmppipe)]
    (when-not (zero? (:exit fifo))
      (throw (ex-info "mkfifo failed" fifo)))
    tmppipe))

(defn exited?
  "Returns true if the given conch process exited."
  [proc]
  (try
    (.exitValue (:process proc))
    true
    (catch IllegalThreadStateException _
      false)))

(defn match-progress-update
  "Returns the pair [progress message] if the given string is a
   simulation progress update. Returns nil otherwise."
  [s]
  (when-let [[_ percent progress] (re-matches #"\^\^\^\^JOB-PROGRESS: (\d*)(?: )?(.*)" s)]
    (try
      [(Long/parseLong percent) progress]
      (catch NumberFormatException e
        ;;Ignore
        ))))

(defn run-job
  "Runs a job with the jobsystem interceptor wrapper."
  [{:keys [local-dir commands ipc-sink]}]
  (log/info "RUN-JOB")
  (log/info "local-dir" local-dir)
  (log/info "commands" commands)
  (let [commands (if-not (vector? commands) (vec commands) commands)
        fifo (mkfifo)
        commands (conj commands
                       :env {"JOBAGENT_IPC_FILENAME" fifo
                             "JOBAGENT_PROGRESS" "true"
                             "HOME" local-dir
                             "KRB5CCNAME" (System/getenv "KRB5CCNAME")
                             "PATH" "/usr/local/sbin:/usr/local/bin:/usr/bin:/usr/sbin:/sbin:/bin"
                             "USER" (or (System/getenv "USER") (System/getProperty "user.name"))}
                       :clear-env true
                       :verbose :very)
        ;; temporarily set oom_score_adj to 500 to be inherited by the process
        ;;  created via sh/proc
        ;; reset the original value after the process has been forked
        ;; this will bias the cgroup oom killer to not kill the cook-agent itself
        ;; possibly racy in the case that the subprocess oom's immediately at for
        ;; but that is an unlikely case in our environment
        old-oom (clojure.string/trim-newline (slurp "/proc/self/oom_score_adj"))
        _ (spit "/proc/self/oom_score_adj" "500")
        p (apply sh/proc commands)
        _ (spit "/proc/self/oom_score_adj" old-oom)]
    ;; Thread for sending status update.
    (async/thread
      (try
        (with-open [reader (io/reader fifo)]
          (binding [*in* reader]
            (let [prior-percent (atom nil)]
              (while (not (exited? p))
                (try
                  (let [line (read-line)]
                    ;; Only send the progress update when the percentage is changed.
                    ;; This will significantly reduce the number of non-significant progress
                    ;; updates for the server (from thousands to at most 12).
                    (when-let [[percent progress] (and line (match-progress-update line))]
                      (when (and (not= percent @prior-percent)
                                 (contains? #{0 1 5 10 30 60 80 90 95 98 99 100} percent))
                        (log/info (format "percent %s progress %s" percent progress))
                        (ipc-sink {:line line
                                   :percent percent
                                   :progress progress})
                        (reset! prior-percent percent))))
                  (catch Exception e
                    (log/error e "Fail to send progress update but will continue...")
                    (Thread/sleep 5000)))))))
        (catch Exception e
          (log/error e "Failed to send status update!"))))
    ;; Thread for logging process output.
    (async/thread
      (try
        (doseq [line (line-seq (io/reader (:out p)))]
          (log/info line)
          (.println System/out line))
        (catch Exception e
          (log/error e "Failed to log process stdout!"))))
    ;; Thread for logging process error.
    (async/thread
      (try
        (doseq [line (line-seq (io/reader (:err p)))]
          (when-let [[percent progress] (match-progress-update line)]
            (println "Percent, progress: " percent progress))
          (log/error line)
          (.println System/err line))
        (catch Exception e
          (log/error e "Failed to log process stderr!"))))
    p))
