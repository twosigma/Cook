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
(ns cook.util
  (:refer-clojure :exclude [cast merge empty split replace])
  (:require [clojure.java.io :as io :refer [reader file]]
            [postal.core :as postal]
            [clojure.data.json :as json]
            [instaparse.core :as insta]
            [postal.core :as postal]
            [clojure.tools.logging :as log]
            [clj-http.client :as client]
            [clojure.core.memoize :as memo]
            [clj-time.core :as time])
  (:use [clojure.java.shell :only [sh]]
        [clojure.string :only [join split replace]]
        [clojure.tools.namespace.dependency]
        [clojure.pprint :only [pp pprint]]
        [clojure.tools.macro :as macro]
        [clojure-miniprofiler :as miniprofiler]))

(def deployment-users #{"tsram"})

(defmacro assert-not-null
  "Simple macro to assert that the argument is not null, and if it is,
  it throws an exception that includes the argument in the message."
  [form]
  `(when (nil? ~form)
     (throw (ex-info (str '~form " cannot be null") {}))))

(defn add-duration-to-findate
  "Takes a duration from `parse-duration` and a FinDate
   and adds the duration to the FinDate."
  [findate duration]
  (reduce (fn [findate [field amount]]
            (.addDays findate field amount))
          findate
          duration))

(defn subtract-duration-from-findate
  "Takes a duration from `parse-duration` and a FinDate
   and adds the duration to the FinDate."
  [findate duration]
  (reduce (fn [findate [field amount]]
            (.addDays findate field (- amount)))
          findate
          duration))

(defmacro try-timeout
  "Evaluates an expression in a separate thread and kills it if it takes too long"
  [millis failure & body]
  `(let [result# (promise)
         except# (promise)
         action-fn#
         (fn []
           (deliver result#
                    (try ~@body
                         (catch Exception e#
                           (deliver except# e#)))))
         action-thread# (Thread. action-fn#)]
    (.start action-thread#)
    (.join action-thread# ~millis)
    (cond (realized? except#) (throw @except#)
          (realized? result#) @result#
          :else
          (do (.stop action-thread#)
              ~failure))))

(defonce thread-counter (java.util.concurrent.atomic.AtomicLong.))

(defmacro thread
  "Runs the body in a new thread"
  [& body]
  `(.start (Thread. (fn* [] ~@body)
                    (str "cook-util-"
                         (.getAndIncrement
                           ^java.util.concurrent.atomic.AtomicLong thread-counter)))))

(defn quantile
  [coll]
  (let [s-coll (sort coll)
        n (count s-coll)]
    (if (zero? n)
      [0 0 0 0 0]
      [(first s-coll)
       (nth s-coll (int  (/ n 4)))
       (nth s-coll (int  (/ n 2)))
       (nth s-coll (int  (* 3 (/ n 4))))
       (last s-coll)])))

(defn min-max
  [coll]
  (let [s-coll (sort coll)]
    [(first s-coll)
     (last s-coll)]))

(defn get-app-locator-impl
  [marathon timeout appid]
  (try
    (let [tasks-url (clojure.string/join "/" [marathon "v2" "apps" appid "tasks"])
          response (client/get tasks-url {:as :json
                                          :spnego-auth true
                                          :socket-timeout timeout
                                          :conn-timeout timeout})
          tasks-data (:body response)
          task (rand-nth (:tasks tasks-data))]
      [(:host task) (:ports task)])
    (catch clojure.lang.ExceptionInfo e
      (if (= 404 (get-in (.getData e) [:object :status]))
        (throw (ex-info "App not found." {:appid appid :noemail true}))
        (throw e)))))

(defn get-app-locator
  "Given a marathon uri, returns an app locator function that:

   Queries the marathon tasks endpoint for the given appid, chooses a random task,
   returns the host the task is running on and the port(s) the task is using.

   The GET request is memoized with a ttl of 10s to reduce the burden on Marathon when
   we have many submissions.

   Optionally takes a timeout to use when contacting the given Marathon, default
   timeout is 1 minute

   Returns [host [& ports]]"
  [marathon & {:keys [timeout] :or {timeout (* 1000 60)}}]
  (memo/ttl
    (partial get-app-locator-impl marathon timeout)
    :ttl/threshold 10000))

(defmacro deftraced
  "Creates a miniprofiler-traced version of a function.  Use like defn
  to wrap your function in clojure-miniprofiler/trace with the
  function's namespace and name as the trace name.

  Optionally, you can add the metadata key :untraced to provide a name
  for the untraced version of the function, if you need to use that in
  some contexts.  By default, the untraced version is not bound to
  anything public.

  Optionally, you can provide in the metadata
  map :custom-timing [call-type execute-type command-string-fn].  The
  call-type and execute-type should be strings, command-string-fn will
  be called with the created function's arguments and should return a
  string.  These three values will be provided to
  clojure-miniprofiler/custom-timing instead of using
  clojure-miniprofiler/trace (which is the default behavior).

  Example:

      (deftraced ^{:untraced foo-bare}
        foo
        ([x] (+ x 2))
        ([x y] (* x y)))

  is roughly equivalent to:

      (defn foo-bare
        ([x] (+ x 2))
        ([x y] (* x y)))
      (defn foo [& args]
        (clojure-miniprofiler/trace
         \"my-ns/foo\"
         (apply foo-bare args)))

  Example:

      (deftraced ^{:custom-timing [\"datomic\" \"query\" #(str %2)]}
        run-db-query
        [db query]
        (q query db))

  is roughly equivalent to:

      (defn run-db-query
        [db query]
        (clojure-miniprofiler/custom-timing
         \"datomic\" \"query\" (str query)
         (q query db)))
  "
  [symb & args]
  (let [[symb body] (macro/name-with-attributes symb args)
        single-arity? (vector? (first body))
        ;; Handle possibly single-arity case, this is a degenerate
        ;; case of multiple-arity.
        arity-forms (if single-arity? (list body) body)
        max-fixed-arity (->> arity-forms
                             (map first)
                             (filter #(not-any? (partial = '&) %))
                             (map count)
                             (reduce max 0))
        [call-type execute-type command-string-fn] (:custom-timing (meta symb))
        untraced-gensym (gensym "deftraced-untraced-fn")]
    `(let [~untraced-gensym (fn ~@body)]
       ;; def the untraced symbol name, with most of the metadata we want.
       ~(when-let [untraced-symbol (:untraced (meta symb))]
          `(def ~(with-meta untraced-symbol
                   (dissoc (meta symb) :untraced :custom-timing))
             ~untraced-gensym))

       ;; Create the actual symbol.  We map each arity to a body
       ;; which will execute untraced-internal inside a
       ;; miniprofiler block.
       (defn ~symb
         ~@(map
            (fn [arity]
              (let [arglist (first arity)
                    trace-name (apply str
                                      (concat
                                       (list *ns* "/" symb)
                                       (when-not single-arity?
                                         (list "-arity_" (count arity)))))]
                (if (some (partial = '&) arglist)
                  ;; Binding forms with & must be passed through
                  ;; with apply, no way around it.
                  (let [fixed-args (vec (repeatedly max-fixed-arity gensym))
                        rest-sym (gensym "rest")
                        variadic-arglist (conj fixed-args '& rest-sym)]
                    `(~variadic-arglist
                      ~(if call-type
                         `(miniprofiler/custom-timing
                           ~call-type ~execute-type (apply ~command-string-fn ~@fixed-args ~rest-sym)
                           (apply ~untraced-gensym ~@fixed-args ~rest-sym))
                         `(miniprofiler/trace
                           ~trace-name
                           (apply ~untraced-gensym ~@fixed-args ~rest-sym)))))
                  (let [arglist' (repeatedly (count arglist) gensym)]
                    ;; Because a form in arglist may destructure,
                    ;; we shouldn't use it directly.  Instead,
                    ;; gensym a name for it here, and let the
                    ;; inner function do the destructuring.
                    ;; arglist' is simply enough gensyms for this
                    ;; arity.
                    `(~(vec arglist')
                      ~(if call-type
                         `(miniprofiler/custom-timing
                           ~call-type ~execute-type (~command-string-fn ~@arglist')
                           (~untraced-gensym ~@arglist'))
                         `(miniprofiler/trace
                           ~trace-name
                           (~untraced-gensym ~@arglist'))))))))
            arity-forms)))))

(defn exponential-seq
  "Return a lazy-seq sequence of
   '(scalar scalar*base scalar*base^2 scalar*base^3 ...)"
  [scalar base]
  (cons scalar (lazy-seq (exponential-seq (* scalar base) base))))

(defn rand-exponential-seq
  "Return a randon exponential sequence of lengh n."
  ([n]
   (rand-exponential-seq n 1 2))
  ([n scalar]
   (rand-exponential-seq n scalar 2))
  ([n scalar base]
   (mapv rand-int (take n (exponential-seq scalar base)))))

(comment
  (println (rand-exponential-seq 10 1000))
  (println (rand-exponential-seq 5 60))
 )

(def version
  (try (slurp (io/resource "git-log"))
       (catch Exception e
         "dev")))

(defn get-html-stacktrace
  "Returns a string representation of the exception. The 3 argument form fleshes
   out information about the thread, user, and host."
  ([exception thread]
   (str "thread name: " (.getName thread)
        "\n\n user: " (System/getProperty "user.name")
        "\n\n host: " (.getHostName (java.net.InetAddress/getLocalHost))
        "\n\n commit: " version
        "\n\n" (get-html-stacktrace exception)))
  ([exception]
   (str exception "\n"
        (->> (.getStackTrace exception)
             (map #(str "  " (.getClassName %) "/" (.getMethodName %) "(" (.getFileName %) ":" (.getLineNumber %) ")"))
             (join "\n"))
        (when-let [cause (.getCause exception)]
          (str "\nCaused by:" (get-html-stacktrace cause))))))

(defn install-email-on-exception-handler
  "When an exception isn't caught anywhere, this installs a global handler.
   It will log the exception at the given log-level (:error is recommended),
   and it will send an email to the address specified by the email-config.
   
   email-config comes from the postal library. Usually, you'll specify
   a map like:
   {:to [\"admin@example.com\"]
    :from \"cook@example.com\"
    :subject \"unhandled exception in cook\"}"
  [log-level email-config]
  (Thread/setDefaultUncaughtExceptionHandler
    (reify Thread$UncaughtExceptionHandler
      (uncaughtException [_ thread e]
        (log/logp log-level e "Uncaught exception on thread" (.getName thread))
        (when email-config
          (let [stacktrace (get-html-stacktrace e)]
            (postal/send-message (-> email-config
                                     (update-in [:subject] str " on thread " (.getName thread))
                                     (assoc :body stacktrace)))))))))
