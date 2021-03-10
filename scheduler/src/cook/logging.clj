(ns cook.logging
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.tools.logging :as ctl]))

(defmacro logp
  [log-fn & args]
  `(~log-fn
     (json/write-str
       (if (map? ~(last args))
         (assoc ~(last args) :msg ~(str/join " " (butlast args)))
         {:msg ~(str/join " " args)}))))

(defmacro trace
  [& args]
  `(logp ctl/trace ~@args))

(defmacro debug
  [& args]
  `(logp ctl/debug ~@args))

(defmacro info
  [& args]
  `(logp ctl/info ~@args))

(defmacro warn
  [& args]
  `(logp ctl/warn ~@args))

(defmacro error
  [& args]
  `(logp ctl/error ~@args))

(defmacro fatal
  [& args]
  `(logp ctl/fatal ~@args))
