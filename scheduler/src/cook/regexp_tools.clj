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
(ns cook.regexp-tools
  (:require [clojure.tools.logging :as log]))

(defn match-based-on-regexp
  "Given a list of dictionaries [{:<regexp-name> <regexp> :<field-name> <field>} {:<regexp-name> <regexp> :<field-name> <field>} ...], match-list,
   a key <field-name> and <regexp-name> name, return the first matching <field> where the <regexp> matches the key."
  [regexp-name field-name match-list key]
  (try
    (-> match-list
        (->> (filter (fn [map]
                       (let [regexp (get map regexp-name)
                             pattern (re-pattern regexp)]
                         (re-find pattern key)))))
        first
        (get field-name))
    (catch Exception e
      (throw (ex-info "Failed matching key" {:regexp-name regexp-name :field-name field-name :match-list match-list :key key} e)))))

(defn match-based-on-pool-name
  "Given a list of dictionaries [{:pool-regexp .. :field ...} {:pool-regexp .. :field ...}
   a pool name and a <field> name, return the first matching <field> where the regexp matches the pool name."
  [match-list effective-pool-name field & {:keys [default-value] :or {default-value nil}}]
  (let [value (match-based-on-regexp
                :pool-regex
                field
                match-list
                effective-pool-name)]
    (if (some? value)
      value
      default-value)))
