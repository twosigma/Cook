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
(ns cook.riemann
  (:require [riemann.client :as riemann]
            [cook.util]
            [clojure.tools.logging :as log]))

;;TODO rewrite this to be functional; lol it sucks

(def riemann-host
  (System/getProperty "cook.riemann"))

(def riemann (delay (riemann/udp-client :host riemann-host)))

(defn riemann-send
  "Takes some events, and sends them over the riemann connection!"
  [& events]
  (riemann/send-events @riemann events false))
