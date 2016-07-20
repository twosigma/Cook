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
(ns cook.global-state
  "Quarantine area for the app's global state. This should be kept as minimal as possible. 
   Prefer functional solutions where practical.

   Currently, we retain the parsed config file and references to the
   various Java objects we use in a single top-level Ref that's initialized in cook.components.
  
   The ref itself isn't in cook.components so that we can reference it
   from other namespaces that are required in cook.components without
   creating dependency cycles.")


(defonce global-state (ref nil))
