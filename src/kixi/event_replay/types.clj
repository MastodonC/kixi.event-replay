(ns kixi.event-replay.types
  (:require [clojure.spec.alpha :as s]
            [clj-time.core :as t]
            [clj-time.format :as f]
            [clj-time.periodic :as p]))


(def datehour-formatter
  (f/formatter :date-hour))

(def parse-datahour
  (partial f/parse datehour-formatter))

(def unparse-datahour
  (partial f/unparse datehour-formatter))

(defn datehour?
  [x]
  (if (instance? org.joda.time.DateTime x)
    x
    (try
      (if (string? x)
        (parse-datahour x)
        :clojure.spec.alpha/invalid)
      (catch IllegalArgumentException e
        :clojure.spec.alpha/invalid))))

(def datehour
  (s/conformer datehour? identity))
