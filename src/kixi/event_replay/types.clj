(ns kixi.event-replay.types
  (:require [clojure.spec.alpha :as s]
            [clj-time.core :as t]
            [clj-time.format :as f]
            [clj-time.periodic :as p]))


(def datehour-formatter
  (f/formatter :date-hour))

(def parse-datehour
  (partial f/parse datehour-formatter))

(def unparse-datehour
  (partial f/unparse datehour-formatter))

(defn datehour?
  [x]
  (if (instance? org.joda.time.DateTime x)
    x
    (try
      (if (string? x)
        (parse-datehour x)
        :clojure.spec.alpha/invalid)
      (catch IllegalArgumentException e
        :clojure.spec.alpha/invalid))))

(def datehour
  (s/conformer datehour? identity))

(defn str-integer?
  [x]
  (if (string? x)
    (try
      (Integer/parseInt x)
      (catch NumberFormatException e
        :clojure.spec.alpha/invalid))
    (if (integer? x)
      x
      :clojure.spec.alpha/invalid)))

(def str-integer
  (s/conformer str-integer?
               identity))
