(ns kixi.event-replay.hour-sequence
  (:require [clj-time.core :as t]
            [clj-time.periodic :as p]
                        [kixi.event-replay.types
             :refer
             [datehour parse-datehour unparse-datehour]]))


(def one-hour (t/hours 1))

(defn hour-sequence
  [{:keys [start-datehour end-datehour]
    :as config
    :or {end-datehour (unparse-datehour (t/now))}}]
  (let [start (parse-datehour start-datehour)
        end (parse-datehour end-datehour)]
    (p/periodic-seq start
                    end
                    one-hour)))
