(ns kixi.event-replay.hour-sequence
  (:require [clj-time.core :as t]
            [clj-time.periodic :as p]
                        [kixi.event-replay.types
             :refer
             [datehour parse-datahour unparse-datahour]]))


(def one-hour (t/hours 1))

(defn hour-sequence
  [{:keys [start-datehour end-datehour]
    :as config
    :or {end-datehour (unparse-datahour (t/now))}}]
  (let [start (parse-datahour start-datehour)
        end (parse-datahour end-datehour)]
    (p/periodic-seq start
                    end
                    one-hour)))
