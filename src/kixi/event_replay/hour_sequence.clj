(ns kixi.event-replay.hour-sequence
  (:require [clj-time.core :as t]
            [clj-time.periodic :as p]))

(def one-hour (t/hours 1))

(defn hour-sequence
  [{:keys [start-datehour end-datehour]
    :as config}]
  (p/periodic-seq start-datehour
                  end-datehour
                  one-hour))
