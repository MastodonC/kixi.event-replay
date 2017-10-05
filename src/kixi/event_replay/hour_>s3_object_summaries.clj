(ns kixi.event-replay.hour->s3-object-summaries
  (:require [amazonica.aws.s3 :as s3]
            [clj-time.core :as t]))


(defn hour->s3-prefix
  [hour]
  (->> [(t/year hour)
        (t/month hour)
        (t/day hour)
        (t/hour hour)]
       (map str)
       (map #(if (= 1 (count %))
               (str "0" %)
               %))
       (interpose "/")
       (apply str)))

(def max-objects 20)

(defn hour->s3-object-summaries
  ([{:keys [s3-base-dir] :as config}
    hour]
   (hour->s3-object-summaries (:s3-base-dir @config)
                              (hour->s3-prefix hour)
                              nil))
  ([s3-base-dir prefix marker]
   (let [list-objects-res (s3/list-objects (merge {:bucket-name s3-base-dir
                                                   :prefix prefix
                                                   :max-keys max-objects}
                                                  (when marker
                                                    {:marker marker})))]
     (concat (:object-summaries list-objects-res)
             (when (:next-marker list-objects-res)
               (hour->s3-object-summaries s3-base-dir prefix (:next-marker list-objects-res)))))))
