(ns kixi.event-replay
  (:gen-class)
  (:require [aero.core :as aero]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [kixi.event-replay.ensure-event-order :refer [exception-on-out-of-order-event]]
            [kixi.event-replay.hour->s3-object-summaries
             :refer
             [hour->s3-object-summaries]]
            [kixi.event-replay.hour-sequence :refer [hour-sequence]]
            [kixi.event-replay.s3-object-summary->nippy-encoded-events
             :refer
             [s3-object-summary->nippy-encoded-events]]
            [kixi.event-replay.types :refer [datehour]]
            [taoensso.nippy :as nippy]))

(s/def ::start-datehour datehour)
(s/def ::end-datehour
  (s/or :t datehour
        :n nil?))

(s/def ::target-stream string?)
(s/def ::s3-base-dir
  (s/and string?
         #(string/ends-with? % "-log")))

(s/def ::config
  (s/keys :req-un [::start-datehour
                   ::end-datehour
                   ::target-stream
                   ::s3-base-dir]))

(defn validate-config
  [config]
  (if (s/valid? ::config config)
    [true (s/conform ::config config)]
    [false (s/explain-data ::config config)]))

(defn send-event
  [_]
  :done)

(defn execute-replay
  [config]
  (comp (mapcat (partial hour->s3-object-summaries config))
        ;; TODO ensure file timestamps are contiguous
        (mapcat (partial s3-object-summary->nippy-encoded-events config))
        exception-on-out-of-order-event
        (map nippy/thaw)
        ;; TODO send event
        ))

(defn aggregate-events
  ([] {:errors 0
       :events 0
       :total 0})
  ([acc] acc)
  ([acc x]
   (if (= x :error-decoding)
     (update (update acc :total inc) :errors inc)
     (update (update acc :total inc) :events inc))))

(defn execute
  [config]
  (transduce
   (execute-replay config)
   aggregate-events
   (hour-sequence config)))

(defn exit
  [status msg]
  (println msg)
  (System/exit status))

(defn -main
  [& args]
  (let [[ok? config] ((comp validate-config aero/read-config io/resource) "config.edn")]
    (if ok?
      (exit 0 (execute config))
      (exit 1 config))))
