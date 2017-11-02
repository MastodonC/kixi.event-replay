(ns kixi.event-replay
  (:gen-class)
  (:require [aero.core :as aero]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [clojure.pprint :refer [pprint]]
            [kixi.event-replay.ensure-event-order :refer [time-windowed-ordering exception-on-out-of-order-event]]
            [kixi.event-replay.hour->s3-object-summaries
             :refer
             [hour->s3-object-summaries]]
            [kixi.event-replay.hour-sequence :refer [hour-sequence]]
            [kixi.event-replay.s3-object-summary->nippy-encoded-events
             :refer
             [s3-object-summary->nippy-encoded-events]]
            [kixi.event-replay.types :refer [datehour str-integer]]
            [kixi.event-replay.kinesis-send-events :refer [send-event-batches ensure-stream]]
            [taoensso.nippy :as nippy]))

(s/def ::start-datehour datehour)
(s/def ::end-datehour
  (s/or :t datehour
        :n nil?))

(s/def ::target-stream
  (s/and string?
         #(not (#{"prod-witan-event"
                  "staging-witan-event"} %))))
(s/def ::endpoint string?)
(s/def ::region string?)
(s/def ::batch-size str-integer)

(s/def ::kinesis
  (s/keys :req-un [::target-stream
                   ::endpoint
                   ::batch-size
                   ::region]))

(s/def ::base-dir
  (s/and string?
         #(string/ends-with? % "-log")))

(s/def ::s3
  (s/keys :req-un [::base-dir
                   ::region]))

(s/def ::config
  (s/keys :req-un [::start-datehour
                   ::end-datehour
                   ::kinesis
                   ::s3]))

(defn validate-config
  [config]
  (if (s/valid? ::config config)
    [true (s/unform ::config (s/conform ::config config))]
    [false (s/explain-data ::config config)]))


(defn replay-events
  [config]
  (comp (mapcat (partial hour->s3-object-summaries config))
        (mapcat (partial s3-object-summary->nippy-encoded-events config))
        (map nippy/thaw)
        time-windowed-ordering
        exception-on-out-of-order-event
        (send-event-batches config)))

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
  (println "Configuration: ")
  (pprint config)
  (ensure-stream config)
  (transduce
   (replay-events config)
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
