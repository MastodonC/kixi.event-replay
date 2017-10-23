(ns kixi.event-replay.kinesis-send-events
  (:require [amazonica.aws.kinesis :as kinesis]))

(defn send-event
  [{{:keys [kixi.event-replay/endpoint
            kixi.event-replay/region-name
            kixi.event-replay/target-stream]}
    :kixi.event-replay/kinesis
    :as config}
   {:keys [event partition-key sequence-num]
    :as event}]
  (kinesis/put-record {:endpoint endpoint
                       :region region-name}
                      target-stream
                      event
                      partition-key
                      sequence-num))

(defn send-batch
  [{{:keys [kixi.event-replay/endpoint
            kixi.event-replay/region-name
            kixi.event-replay/target-stream]}
    :kixi.event-replay/kinesis
    :as config}
   batch]
  (kinesis/put-records {:endpoint endpoint
                        :region region-name}
                       target-stream
                       batch))

(defn rename-event->data
  [event]
  (-> event
      (assoc :data (:event event))
      (dissoc :event)))

(defn send-event-batches
  [{{:keys [kixi.event-replay/batch-size]}
    :kixi.event-replay/kinesis
    :as config}]
  (comp
   (map rename-event->data)
   (partition-all batch-size)
   (map (partial send-batch config))))
