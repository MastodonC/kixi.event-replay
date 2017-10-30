(ns kixi.event-replay.kinesis-send-events
  (:require [amazonica.aws.kinesis :as kinesis]))

(defn stream-exists
  [conn stream]
  (try
    (kinesis/describe-stream conn stream)
    true
    (catch Exception e
      false)))

(defn get-stream-status
  [conn stream-name]
  (get-in (kinesis/describe-stream conn stream-name)
          [:stream-description :stream-status]))

(defn create-stream!
  [conn stream-name]
  (let [shards 1]
    (println "Creating stream" stream-name "with" shards "shard(s)!")
    (kinesis/create-stream conn stream-name shards)
    (loop [n 0
           status (get-stream-status conn stream-name)]
      (when-not (= "ACTIVE" status)
        (if (< n 50)
          (do
            (println "Waiting for" stream-name "status to be ACTIVE:" status)
            (Thread/sleep 500)
            (recur (inc n) (get-stream-status conn stream-name)))
          (throw (Exception. (str "Failed to create stream " stream-name))))))))

(defn ensure-stream
  [{{:keys [endpoint
            region
            target-stream]}
    :kinesis
    :as config}]
  (let [conn {:endpoint endpoint
              :region region}]
    (when-not (stream-exists conn target-stream)
      (create-stream! conn target-stream))))

(defn send-event
  [{{:keys [endpoint
            region
            target-stream]}
    :kinesis
    :as config}
   {:keys [event partition-key sequence-num]
    :as event}]
  (kinesis/put-record {:endpoint endpoint
                       :region region}
                      target-stream
                      event
                      partition-key
                      sequence-num))

(defn send-batch
  [{{:keys [endpoint
            region
            target-stream]}
    :kinesis
    :as config}
   batch]
  (kinesis/put-records {:endpoint endpoint
                        :region region}
                       target-stream
                       batch))

(defn rename-event->data
  [event]
  (-> event
      (assoc :data (:event event))
      (dissoc :event)))

(defn wrap-byte-buffers
  [event]
  (update event
          :data
          #(java.nio.ByteBuffer/wrap %)))

(defn send-event-batches
  [{{:keys [batch-size]}
    :kinesis
    :as config}]
  (comp
   (map rename-event->data)
   (map wrap-byte-buffers)
   (partition-all batch-size)
   (map (partial send-batch config))))
