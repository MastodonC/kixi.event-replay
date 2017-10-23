(ns kixi.event-replay.kinesis-send-events-test
  (:require [amazonica.aws.kinesis :as kinesis]
            [clojure.test :refer :all]
            [environ.core :refer [env]]
            [kixi.event-replay.kinesis-send-events :as sut]))

(def test-stream "test-kixi.event-replay")
(def test-kinesis-endpoint (or (env :kinesis-endpoint) "kinesis.eu-central-1.amazonaws.com"))
(def test-region (or (env :region) "eu-central-1"))

(def kinesis-conn {:kixi.event-replay/kinesis {:kixi.event-replay/endpoint test-kinesis-endpoint
                                               :kixi.event-replay/region-name test-region
                                               :kixi.event-replay/target-stream test-stream}})

(def conn {:region test-region
           :endpoint test-kinesis-endpoint})

(defn list-streams
  [conn]
  (kinesis/list-streams conn))

(defn get-stream-status
  [conn stream-name]
  (get-in (kinesis/describe-stream conn stream-name)
          [:stream-description :stream-status]))

(defn create-streams!
  [conn streams]
  (let [{:keys [stream-names]} (list-streams conn)
        missing-streams (remove (set stream-names) streams)
        shards 1]
    (doseq [stream-name missing-streams]
      (println "Creating stream" stream-name "with" shards "shard(s)!")
      (kinesis/create-stream conn stream-name shards))
    (doseq [stream-name missing-streams]
      (loop [n 0
             status (get-stream-status conn stream-name)]
        (when (not (= "ACTIVE" status))
          (if (< n 50)
            (do
              (println "Waiting for" stream-name "status to be ACTIVE:" status)
              (Thread/sleep 500)
              (recur (inc n) (get-stream-status conn stream-name)))
            (throw (Exception. (str "Failed to create stream " stream-name)))))))))

(defn stream-exists
  [conn stream]
  (try
    (kinesis/describe-stream conn stream)
    true
    (catch Exception e
      false)))

(defn delete-streams!
  [conn streams]
  (doseq [stream-name streams]
    (kinesis/delete-stream conn stream-name))
  (doseq [stream-name streams]
    (loop []
      (println "Waiting for" stream-name " to be deleted")
      (when (stream-exists conn stream-name)
        (Thread/sleep 100)
        (recur)))))

(defn cycle-stream
  [conn all-tests]
  (try
    (create-streams! conn [test-stream])
    (all-tests)
    (finally
      (delete-streams! conn [test-stream]))))

(use-fixtures :once (partial cycle-stream conn))

(defn make-event
  []
  {:event {:string "event"}
   :partition-key "12345"
   :sequence-num "6789"})

(deftest send-event-test
  (let [event (make-event)
        resp (sut/send-event kinesis-conn event)]
    (is (string? (:sequence-number resp)))
    (is (= "shardId-000000000000"
           (:shard-id resp)))))

(deftest send-event-batches-test
  (let [events (repeatedly 20  make-event)
        resp (into []
                   (sut/send-event-batches (assoc-in kinesis-conn
                                                     [:kixi.event-replay/kinesis
                                                      :kixi.event-replay/batch-size]
                                                     10))
                   events)]
    (is (= 2
           (count resp)))
    (is (= 10
           (count (:records (first resp)))))
    (is (= 10
           (count (:records (second resp)))))))
