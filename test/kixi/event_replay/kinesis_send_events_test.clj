(ns kixi.event-replay.kinesis-send-events-test
  (:require [amazonica.aws.kinesis :as kinesis]
            [clojure.test :refer :all]
            [environ.core :refer [env]]
            [kixi.event-replay.kinesis-send-events :as sut]))

(def test-stream "test-kixi.event-replay")
(def test-kinesis-endpoint (or (env :kinesis-endpoint) "kinesis.eu-central-1.amazonaws.com"))
(def test-region (or (env :region) "eu-central-1"))

(def kinesis-conn {:kinesis {:endpoint test-kinesis-endpoint
                             :region test-region
                             :target-stream test-stream}})

(def conn {:region test-region
           :endpoint test-kinesis-endpoint})

(defn delete-streams!
  [conn streams]
  (doseq [stream-name streams]
    (kinesis/delete-stream conn stream-name))
  (doseq [stream-name streams]
    (loop []
      (println "Waiting for" stream-name " to be deleted")
      (when (sut/stream-exists conn stream-name)
        (Thread/sleep 100)
        (recur)))))

(defn cleanup-stream
  [conn all-tests]
  (try
    (all-tests)
    (finally
      (delete-streams! conn [test-stream]))))

(use-fixtures :once (partial cleanup-stream conn))

(defn make-event
  []
  {:event {:string "event"}
   :partition-key "12345"
   :sequence-num "6789"})

(deftest send-event-test
  (let [event (make-event)
        _ (sut/ensure-stream kinesis-conn)
        resp (sut/send-event kinesis-conn event)]
    (is (string? (:sequence-number resp)))
    (is (= "shardId-000000000000"
           (:shard-id resp)))))

(deftest send-event-batches-test
  (let [events (repeatedly 20  make-event)
                _ (sut/ensure-stream kinesis-conn)
        resp (into []
                   (sut/send-event-batches (assoc-in kinesis-conn
                                                     [:kinesis
                                                      :batch-size]
                                                     10))
                   events)]
    (is (= 2
           (count resp)))
    (is (= 10
           (count (:records (first resp)))))
    (is (= 10
           (count (:records (second resp)))))))
