(ns kixi.event-replay.event-replay-test
  (:require  [clojure.test :refer :all]
             [clojure.java.io :as io]
             [kixi.event-replay.hour->s3-object-summaries :as os]
             [amazonica.aws.s3 :as s3]
             [kixi.event-replay :as sut]
             [kixi.event-replay.kinesis-send-events :as ke]))

(def test-file "./test-resources/staging-test-event-file")

(deftest mock-event-replay-test
  (let [sent-events (atom [])]
    (with-redefs [ke/ensure-stream (fn [_] true)
                  os/hour->s3-object-summaries (fn [_ _]
                                                 [{:na "na"}])
                  s3/get-object (fn [& _]
                                  {:object-content (io/input-stream test-file)})
                  ke/send-batch (fn [_ batch]
                                  (swap! sent-events concat batch))]
      (sut/execute (second (sut/validate-config {:start-datehour "2017-09-01T01"
                                                 :end-datehour "2017-09-01T02"
                                                 :s3-base-dir "na-log"
                                                 :kinesis {:target-stream "na"
                                                           :region "na"
                                                           :endpoint "na"
                                                           :batch-size 10}})))
      (is (= 17
             (count @sent-events))))))
