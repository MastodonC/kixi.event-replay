(ns kixi.event-replay.s3-object-summary->nippy-encoded-events
  (:require [amazonica.aws.s3 :as s3]))

(defn s3-object-summary->nippy-encoded-events
  [{:keys [s3-base-dir]
    :as config}
   s3-object-summary]
  (let [s3-object (s3/get-object :bucket-name s3-base-dir
                                 :key (:key s3-object-summary))]
    ;;TODO parse whole object through baldr-seq, doall and close object stream
    ))
