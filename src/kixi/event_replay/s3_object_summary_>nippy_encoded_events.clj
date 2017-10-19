(ns kixi.event-replay.s3-object-summary->nippy-encoded-events
  (:require [amazonica.aws.s3 :as s3]
            [baldr.core :as baldr])
  (:import [java.io InputStream]))

(defn s3-object-summary->nippy-encoded-events
  "Eagerly consumes objects contents, through baldr-seq"
  [{:keys [s3-base-dir]
    :as config}
   s3-object-summary]
  (let [s3-object (s3/get-object :bucket-name s3-base-dir
                                 :key (:key s3-object-summary))]
    (with-open [^InputStream in (:object-content s3-object)]
      (doall
       (baldr/baldr-seq in)))))
