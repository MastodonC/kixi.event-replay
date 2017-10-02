(ns kixi.event-replay.inputstream-to-events
  (:require [gloss.io :as gloss.io]
            [taoensso.nippy :as nippy])
  (:import [java.nio ByteBuffer]
           [java.io InputStream]))

(def line-break 10)

(def decode-buffer-size (int 1024))

(defn input-stream->lines
  "Splits input stream into byte buffers by line-breaks or decode-buffer-size."
  ([^InputStream rdr]
   (input-stream->lines rdr (ByteBuffer/allocate decode-buffer-size)))
  ([^InputStream rdr 
    ^ByteBuffer buffer]
   (if-not (.hasRemaining buffer)
     (cons buffer
           (lazy-seq (input-stream->lines rdr)))     
     (loop [b (.read rdr)]
       (when-not (= -1 b)
         (.put buffer (byte b))
         (if (or (= line-break b)
                 (not (.hasRemaining buffer)))
           (cons buffer
                 (lazy-seq (input-stream->lines rdr)))
           (recur (.read rdr))))))))

(def nippy-head-sig 
  "First 3 bytes of Nippy header" 
  (.getBytes "NPY" "UTF-8"))

(def nippy-version-1 (byte 0))

(defn npy-headed-buffer?
  "True if first bytes in buffer match sig"
  [^ByteBuffer buffer]
  (let [^bytes nhs nippy-head-sig]
    (and (= (aget nhs 0)
            (.get buffer 0))
         (= (aget nhs 1)
            (.get buffer 1))
         (= (aget nhs 2)
            (.get buffer 2))
         (= nippy-version-1
            (.get buffer 3)))))

(defn ^ByteBuffer rewind-buffer
  [^ByteBuffer b]
  (.rewind b))

(defn trim-to-data
  "Creates a new buffer limited to that portion of the parent that contains data"
  [^ByteBuffer buffer]
  (let [position (.position buffer)]
    (-> buffer (rewind-buffer) (.slice) (.limit position))))

(defn partition-into-nippy-sequence
  "Partitions seq into vectors containing all parts of an event"
  [xf]
  (let [a (java.util.ArrayList.)]
    (fn
      ([] (xf))
      ([acc]
       (let [complete-seq (vec (.toArray a))]
         (.clear a)
         (->> complete-seq
              (xf acc)
              xf)))
      ([acc buffer]
       (if (or (.isEmpty a)
               (not (npy-headed-buffer? buffer)))
         (do
           (.add a buffer)
           acc)
         (let [complete-seq (vec (.toArray a))]
           (.clear a)
           (.add a buffer)
           (xf acc complete-seq)))))))

(defn combine-nippy-sequence
  [nippy-seq]
  (gloss.io/contiguous (map rewind-buffer nippy-seq)))

(defn buffer->event
  [^java.nio.ByteBuffer barray]
  (nippy/thaw (.array barray)))

(def input-stream->events
  (comp
   (mapcat input-stream->lines)
   (map trim-to-data)
   partition-into-nippy-sequence
   (map combine-nippy-sequence)
   (map buffer->event)))
