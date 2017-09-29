(ns kixi.event-replay
  (:gen-class)
  (:require [aero.core :as aero]
            [amazonica.aws.s3 :as s3]
            [clj-time.core :as t]
            [clj-time.format :as f]
            [clj-time.periodic :as p]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [kixi.event-replay.inputstream-to-events :refer [input-stream->events]]))

(def config (atom {}))

(def datehour-formatter
  (f/formatter :date-hour))

(def parse-datahour
  (partial f/parse datehour-formatter))

(def unparse-datahour
  (partial f/unparse datehour-formatter))

(defn datehour?
  [x]
  (if (instance? org.joda.time.DateTime x)
    x
    (try
      (if (string? x)
        (parse-datahour x)
        :clojure.spec.alpha/invalid)
      (catch IllegalArgumentException e
        :clojure.spec.alpha/invalid))))

(def datehour
  (s/conformer datehour? identity))

(s/def ::start-datehour datehour)
(s/def ::end-datehour 
  (s/or :t datehour
        :n nil?))

(s/def ::target-stream string?)
(s/def ::s3-base-dir
  (s/and string?
         #(string/ends-with? % "-backup")))

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

(def one-hour (t/hours 1))

(defn hour-sequence
  []
  (let [{:keys [start-datehour
                end-datehour]} @config
        end-datehour (or end-datehour 
                         (unparse-datahour (t/now)))
        start (parse-datahour start-datehour)
        end (parse-datahour end-datehour)]    
    (p/periodic-seq start
                    (t/plus end
                            one-hour)
                    one-hour)))

(defn hour->s3-prefix
  [hour]
  (->> [(t/year hour)       
        (t/month hour)
        (t/day hour)
        (t/hour hour)]
       (map str)
       (map #(if (= 1 (count %))
               (str "0" %)
               %))
       (interpose "/")
       (apply str)))

(def max-objects 20)

(defn hour->s3-object-summaries
  ([hour]
   (hour->s3-object-summaries (:s3-base-dir @config) 
                              (hour->s3-prefix hour)
                              nil))
  ([s3-base-dir prefix marker]
   (let [list-objects-res (s3/list-objects (merge {:bucket-name s3-base-dir
                                                   :prefix prefix
                                                   :max-keys max-objects}
                                                  (when marker
                                                    {:marker marker})))]
     (concat (:object-summaries list-objects-res)
             (when (:next-marker list-objects-res)
               (hour->s3-object-summaries s3-base-dir prefix (:next-marker list-objects-res)))))))

(defn object-summary->input-stream
  [s3-object-summary]
  (let [{:keys [s3-base-dir
                target-stream]} @config
        s3-object (s3/get-object :bucket-name s3-base-dir
                                 :key (:key s3-object-summary))]
    (:object-content s3-object)))

(defn uuid
  [_]
  (str (java.util.UUID/randomUUID)))

(def event-type-version->partition-key-fn
  {[:kixi.datastore.file-metadata/updated "1.0.0"] #(get-in % [:kixi.comms.event/payload :kixi.datastore.metadatastore/id])
   [:kixi.datastore.filestore/upload-link-created "1.0.0"] #(get-in % [:kixi.comms.event/payload :kixi.datastore.filestore//id])
   [:kixi.heimdall/user-logged-in "1.0.0"] uuid
   [:kixi.datastore.file/created "1.0.0"] #(get-in % [:kixi.comms.event/payload :kixi.datastore.filestore//id])})

(def event->event-type-version
  (juxt #(or (:kixi.event/type %) (:kixi.comms.event/key %))
        #(or (:kixi.event/version %) (:kixi.comms.event/version %))))

(defn event->partition-key-event
  [event]
  (if-let [partition-key-fn (->> event
                                 event->event-type-version
                                 event-type-version->partition-key-fn)]
    [(partition-key-fn event)
     event]
    (prn "Unknown event type: " (event->event-type-version event))))

(defn send-event
  [[partition-key event]]
  :done)

(defn prn-t
  [x]
  (prn x)
  x)

(def execute-replay
  (comp (mapcat hour->s3-object-summaries)
        (take 2)
        (map object-summary->input-stream)
        input-stream->events
        (map prn-t)
        (keep event->partition-key-event)
        (map send-event)))

(defn aggregate-events
  ([] 0)
  ([acc] acc)
  ([acc x] (inc acc)))

(defn execute
  []
  (transduce execute-replay 
             aggregate-events
             (hour-sequence)))

(defn exit
  [status msg]
  (println msg)
  (System/exit status))

(defn -main
  [& args]
  (let [[ok? loaded-config] (aero/read-config (io/resource "config.edn"))]
    (if ok?
      (do (reset! config loaded-config)
          (exit 0 (execute)))
      (exit 1 config))))
