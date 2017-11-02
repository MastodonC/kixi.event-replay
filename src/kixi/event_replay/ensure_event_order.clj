(ns kixi.event-replay.ensure-event-order
  "Our Events contain all the mutations for our domain, as such they must be processed in order.
  This order is maintained until they reach the AWS Firehose that is delivering them to S3, therefore
  when reading them back in for processing we have to ensure the order is maintained.

  For now we will throw an exception if this happens, but there is a prototype windowing ordering
  implementation to try should we encounter the problem"
  (:require [clj-time.core :as t]
            [clj-time.format :as f]
            [taoensso.nippy :as nippy]))

(def parse-timestamp
  (partial f/parse (f/formatter :basic-date-time)))

(defn event->created-time
  [event]
  (let [contained-event (nippy/thaw (:event event))]
    (parse-timestamp
     (or (:kixi.comms.event/created-at contained-event)
         (:kixi.event/created-at contained-event)))))

(defn exception-on-out-of-order-event
  [xf]
  (let [state (atom nil)]
    (fn
      ([] (xf))
      ([result]
       (->> (second @state)
            (xf result)
            xf))
      ([result event]
       (if (nil? @state)
         (do (reset! state [(event->created-time event) event])
             result)
         (let [event-created-time (event->created-time event)]
           (if-not (pos? (compare (first @state) event-created-time))
             (let [release (second @state)]
               (reset! state [event-created-time event])
               (xf result release))
             (throw (ex-info "Event created time stamps out of order"
                             {:first-event (second @state)
                              :first-event-time (first @state)
                              :second-event event
                              :second-event-time event-created-time})))))))))

(defn compare-event-created-times
  [f s]
  (compare (event->created-time f)
           (event->created-time s)))

(defn event-created-ordered-set
  ([]
   (event-created-ordered-set []))
  ([ks]
   (apply (partial sorted-set-by
                   (comp #(if (zero? %)
                            -1 %)
                         compare-event-created-times))
          ks)))

(def sort-window-interval (t/millis 10))

;This doesn't perform very well at all, but should be enough to prove the theory on a discovered out of order file
(defn time-windowed-ordering
  [xf]
  (let [state (atom (event-created-ordered-set))]
    (fn
      ([] (xf))
      ([result]
       (xf
        (reduce
         (fn [r e]
           (xf r e))
         result
         @state)))
      ([result event]
       (if (empty? @state)
         (do (swap! state conj event)
             result)
         (let [event-dt-minus-window (t/minus (event->created-time event)
                                              sort-window-interval)
               [releasable-events keep-events] (split-with
                                                #(t/before? (event->created-time %)
                                                            event-dt-minus-window)
                                                @state)]
           (reset! state (conj (event-created-ordered-set keep-events)
                               event))
           (reduce
            (fn [r e]
              (xf r e))
            result
            releasable-events)))))))
