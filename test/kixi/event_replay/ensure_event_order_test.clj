(ns kixi.event-replay.ensure-event-order-test
  (:require [clj-time.coerce :as c]
            [clj-time.core :as t]
            [clj-time.format :as f]
            [clojure.spec.alpha :as spec]
            [clojure.spec.gen.alpha :as gen]
            [clojure.spec.test.alpha :as stest]
            [clojure.test :refer :all]
            [kixi.event-replay.ensure-event-order :as sut]
            [clojure.test.check :as tc]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.properties :as prop]
            [taoensso.nippy :as nippy]))

(comment "Cider bug https://github.com/clojure-emacs/cider/issues/1841 means [clojure.test.check :as tc] being required without use")

(def generative-iterations 1000)

(def timestamp-gen
  (gen/fmap
   (comp (partial f/unparse (f/formatter :basic-date-time))
         c/from-date)
   (spec/gen (spec/inst-in #inst "2017-10-19T00:00:00.000" #inst "2017-10-19T01:00:00.000"))))

(def event-gen
  (gen/map (gen/elements [:kixi.comms.event/created-at :kixi.event/created-at])
           timestamp-gen
           {:num-elements 1}))

(def events-gen
  (gen/vector (gen/fmap
               (fn [e]
                 {:event (nippy/freeze e)})
               event-gen)
              1 50))

(defn exception-on-out-of-order-event-test
  [events]
  (into []
   sut/exception-on-out-of-order-event
   events))

(defspec exception-on-out-of-order-event
  generative-iterations
  (prop/for-all [events events-gen]
                (if (= (sort-by sut/event->created-time events)
                       events)
                  (= (exception-on-out-of-order-event-test events)
                     events)
                  (try
                    (exception-on-out-of-order-event-test events)
                    false
                    (catch Exception e
                      true)))))


(def ordering-window (t/minutes 5))

(defn within-reverse-window
  [ft st]
  (t/within? (t/minus ft ordering-window)
             ft
             st))

(defn after-unless-out-of-window
  "Needs to be able to detect the reason for an out of order timestamp, probably by scanning ahead and finding a suspected window breaking timestamp"
  [events]
  (reduce
   (fn [f s]
     (let [ft (sut/event->created-time f)
           st (sut/event->created-time s)]
       (if (or (t/after? st ft)
               (t/equal? st ft))
         s
         (if (within-reverse-window ft st)
           (reduced false)
           s))))
   events))

(spec/fdef time-windowed-ordering-test
           :args (spec/with-gen vector?
                   #(gen/vector events-gen
                                1))
           :fn (fn [{:keys [args ret]}]
                 (= (count (first args))
                    (count ret)))
           :ret vector?) ;no assertion, need effective after-unless-out-of-window

(defn time-windowed-ordering-test
  [events]
  (into []
        (sut/time-windowed-ordering ordering-window)
        events))

(deftest check-time-windowed-ordering-test
  (is (every? (comp nil? :failure)
              (stest/check `time-windowed-ordering-test
                           {:max-size generative-iterations}))))
