(ns kixi.event-replay-test
  (:require [clojure.test :refer :all]
            [kixi.event-replay :refer :all]
            [clojure.java.io :as io]
            [kixi.event-replay.inputstream-to-events :refer [input-stream->events]])
  (:import [java.nio ByteBuffer]))

(def ^java.nio.charset.Charset utf8 java.nio.charset.StandardCharsets/UTF_8)

(deftest test-split-short-lines
  (let [input (io/input-stream "./test-resources/split-lines")]
    (is (= [{:kixi.event/created-at "20170928T023836.062Z",
             :kixi.datastore.metadatastore/bundled-ids
             #{"3e01c432-f8a2-4253-9f2c-10e58ce79479"},
             :kixi.message/type :event,
             :kixi.event/version "1.0.0",
             :kixi.command/id "46b81e38-588d-4329-bb10-1ec5ace56ecb",
             :kixi/user {:kixi.user/id "9ec12a1a-ceda-467d-9d86-b21a0a42b2fb",
                         :kixi.user/groups ["9ec12a1a-ceda-467d-9d86-b21a0a42b2fb"]},
             :kixi.message/origin "99ec54cee25f",
             :kixi.event/type :kixi.datastore/files-removed-from-bundle,
             :kixi.datastore.metadatastore/id
             "b0e4a500-97e8-41d5-be15-38e03a4d4616",
             :kixi.event/id "a2754f71-84a8-42c5-9bb9-da4fde723535"}
            {:kixi.event/created-at "20170928T023839.062Z",
             :kixi.datastore.metadatastore/bundled-ids
             #{"79bd5ea0-8fdf-4213-8fe9-7319fbf015d2"},
             :kixi.message/type :event,
             :kixi.event/version "1.0.0",
             :kixi.command/id "29425737-c9dc-423e-b680-0947cabe0aba",
             :kixi/user {:kixi.user/id "9ec12a1a-ceda-467d-9d86-b21a0a42b2fb",
                         :kixi.user/groups ["9ec12a1a-ceda-467d-9d86-b21a0a42b2fb"]},
             :kixi.message/origin "99ec54cee25f",
             :kixi.event/type :kixi.datastore/files-removed-from-bundle,
             :kixi.datastore.metadatastore/id
             "b0e4a500-97e8-41d5-be15-38e03a4d4616",
             :kixi.event/id "cc8097bb-c338-41f7-be2c-a1bc188a6bf4"}]
           (transduce input-stream->events
                      conj
                      [input])))))
