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

(deftest test-NPY-header-in-description
  (let [input (io/input-stream "./test-resources/test-file-with-NPY-header-in-description")]
    (is (=  [{:kixi.comms.message/type "event",
              :kixi.comms.event/id "c39a7a9a-0ed3-4d25-a993-5701f21f411c",
              :kixi.comms.event/key
              :kixi.datastore.filestore/upload-link-created,
              :kixi.comms.event/version "1.0.0",
              :kixi.comms.event/created-at "20171002T082540.334Z",
              :kixi.comms.event/payload
              {:kixi.datastore.filestore/upload-link
               "https://staging-witan-kixi-datastore-filestore.s3.eu-central-1.amazonaws.com/6ac6f9e5-54e7-4032-9dc9-4d82770b7eeb?X-Amz-Security-Token=FQoDYXdzEOj%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaDAGhvyw8ZjwRgWFpkSK9A1yIr2s4F9ZwFayzFBtmldq3yw%2BTfq3MNNJZ%2FJVEvqaOU88%2BRDLeCRnRzw1soL7nZzXZtXLCMNDeF4QNU7mT%2B6kY%2FNQguRNjjtuiNc6kY8O1jNJShDYKzCY5z2f6%2FNAoldzTw2C1Jh7hpdrvmNvCtb%2FL8QwGEW5QwpmKyT8p8Dqrd6UVYqUoJ0rDl8oNyNc%2FyxhJ2btoqhhvsgPPYJNtHlt792IJaZ%2FXBHYJNkLIddYXxrAlplcKtCWlKawApAJoPX7IYfoGwhzDIBtp9ZpJRychrsYyESgM7pYqEHLq0Vai2rAj3BvCyamxv3K%2BKpfD%2FEbl2osVdnQ93HMqZz1UyYEKg0VxPKL3d8MSip3ROgjF64ABQF%2Bw47JeYEnZb2XSCe%2FSMWV%2FqCF8Ns1OohvMOR9WrEEDyog4kr1OU%2FcyWuRYLLN1TwvohmHKJO5FS3ZZHbpdH7y29wWoojXaKCo1C8008y%2F47gphb5T1InmGuWC%2FFGtergSKOSCFNdVodbSYB9pLQxdsVQioJvbtSye0CT1152UppFfcnBOAx3ag0imZUMKSUqm0VjzpjB5bkOb4oKc5452T%2FohqG7TVX2go377HzgU%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20171002T082540Z&X-Amz-SignedHeaders=host&X-Amz-Expires=1799&X-Amz-Credential=ASIAJZY3P4ELITMFJPZQ%2F20171002%2Feu-central-1%2Fs3%2Faws4_request&X-Amz-Signature=27ad0fb7493fc329cb32f2a51c87562400271d80673dba29ce4d6e654c5f3b21",
               :kixi.datastore.filestore/id
               "6ac6f9e5-54e7-4032-9dc9-4d82770b7eeb",
               :kixi.user/id "3eea2386-1000-46e2-a455-1d119f44007c"},
              :kixi.comms.event/origin "0313594b38b8",
              :kixi.comms.command/id "735315d1-9e0a-4397-98ac-52bf37a211ce"}
             {:kixi.comms.message/type "event",
              :kixi.comms.event/id "9c417835-ea1a-4139-bbec-0fe234dbbfbc",
              :kixi.comms.event/key :kixi.datastore.file/created,
              :kixi.comms.event/version "1.0.0",
              :kixi.comms.event/created-at "20171002T082543.632Z",
              :kixi.comms.event/payload
              #:kixi.datastore.metadatastore{:header true,
                                             :file-type "csv",
                                             :name "Break Event Parsing Test",
                                             :id
                                             "6ac6f9e5-54e7-4032-9dc9-4d82770b7eeb",
                                             :type "stored",
                                             :sharing
                                             #:kixi.datastore.metadatastore{:meta-read
                                                                            ["3e2a379a-ccbb-49ef-93bb-24d58a5f428f"],
                                                                            :meta-update
                                                                            ["3e2a379a-ccbb-49ef-93bb-24d58a5f428f"],
                                                                            :file-read
                                                                            ["3e2a379a-ccbb-49ef-93bb-24d58a5f428f"]},
                                             :provenance
                                             {:kixi.datastore.metadatastore/source
                                              "upload",
                                              :kixi.user/id
                                              "3eea2386-1000-46e2-a455-1d119f44007c",
                                              :kixi.datastore.metadatastore/created
                                              "20171002T082543.331Z"},
                                             :size-bytes 14},
              :kixi.comms.event/origin "0313594b38b8",
              :kixi.comms.command/id "9026d676-1dae-48aa-a946-d00cf6549b1f"}
             {:kixi.comms.message/type "event",
              :kixi.comms.event/id "5bfe7b4d-9007-47d0-a36e-db9831bcb3cd",
              :kixi.comms.event/key :kixi.datastore.file-metadata/updated,
              :kixi.comms.event/version "1.0.0",
              :kixi.comms.event/created-at "20171002T082543.645Z",
              :kixi.comms.event/payload
              {:kixi.datastore.metadatastore/file-metadata
               #:kixi.datastore.metadatastore{:header true,
                                              :file-type "csv",
                                              :name "Break Event Parsing Test",
                                              :id
                                              "6ac6f9e5-54e7-4032-9dc9-4d82770b7eeb",
                                              :type "stored",
                                              :sharing
                                              #:kixi.datastore.metadatastore{:meta-read
                                                                             ["3e2a379a-ccbb-49ef-93bb-24d58a5f428f"],
                                                                             :meta-update
                                                                             ["3e2a379a-ccbb-49ef-93bb-24d58a5f428f"],
                                                                             :file-read
                                                                             ["3e2a379a-ccbb-49ef-93bb-24d58a5f428f"]},
                                              :provenance
                                              {:kixi.datastore.metadatastore/source
                                               "upload",
                                               :kixi.user/id
                                               "3eea2386-1000-46e2-a455-1d119f44007c",
                                               :kixi.datastore.metadatastore/created
                                               "20171002T082543.331Z"},
                                              :size-bytes 14},
               :kixi.datastore.communication-specs/file-metadata-update-type
               :kixi.datastore.communication-specs/file-metadata-created},
              :kixi.comms.event/origin "0313594b38b8",
              :kixi.comms.command/id "9026d676-1dae-48aa-a946-d00cf6549b1f"}
             {:kixi.comms.message/type "event",
              :kixi.comms.event/id "749afab7-0c6d-4ab8-bb84-9940bee39ba3",
              :kixi.comms.event/key :kixi.datastore.file-metadata/updated,
              :kixi.comms.event/version "1.0.0",
              :kixi.comms.event/created-at "20171002T082618.350Z",
              :kixi.comms.event/payload
              {:kixi.datastore.metadatastore/id
               "6ac6f9e5-54e7-4032-9dc9-4d82770b7eeb",
               :kixi.datastore.metadatastore.update/description
               {:set
                "Here is a bit of descriptive text\nNPYthat breaks the event backup parsing"},
               :kixi.datastore.communication-specs/file-metadata-update-type
               :kixi.datastore.communication-specs/file-metadata-update,
               :kixi/user
               #:kixi.user{:kixi.user/id "3eea2386-1000-46e2-a455-1d119f44007c",
                           :kixi.user/groups
                           ["3e2a379a-ccbb-49ef-93bb-24d58a5f428f"
                            "03ffa30a-d2aa-487d-ad5e-53fa39e56426"
                            "0ace7b64-4f2a-4665-8784-b44ff7be63db"]}},
              :kixi.comms.event/origin "0313594b38b8",
              :kixi.comms.command/id "79788b7a-fb90-47dc-8d30-16330adf829f"}]
            (transduce input-stream->events
                       conj
                       [input])))))
