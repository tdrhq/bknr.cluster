;;;; Copyright 2018-Present Modern Interpreters Inc.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this
;;;; file, You can obtain one at https://mozilla.org/MPL/2.0/.

(defpackage :bknr.cluster/test-snapshots
  (:use #:cl
        #:fiveam
        #:fiveam-matchers)
  (:import-from #:bknr.cluster/snapshots
                #:snapshot-timestamps-to-delete)
  (:import-from #:local-time
                #:parse-timestring)
  (:import-from #:fiveam-matchers/core
                #:does-not
                #:assert-that))
(in-package :bknr.cluster/test-snapshots)


(util/fiveam:def-suite)

(def-fixture snapshots-to-delete ()
  ;; Real timestamps!
  (let* ((timestamps
          "2024-12-22T06:01:31.202776Z.tar.gz
2024-12-23T06:01:27.057467Z.tar.gz
2024-12-24T06:01:28.090583Z.tar.gz
2024-12-25T06:01:29.742115Z.tar.gz
2024-12-26T06:01:31.607890Z.tar.gz
2024-12-27T06:01:37.463320Z.tar.gz
2024-12-28T06:01:27.491526Z.tar.gz
2024-12-29T06:01:33.181207Z.tar.gz
2024-12-30T06:01:24.178870Z.tar.gz
2024-12-31T06:01:27.035301Z.tar.gz
2025-01-01T06:01:32.059641Z.tar.gz
2025-01-02T06:01:31.765585Z.tar.gz
2025-01-03T06:01:29.687138Z.tar.gz
2025-01-04T06:01:40.347807Z.tar.gz
2025-01-05T06:01:33.716786Z.tar.gz
2025-01-06T06:01:29.211524Z.tar.gz
2025-01-07T06:01:35.581537Z.tar.gz
2025-01-08T06:01:33.514377Z.tar.gz
2025-01-09T06:01:31.519362Z.tar.gz
2025-01-10T06:01:24.333182Z.tar.gz
2025-01-11T06:01:21.292447Z.tar.gz
2025-01-12T06:01:37.254664Z.tar.gz
2025-01-13T06:01:32.775781Z.tar.gz
2025-01-14T06:01:34.168139Z.tar.gz
2025-01-15T06:01:35.893747Z.tar.gz
2025-01-16T06:01:38.791653Z.tar.gz
2025-01-17T06:01:33.023031Z.tar.gz
2025-01-18T06:01:36.142141Z.tar.gz
2025-01-19T06:01:31.520360Z.tar.gz
2025-01-20T06:01:35.939607Z.tar.gz
2025-01-21T06:01:36.385916Z.tar.gz
2025-01-22T06:01:48.918824Z.tar.gz
2025-01-23T06:01:40.217339Z.tar.gz
2025-01-24T06:01:35.115817Z.tar.gz
2025-01-25T06:01:40.610016Z.tar.gz
2025-01-26T06:01:35.515151Z.tar.gz
2025-01-26T18:07:42.179451Z.tar.gz
2025-01-26T20:29:23.857867Z.tar.gz
2025-01-27T06:01:35.691601Z.tar.gz
2025-01-28T06:01:45.479065Z.tar.gz
2025-01-29T06:01:38.980523Z.tar.gz
2025-01-30T06:01:40.951473Z.tar.gz
2025-01-31T06:01:42.953026Z.tar.gz
2025-02-01T06:01:41.071904Z.tar.gz
2025-02-02T06:01:39.273604Z.tar.gz
2025-02-03T06:01:36.975659Z.tar.gz
2025-02-04T06:01:32.387555Z.tar.gz
2025-02-05T06:01:39.869423Z.tar.gz
2025-02-06T06:01:38.406103Z.tar.gz
2025-02-07T06:01:39.696043Z.tar.gz
2025-02-08T06:01:32.506827Z.tar.gz
2025-02-09T06:01:44.899164Z.tar.gz
2025-02-10T06:01:36.662999Z.tar.gz
2025-02-11T06:01:34.943563Z.tar.gz
2025-02-12T06:01:44.472930Z.tar.gz
2025-02-13T06:01:33.937036Z.tar.gz
2025-02-14T06:01:40.742849Z.tar.gz
2025-02-15T06:01:40.056559Z.tar.gz
2025-02-16T06:01:35.962157Z.tar.gz
2025-02-17T06:01:36.954105Z.tar.gz
2025-02-18T06:01:42.249969Z.tar.gz
2025-02-19T06:01:36.439887Z.tar.gz")
         (timestamps (str:lines timestamps))
         (timestamps (mapcar
                      #'local-time:parse-timestring
                      (loop for ts in timestamps
                            collect (str:replace-all ".tar.gz" "" ts)))))
    (&body)))

(defun %snapshot-timestamps-to-delete (timestamps &rest args &key now &allow-other-keys)
  (mapcar (lambda (ts)
            (local-time:format-timestring nil ts :timezone local-time:+utc-zone+) )
          (apply #'snapshot-timestamps-to-delete timestamps :now (parse-timestring now) args)))

(test simple-snapshots-to-delete
  (with-fixture snapshots-to-delete ()
    (let ((result (%snapshot-timestamps-to-delete timestamps :now "2025-02-20T06:01:36.439887Z")))
      (assert-that result
                   (does-not (has-item "2025-02-16T06:01:35.962157Z"))))
    (let ((result (%snapshot-timestamps-to-delete timestamps :now "2027-02-20T06:01:36.439887Z")))
      (assert-that result
                   (has-item "2025-02-16T06:01:35.962157Z")))
    (let ((to-delete (%snapshot-timestamps-to-delete timestamps :now "2025-02-20T06:00:36.439887Z"
                                                                :keep-none 1
                                                                :keep-all 1)))
      (assert-that to-delete
                   (does-not (has-item "2025-02-19T06:01:36.439887Z"))
                   (has-item "2025-02-18T06:01:42.249969Z")
                   ;; Everything except the last is deleted
                   (has-length (1- (length timestamps)))))
    (let ((to-delete (%snapshot-timestamps-to-delete timestamps :now "2025-02-20T06:00:36.439887Z"
                                                                :keep-none 2
                                                                :keep-all 1)))
      (assert-that to-delete
                   (does-not (has-item "2025-02-19T06:01:36.439887Z"))
                   (does-not (has-item "2025-02-18T06:01:42.249969Z"))
                   ;; Everything except the last two deleted
                   (has-length (- (length timestamps) 2))))))

(test snapshots-to-delete-over-the-weeks
  (with-fixture snapshots-to-delete ()
    (let ((to-delete (%snapshot-timestamps-to-delete timestamps :now "2025-02-20T06:00:36.439887Z"
                                                                :keep-none 90
                                                                :keep-all 1)))
      (assert-that to-delete
                   (does-not (has-item "2025-02-19T06:01:36.439887Z"))
                   (described-as "the last timestamp in the week is not deleted"
                     (does-not (has-item "2025-02-18T06:01:42.249969Z")))
                   (has-item "2025-02-17T06:01:36.954105Z")
                   (has-item (starts-with "2025-01-15"))
                   (has-item (does-not (starts-with "2025-01-16")))
                   (has-item (starts-with "2025-01-14"))))))
