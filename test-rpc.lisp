;;;; Copyright 2018-Present Modern Interpreters Inc.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this
;;;; file, You can obtain one at https://mozilla.org/MPL/2.0/.

(defpackage :bknr.cluster/test-rpc
  (:use #:cl
        #:fiveam)
  (:import-from #:bknr.datastore
                #:decode
                #:encode)
  (:import-from #:bknr.cluster/rpc
                #:request-vote
                #:last-log-index
                #:term))
(in-package :bknr.cluster/test-rpc)

(util/fiveam:def-suite)

(test simple-encode-decode
  (let ((in (make-instance 'request-vote
                           :term "foo"
                           :last-log-index 10)))
    (let ((stream (flex:make-in-memory-output-stream)))
      (encode in stream)
      (let ((out (decode (flex:make-in-memory-input-stream
                          (flex:get-output-stream-sequence stream)))))
        (is (equal (term out) "foo"))
        (is (eql 10 (last-log-index out)))))))
