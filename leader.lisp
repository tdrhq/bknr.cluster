;;;; Copyright 2018-Present Modern Interpreters Inc.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this
;;;; file, You can obtain one at https://mozilla.org/MPL/2.0/.

(defpackage :bknr.cluster/leader
  (:use #:cl
        #:bknr.cluster/transport)
  (:import-from #:bknr.cluster/server
                #:quorum
                #:apply-transaction
                #:log-file
                #:commit-transaction
                #:current-term
                #:broadcast
                #:maybe-demote
                #:election-timeout
                #:lock
                #:cv
                #:state-machine
                #:leader-tick)
  (:import-from #:bknr.cluster/log-file
                #:append-log-entry)
  (:import-from #:bknr.datastore
                #:decode
                #:encode)
  (:import-from #:bknr.cluster/rpc
                #:append-entries))
(in-package :bknr.cluster/leader)

(defmethod leader-tick ((Self state-machine))
  (let ((wakep (mp:condition-variable-wait (cv self) (lock self)
                                           :timeout (/ (election-timeout self) 4))))
    (send-heartbeat self)
    (when wakep
      (log:info "TODO: got woken up: ~a" self))))

(defmethod send-heartbeat ((self state-machine))
  (log:debug "Sending heartbeat: ~a" self)
  (broadcast
   self
   (make-instance 'append-entries
                  :term (current-term self)
                  :leader-id (peer-id self)
                  :entries nil)
   :on-result (lambda (result)
                (bt:with-lock-held ((lock self))
                  (maybe-demote self result)))))

(defun encode-to-array (obj)
  (let ((stream (flex:make-in-memory-output-stream)))
    (encode obj stream)
    (flex:get-output-stream-sequence stream)))

(defun decode-from-array (arr)
  (let ((stream (flex:make-in-memory-input-stream arr)))
    (decode stream)))

(defmethod apply-transaction ((self state-machine) transaction)
  (let ((data (encode-to-array transaction)))
    (append-log-entry
     (log-file self)
     (current-term self)
     data)
    (let ((lock (bt:make-lock))
          (cv (bt:make-condition-variable))
          (count 1))
      (bt:with-lock-held (lock)
        (broadcast-append-entry
         self data
         :on-result (lambda ()
                      (bt:with-lock-held (lock)
                        (incf count)
                        (when (> count (quorum self))
                          (bt:condition-notify cv)))))
        (bt:condition-wait cv lock)
        (commit-transaction self transaction)))))

(defun broadcast-append-entry (self data &key on-result)
  (dotimes (i 2)
   (bt:make-thread
    (lambda ()
      (funcall on-result)))))
