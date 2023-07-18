;;;; Copyright 2018-Present Modern Interpreters Inc.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this
;;;; file, You can obtain one at https://mozilla.org/MPL/2.0/.

(defpackage :bknr.cluster/transport
  (:use #:cl)
  (:import-from #:bknr.datastore
                #:decode
                #:encode)
  (:export
   #:peer
   #:hostname
   #:port
   #:peer-id
   #:send-message
   #:transport))
(in-package :bknr.cluster/transport)

(defclass peer ()
  ((hostname :initarg :hostname
             :initform "localhost"
             :reader hostname)
   (port :initarg :port
         :reader port)
   (id :initarg :id
       :reader peer-id)))

(defclass transport ()
  ())

(defmethod send-message ((transport transport) (peer peer) body &key on-result
                                                                  retryp)
  ;; TODO: cache the connection here, and dispatch the requesting to
  ;; the thread for the connection.
  (bt:make-thread
   (lambda ()
     (handler-case
         (with-open-stream (stream (comm:open-tcp-stream (hostname peer) (port peer)
                                                         :element-type '(unsigned-byte 8)
                                                         :direction :io
                                                         :read-timeout 1
                                                         :write-timeout 1))
           ;;(encode +protocol-version+ stream)

           (encode body stream)
           (finish-output stream)
           (let ((response (decode stream)))
             (funcall
              (or on-result #'identity)
              response)))
       (error (e)
         (log:error "Got error during transport: ~a" e))))))
