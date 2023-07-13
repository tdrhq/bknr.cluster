;;;; Copyright 2018-Present Modern Interpreters Inc.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this
;;;; file, You can obtain one at https://mozilla.org/MPL/2.0/.

(defpackage :bknr.cluster/server
  (:use #:cl)
  (:import-from #:bknr.datastore
                #:decode
                #:%write-char
                #:%decode-char
                #:encode
                #:encode-object
                #:encode-char))
(in-package :bknr.cluster/server)

(defconstant +append-entries+ #\A)
(defconstant +request-vote+ #\V)
(defconstant +test-broadcast+ #\T)

(defconstant +protocol-version+ 1)

(defclass peer ()
  ((hostname :initarg :hostname
             :initform "localhost"
             :reader hostname)
   (port :initarg :port
         :reader port)))

(defmethod peer-name ((self peer))
  (format nil "~a:~a" (hostname self) (port self)))

(defclass state-machine ()
  ((peers :initarg :peers
          :accessor peers)
   (this-peer :initarg :this-peer
              :reader this-peer)
   (server-process :accessor server-process)
   (main-process :accessor main-process
                 :documentation "The process that does heartbeat, leadership election and such.")

   (lock :initform (bt:make-lock))
   (cv :initform (bt:make-condition-variable))

   ;; Persistent state on all servers
   (current-term :initform 0
                 :accessor current-term)
   (voted-for :initform nil
              :accessor voted-for)
   (logs :initform nil
         :accessor logs
         :documentation "log entries. Note that the paper calls this log[]")

   ;; Volatile state on allservers
   (commit-index :initform 0
                 :accessor commit-index)
   (last-applied :initform 0
                 :accessor last-applied)

   ;; Volatile state on leaders
   (next-index :accessor next-index)
   (match-index :accessor match-index)))


(defvar *peers*
  (loop for port in (list 5053 5054 5055)
        collect (make-instance 'peer
                               :port port)))
(defvar *machines*
  (loop for peer in *peers*
        collect (make-instance 'state-machine
                               :peers *peers*
                               :this-peer peer)))

(defmethod start-up ((self state-machine))
  (setf
   (server-process self)
   (comm:start-up-server
    :function (lambda (socket)
                (handle-client self socket))
    :service (port (this-peer self))))
  (setf
   (main-process self)
   (mp:process-run-function
    (format nil "Server thread for ~a" (peer-name (this-peer self)))
    nil
    (lambda ()
      (handle-main-process self)))))

(defmethod handle-client ((self state-machine) socket)
  (mp:process-run-function
   "client thread"
   nil
   (lambda ()
    (with-open-stream (stream (make-instance 'comm:socket-stream
                                             :socket socket
                                             :direction :io
                                             :element-type '(unsigned-byte 8)))
      (let ((protocol-version (decode stream)))
        (declare (ignore protocol-version))
        (let ((rpc (%decode-char stream))
              (body (decode stream)))
          (log:info "Got rpc: ~a ~a" rpc body)))))))

(defmethod handle-main-process ((self state-machine))
  (let ((state :follower))
    (loop for i from 0 to 1000
          do
             (broadcast self
                        +test-broadcast+ (format nil "hello from ~a" (peer-name (this-peer self))))
             (sleep 5))))

(defun peer= (peer1 peer2)
  (equal (peer-name peer1)
         (peer-name peer2)))

(defmethod true-peers ((self state-machine))
  (loop for peer in (peers self)
        if (not (peer= peer (this-peer self)))
          collect peer))

(defmethod broadcast ((self state-machine) rpc body)
  (loop for peer in (true-peers self)
        do
           (send-message peer rpc body)))

(defmethod send-message ((peer peer) rpc body)
  ;; TODO: cache the connection here
  (with-open-stream (stream (comm:open-tcp-stream (hostname peer) (port peer)
                                                  :element-type '(unsigned-byte 8)
                                                  :direction :io))
    (encode +protocol-version+ stream)
    (%write-char rpc stream)
    (encode body stream)))

(defmethod shutdown ((self state-machine))
  (mp:process-terminate (server-process self))
  (mp:process-terminate (main-process self))
  (setf (server-process self) nil)
  (setf (main-process self) nil))

(defun start-test ()
  (mapc #'start-up *machines*))

(defun stop-test ()
  (mapc #'shutdown *machines*))

;; (start-test)
;; (stop-test)
