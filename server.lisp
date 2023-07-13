;;;; Copyright 2018-Present Modern Interpreters Inc.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this
;;;; file, You can obtain one at https://mozilla.org/MPL/2.0/.

(defpackage :bknr.cluster/server
  (:use #:cl))
(in-package :bknr.cluster/server)

(defconstant +append-entries+ 1)
(defconstant +request-vote+ 2)

(defclass peer ()
  ((hostname :initarg :hostname
             :initform "localhost"
             :reader hostname)
   (port :initarg :port
         :reader port)))

(defclass state-machine ()
  ((peers :initarg :peers)
   (this-peer :initarg :this-peer
              :reader this-peer)
   (server-process :accessor server-process)))


(defvar *peers*
  (loop for port in (list 5050 5051 5052)
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
    :function (lambda (stream)
                (handle-client self stream))
    :create-stream t
    :service (port (this-peer self)))))

(defmethod shutdown ((self state-machine))
  (mp:process-terminate (server-process self))
  (setf (server-process self) nil))

(defun start-test ()
  (mapc #'start-up *machines*))

(defun stop-test ()
  (mapc #'shutdown *machines*))

;; (start-test)
;; (stop-test)
