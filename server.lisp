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
         :reader port)
   (id :initarg :id
       :reader peer-id)))

(defclass request-vote ()
  ((term :initarg :term
         :reader term)
   (candidate-id :initarg :candidate-id
                 :reader candidate-id)
   (last-log-index :initarg :last-log-index
                   :reader last-log-index)
   (last-log-term :initarg :last-log-term
                  :reader loast-log-term)))

(defmethod encode-rpc-body ((self request-vote) stream)
  (with-slots (term candidate-id last-log-index last-log-term)
      self
    (encode term stream)
    (encode candidate-id stream)
    (encode last-log-index stream)
    (encode last-log-term stream)))

(defmethod decode-rpc-body ((rpc (eql #\V)) stream)
  (make-instance 'request-vote
                 :term (decode stream)
                 :candidate-id (decode stream)
                 :last-log-index (decode stream)
                 :last-log-term (decode stream)))

(defclass request-vote-result ()
  ((term :initarg :term)
   (vote-granted-p :initarg :vote-granted-p)))


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
   (state :accessor state)

   (role :initform :follower
         :accessor role)

   (election-timeout :initform 1
                     :reader election-timeout
                     :documentation "The actual timeout is chosen randomly between election-timeout and 2*election-timeout")

   (lock :initform (bt:make-lock)
         :reader lock)
   (cv :initform (bt:make-condition-variable)
       :reader cv)

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

(defmethod peer-id ((self state-machine))
  (peer-id (this-peer self)))


(defvar *peers*
  (loop for port in (list 5053 5054 5055)
        collect (make-instance 'peer
                               :port port
                               :id (- port 5053))))
(defvar *machines*
  (loop for peer in *peers*
        collect (make-instance 'state-machine
                               :peers *peers*
                               :this-peer peer)))

(defmethod start-up ((self state-machine))
  (setf (state self) :follower)
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

(defmethod decode-rpc-body (rpc stream)
  (decode stream))

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
        (let* ((rpc (%decode-char stream))
               (body (decode-rpc-body rpc stream)))
          (handle-rpc self rpc body)))))))

(defmethod handle-rpc ((self state-machine) (rpc (eql #\V)) request-vote)
  (bt:with-lock-held ((lock self))
    (log:info "got object: ~a" request-vote)
    (bt:condition-notify (cv self))))

(defmethod handle-main-process ((self state-machine))
  (setf (role self) :follower)
  (bt:with-lock-held ((lock self))
    (loop
      (main-process-tick self))))

(defmethod main-process-tick ((self state-machine))
  (let* ((divisor 10000)
         (election-timeout (* (+ 1 (/ (random divisor) divisor))
                              (election-timeout self)))
         (peer-name (peer-name (this-peer self))))
    (let ((wakep (mp:condition-variable-wait (cv self) (lock self)
                                             :timeout election-timeout)))
      (cond
        ((not wakep)
         (log:info "Election timed out for ~a" peer-name)
         (start-election self))
        (t
         (log:info "got message for ~a" peer-name)
         #+nil
         (etypecase mail
           (request-vote
            (when (and
                   (= (current-term self)
                      (term request-vote))
                   (not (voted-for self)))
              (setf (voted-for self)
                    (candidate-id request-vote))
              t))))))))

(defmethod start-election ((self state-machine))
  (with-slots (current-term state voted-for) self
    (incf current-term)
    (broadcast self +request-vote+
               (make-instance 'request-vote
                              :term current-term
                              :candidate-id 0
                              :last-log-index 0
                              :last-log-term 0))
    (setf state :candidate)
    (setf voted-for (peer-id self))))

(DEFUN peer= (peer1 peer2)
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

(defmethod encode-rpc-body (body stream)
  (encode body stream))

(defmethod send-message ((peer peer) rpc body)
  ;; TODO: cache the connection here
  (with-open-stream (stream (comm:open-tcp-stream (hostname peer) (port peer)
                                                  :element-type '(unsigned-byte 8)
                                                  :direction :io))
    (encode +protocol-version+ stream)
    (%write-char rpc stream)

    (encode-rpc-body body stream)))

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
