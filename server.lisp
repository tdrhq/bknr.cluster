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
                #:encode-char)
  (:import-from #:util/threading
                #:ignore-and-log-errors
                #:make-thread))
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

(defclass request-vote-result ()
  ((term :initarg :term
         :reader term)
   (vote-granted-p :initarg :vote-granted-p)))

(defclass append-entries ()
  ((term :initarg :term
         :reader term)
   (leader-id :initarg :leader-id
              :reader leader-id)
   (prev-log-index :initarg :prev-log-index)
   (prev-log-term :initarg :prev-log-term)
   (entries :initarg :entries)
   (leader-commit :initarg :leader-commit)))

(defclass append-entries-result ()
  ((term :initarg :term
         :reader term)
   (successp :initarg :successp
             :reader successp)))

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

(defmethod print-object ((self state-machine) output)
  (format output "#<FSM ~a>" (peer-id self)))

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

(defmethod handle-client ((self state-machine) socket)
  (mp:process-run-function
   "client thread"
   nil
   (lambda ()
     (ignore-and-log-errors ()
      (with-open-stream (stream (make-instance 'comm:socket-stream
                                               :socket socket
                                               :direction :io
                                               :element-type '(unsigned-byte 8)))
        (log:info "handling client on stream")
        (let ((protocol-version +protocol-version+))
          (log:info "got protocol version ~a" protocol-version)

          (let* ((body (cl-store:restore stream))
                 (result (handle-rpc self body)))
            (cl-store:store
             result
             stream)
            (finish-output stream))))))))

(defmethod handle-rpc ((self state-machine) (append-entries append-entries))
  (bt:with-lock-held ((lock self))
    ;; If we're a candidate, we might need to revert back to a follower

    (maybe-demote self append-entries)

    (bt:condition-notify (cv self))
    (log:info "Got append-entries")

    (flet ((respond (successp)
             (make-instance 'append-entries-result
                            :term (current-term self)
                            :successp successp)))
      (cond
        ((< (term append-entries) (current-term self))
         (respond nil))
        ;; TODO: log stuff
        (t
         (respond t))))))

(defmethod maybe-demote ((self state-machine) msg)
  ;; Whether we're a leader or candidate, we demote ourselves
  ;; to a follower. (A leader shouldn't get term ==
  ;; current-term though.)
  (when (and
         (not (eql (state self) :follower))
         (< (current-term self) (term msg)))
    (setf (state self) :follower))
  (when (< (current-term self) (term msg))
    (setf (current-term self) (term msg))))



(defmethod handle-rpc ((self state-machine) (request-vote request-vote))
  (bt:with-lock-held ((lock self))
    (log:info "got object: ~a" request-vote)
    (maybe-demote self request-vote)
    (let ((vote-granted (and
                         (<= (current-term self)
                             (term request-vote))
                         (not (voted-for self))
                         (not (= (peer-id self)
                                 (voted-for self)))

                         ;; todo: check logs (5.2, 5.4)
                         )))
      (when vote-granted
        (setf (voted-for self)
              (candidate-id request-vote)))
      (bt:condition-notify (cv self))
      (make-instance 'request-vote-result
                     :term (current-term self)
                     :vote-granted-p vote-granted))))

(defmethod handle-main-process ((self state-machine))
  (setf (role self) :follower)
  (bt:with-lock-held ((lock self))
    (loop
      (main-process-tick self))))

(defmethod main-process-tick ((self state-machine))
  (ecase (state self)
    (:follower
     (follower-tick self))
    (:candidate
     (candidate-tick self))
    (:leader
     (leader-tick self))))

(defmethod follower-tick ((Self state-machine))
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
         (log:info "got message for ~a" peer-name))))))

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

(defmethod candidate-tick ((self state-machine))
  (let ((wakep (mp:condition-variable-wait (cv self) (lock self)
                                           :timeout (election-timeout self))))
    (cond
      (wakep
       ;; Currently this doesn't happen until we implement AppendRPC.
       ))))

(defmethod start-election ((self state-machine))
  (let ((vote-count 1))
   (with-slots (current-term state voted-for) self
     (incf current-term)
     (broadcast self
                (make-instance 'request-vote
                               :term current-term
                               :candidate-id 0
                               :last-log-index 0
                               :last-log-term 0)
                :on-result
                (lambda (result)
                  ;; This should be guaranteed to run on another
                  ;; thread.
                  (log:info "Got a result: ~a" result)
                  (bt:with-lock-held ((lock self))
                    (maybe-demote self result)
                    (when
                        (and (eql :candidate state)
                             (eql current-term (term result)))
                      (incf vote-count)
                      (when (> vote-count (floor (length (peers self)) 2))
                        (log:info "Turning into leader! ~a" (peer-id self))
                        (setf state :leader)
                        (bt:condition-notify (cv self)))))))
     (setf state :candidate)
     (setf voted-for (peer-id self)))))

(DEFUN peer= (peer1 peer2)
  (equal (peer-name peer1)
         (peer-name peer2)))

(defmethod true-peers ((self state-machine))
  (loop for peer in (peers self)
        if (not (peer= peer (this-peer self)))
          collect peer))

(defmethod broadcast ((self state-machine) body
                      &key on-result)
  (loop for peer in (true-peers self)
        do
           (send-message peer body :on-result on-result)))

(defmethod send-message ((peer peer) body &key on-result)
  ;; TODO: cache the connection here, and dispatch the requesting to
  ;; the thread for the connection.
  (make-thread
   (lambda ()
    (with-open-stream (stream (comm:open-tcp-stream (hostname peer) (port peer)
                                                    :element-type '(unsigned-byte 8)
                                                    :direction :io
                                                    :read-timeout 1
                                                    :write-timeout 1))
      ;;(encode +protocol-version+ stream)

      (cl-store:store body stream)
      (finish-output stream)
      (let ((response (cl-store:restore stream)))
        (funcall
         (or on-result #'identity)
         response))))))

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
;; (start-up (elt *machines* 1))
;; (shutdown (elt *machines* 1))
