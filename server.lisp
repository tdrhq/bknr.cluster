;;;; Copyright 2018-Present Modern Interpreters Inc.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this
;;;; file, You can obtain one at https://mozilla.org/MPL/2.0/.

(defpackage :bknr.cluster/server
  (:use #:cl
        #:bknr.cluster/rpc
        #:bknr.cluster/transport)
  (:import-from #:bknr.datastore
                #:decode
                #:%write-char
                #:%decode-char
                #:encode
                #:encode-object
                #:encode-char)
  (:import-from #:util/threading
                #:ignore-and-log-errors
                #:make-thread)
  (:import-from #:bknr.cluster/log-file
                #:append-log-entries-overwriting-stale
                #:term-at
                #:open-log-file)
  (:import-from #:bknr.cluster/rpc
                #:prev-log-index)
  (:import-from #:easy-macros
                #:def-easy-macro)
  (:import-from #:bknr.cluster/util
                #:safely-ignore-errors))
(in-package :bknr.cluster/server)

(defconstant +append-entries+ #\A)
(defconstant +request-vote+ #\V)
(defconstant +test-broadcast+ #\T)

(defconstant +protocol-version+ 1)

(defmethod peer-name ((self peer))
  (format nil "~a:~a" (hostname self) (port self)))

(defclass state-machine ()
  ((peers :initarg :peers
          :accessor peers)
   (directory :initarg :directory
              :accessor state-machine-directory)
   (this-peer :initarg :this-peer
              :reader this-peer)
   (server-process :accessor server-process
                   :initform nil)
   (main-process :accessor main-process
                 :initform nil
                 :documentation "The process that does heartbeat, leadership election and such.")
   (transport :reader transport
              :initarg :transport
              :initform (make-instance 'transport))
   (state :accessor state)

   (role :initform :follower
         :accessor role)

   (election-timeout :initform 1
                     :initarg :election-timeout
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
   (log-file :accessor log-file
             :documentation "log entries. Note that the paper calls this log[]")

   ;; Volatile state on allservers
   (commit-index :initform 0
                 :accessor commit-index)
   (last-applied :initform 0
                 :accessor last-applied)

   ;; Volatile state on leaders
   (next-index :accessor next-index)
   (match-index :accessor match-index)))

(defgeneric commit-transaction (state-machine transaction))

(defgeneric apply-transaction (state-machine transaction))


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
        for i from 0
        collect (make-instance 'state-machine
                               :peers *peers*
                               :directory (ensure-directories-exist (format nil "/tmp/fsm/~a/" i))
                               :this-peer peer)))

(defmethod start-up ((self state-machine))
  (setf (state self) :follower)
  (setf (log-file self) (open-log-file
                         :pathname (path:catfile (state-machine-directory self) "log-file")))
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
     (safely-ignore-errors ()
      (with-open-stream (stream (make-instance 'comm:socket-stream
                                               :socket socket
                                               :direction :io
                                               :element-type '(unsigned-byte 8)))
        (log:info "handling client on stream")
        (let ((protocol-version +protocol-version+))
          (log:info "got protocol version ~a" protocol-version)

          (let* ((body (decode stream))
                 (result (handle-rpc self body)))
            (encode
             result
             stream)
            (finish-output stream))))))))

(defmethod handle-rpc :around ((self state-machine) msg)
  (bt:with-lock-held ((lock self))
    (prog1
        (call-next-method)
      (maybe-demote self msg)
      (bt:condition-notify (cv self)))))

(defmethod handle-rpc ((self state-machine) (append-entries append-entries))
  (log:info "Got append-entries")

  (flet ((respond (successp)
           (make-instance 'append-entries-result
                          :term (current-term self)
                          :successp successp)))
    (cond
      ((< (term append-entries) (current-term self))
       (respond nil))
      ((not
        (eql
         (term-at (log-file self) (prev-log-index append-entries))
         (prev-log-term append-entries)))
       (respond nil))
      (t
       (append-log-entries-overwriting-stale
        (log-file self)
        (prev-log-index append-entries)
        (entries append-entries))
       (respond t)))))


(defmethod maybe-demote ((self state-machine) msg)
  ;; Whether we're a leader or candidate, we demote ourselves
  ;; to a follower. (A leader shouldn't get term ==
  ;; current-term though.)
  (when (and
         (not (eql (state self) :follower))
         (< (current-term self) (term msg)))
    (setf (state self) :follower))
  (when (< (current-term self) (term msg))
    (setf (current-term self) (term msg))
    ;; no votes in the current term
    (setf (voted-for self) nil)))



(defmethod handle-rpc ((self state-machine) (request-vote request-vote))
  (log:info "got object: ~a" request-vote)
  (flet ((make-result (vote-granted)
           (make-instance 'request-vote-result
                          :term (current-term self)
                          :vote-granted-p vote-granted)))
    (cond
      ((< (term request-vote)
          (current-term self))
       (make-result nil))
      (t
       ;; If term == current-term:
       ;;    if we're a follower who hasn't voted(voted-for = nil): then we give them the vote
       ;;    if we're a candidate  (voted-for = self) we deny
       ;;    if we're a leader (also voted-for = self) we deny
       ;; If term > current-term
       ;;    if we're a follower or candidate: give them the vote
       ;;    if we're a leader: demote ourselves and give them the vote
       (let ((vote (and
                    (or
                     (null (voted-for self))
                     (equal (candidate-id request-vote)
                            (voted-for self)))
                    ;; TODO: candidate's log is at least as up-to-date as receiver's log
                    )))
         (log:debug "Granting vote to ~a from ~a"
                    (candidate-id request-vote)
                    (peer-id self))
         (when vote
           (setf (current-term self) (term request-vote))
           (setf (voted-for self) (candidate-id request-vote)))
         (make-result vote))))))

(defmethod handle-main-process ((self state-machine))
  (setf (role self) :follower)
  (bt:with-lock-held ((lock self))
    (loop
      (main-process-tick self))))

(defgeneric leader-tick (state-machine))

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

(defmethod candidate-tick ((self state-machine))
  (let ((wakep (mp:condition-variable-wait (cv self) (lock self)
                                           :timeout (election-timeout self))))
    (cond
      (wakep
       ;; Currently this doesn't happen until we implement AppendRPC.
       ))))

(defun quorum (self)
  (floor (length (peers self)) 2))

(defmethod start-election ((self state-machine))
  (let ((vote-count 1))
   (with-slots (current-term state voted-for) self
     (incf current-term)
     (broadcast self
                (make-instance 'request-vote
                               :term current-term
                               :candidate-id (peer-id self)
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
                      (when (> vote-count (quorum self))
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
           (send-message (transport self) peer body :on-result on-result)))

(defmethod shutdown ((self state-machine))
  (when (server-process self)
    (mp:process-terminate (server-process self))
    (setf (server-process self) nil))

  (when (main-process self)
    (mp:process-terminate (main-process self))
    (setf (main-process self) nil)))

(defun start-test ()
  (mapc #'start-up *machines*))

(defun stop-test ()
  (mapc #'shutdown *machines*))

;; (start-test)
;; (stop-test)
;; (start-up (elt *machines* 1))
;; (shutdown (elt *machines* 0))
