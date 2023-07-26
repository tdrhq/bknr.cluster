;;;; Copyright 2018-Present Modern Interpreters Inc.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this
;;;; file, You can obtain one at https://mozilla.org/MPL/2.0/.

(defpackage :bknr.cluster/test-server
  (:use #:cl
        #:fiveam)
  (:import-from #:bknr.cluster/server
                #:with-logs-hidden
                #:bknr-set-log-level
                #:leaderp
                #:lisp-state-machine
                #:commit-transaction
                #:apply-transaction
                #:start-up
                #:state-machine
                #:shutdown)
  (:import-from #:easy-macros
                #:def-easy-macro)
  (:import-from #:util/random-port
                #:random-port)
  (:export
   #:with-peer-and-machines))
(in-package :bknr.cluster/test-server)

(util/fiveam:def-suite)

(defclass counter (lisp-state-machine)
  ((val :accessor val
        :initform 0)))

(defmethod commit-transaction ((self counter) (msg (eql :incr)))
  (log:info "running transaction")
  (incf (val self)))

(def-easy-macro with-peer-and-machines (&binding ports &binding machines
                                                 &key (num 3)  &fn fn)
  (with-logs-hidden ()
    (tmpdir:with-tmpdir (dir)
      (let* ((ports (loop for i below num
                          collect (util/random-port:random-port)))
             (config
               (str:join ","
                         (loop for port in ports
                               collect (format nil "127.0.0.1:~a:0" port))))
             (machines (loop for port in ports
                             for id from 0
                             collect (make-instance 'counter
                                                    :election-timeout-ms 100
                                                    :data-path (path:catdir dir (format nil "~a/" id))
                                                    :config config
                                                    :group "foobar"
                                                    :port port))))
        (fn ports machines)))))


(def-fixture cluster (&key (num 3))
  (with-peer-and-machines (peers machines :num num)
    (mapc #'start-up machines)
    (unwind-protect
         (&body)
      (mapc #'shutdown machines))))

(def-fixture follower ()
  (with-peer-and-machines (peers machines :num 3)
    (let ((follower (first machines)))
      (start-up follower)
      (unwind-protect
           (&body)
        (shutdown follower)))))

(defun find-leader (machines)
  (log:info "Got state: ~s" (mapcar #'leaderp machines))
  (loop for machine in machines
        if (leaderp machine)
          return machine))

(defun wait-for-leader (machines)
  (let ((start-time (get-universal-time))
          (max-time 10)
          (interval 0.1))
   (loop for i from 0 below (/ max-time interval)
         for machine = (find-leader machines)
         if machine
           return machine
         do
            (sleep interval))))

(test we-elect-a-leader-eventually
  (with-fixture cluster ()
    (is-true
     (wait-for-leader machines))))

(test simple-transaction
  (dotimes (i 2)
    (with-fixture cluster ()
      (let ((leader (wait-for-leader machines)))
        (is (eql 1 (apply-transaction leader :incr)))
        (is (eql 1 (val leader)))
        (flet ((count-replicated ()
                 (loop for machine in machines
                       if (eql 1 (val machine))
                          summing 1)))
          (loop for i from 0 to 100
                if (= (count-replicated) 3)
                  return (pass)
                else
                  do
                     (sleep 0.1)))))))

(test simple-transaction-on-single-machine-cluster
  (dotimes (i 2)
    (with-fixture cluster (:num 1)
      (let ((leader (wait-for-leader machines)))
        (apply-transaction leader :incr)
        (is (eql 1 (val leader)))
        (flet ((count-replicated ()
                 (loop for machine in machines
                       if (eql 1 (val machine))
                          summing 1)))
          (loop for i from 0 to 100
                if (= (count-replicated) 1)
                  return (pass)
                else
                  do
                     (sleep 0.1)))))))


(test make-lisp-state-machine
  (make-instance 'lisp-state-machine))


(defclass my-state-machine (lisp-state-machine)
  ())

(test start-lisp-state-machine
  (tmpdir:with-tmpdir (dir)
    (let* ((port (random-port))
           (self (make-instance 'my-state-machine
                                :port port
                                :config (format nil "127.0.0.1:~a:0" port)
                                :data-path dir
                                :group "foobar")))
      (log:info "Using port: ~a" port)
      (start-up self)
      (sleep 0.1) ;; todo: apply some transactions here.
      (shutdown self)
      (pass))))
