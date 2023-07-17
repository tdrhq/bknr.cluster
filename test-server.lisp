;;;; Copyright 2018-Present Modern Interpreters Inc.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this
;;;; file, You can obtain one at https://mozilla.org/MPL/2.0/.

(defpackage :bknr.cluster/test-server
  (:use #:cl
        #:fiveam)
  (:import-from #:bknr.cluster/server
                #:commit-transaction
                #:apply-transaction
                #:state
                #:start-up
                #:state-machine
                #:shutdown
                #:peer))
(in-package :bknr.cluster/test-server)

(util/fiveam:def-suite)

(defclass counter (state-machine)
  ((val :accessor val
        :initform 0)))

(defmethod commit-transaction ((self counter) (msg (eql :incr)))
  (incf (val self)))

(def-fixture cluster (&key (num 3))
  (let* ((peers (loop for i below num
                      for id from 0
                      collect (make-instance 'peer
                                             :id id
                                             :port (util/random-port:random-port))))
         (machines (loop for peer in peers
                         collect (make-instance 'counter
                                                :election-timeout 0.1
                                                :peers peers
                                                :this-peer peer))))
    (mapc #'start-up machines)
    (unwind-protect
         (&body)
      (mapc #'shutdown machines))))

(defun find-leader (machines)
  (loop for machine in machines
        if (eql :leader (state machine))
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
  (with-fixture cluster ()
    (let ((leader (wait-for-leader machines)))
      (apply-transaction leader :incr)
      (is (eql 1 (val leader)))
      #+nil ;; todo
      (dolist (machine machines)
        (is (eql 1 (val machine)))))))
