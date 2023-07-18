;;;; Copyright 2018-Present Modern Interpreters Inc.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this
;;;; file, You can obtain one at https://mozilla.org/MPL/2.0/.

(defpackage :bknr.cluster/test-server
  (:use #:cl
        #:fiveam)
  (:import-from #:bknr.cluster/server
                #:log-file
                #:handle-rpc
                #:commit-transaction
                #:apply-transaction
                #:state
                #:start-up
                #:state-machine
                #:shutdown
                #:peer)
  (:import-from #:easy-macros
                #:def-easy-macro)
  (:import-from #:bknr.cluster/rpc
                #:append-entries
                #:log-entry)
  (:import-from #:bknr.cluster/log-file
                #:term-at)
  (:import-from #:bknr.cluster/transport
                #:transport)
  (:export
   #:with-peer-and-machines))
(in-package :bknr.cluster/test-server)

(util/fiveam:def-suite)

(defclass counter (state-machine)
  ((val :accessor val
        :initform 0)))

(defmethod commit-transaction ((self counter) (msg (eql :incr)))
  (incf (val self)))

(def-easy-macro with-peer-and-machines (&binding peers &binding machines
                                                 &key (num 3) (transport 'transport) &fn fn)
  (tmpdir:with-tmpdir (dir)
   (let* ((peers (loop for i below num
                       for id from 0
                       collect (make-instance 'peer
                                              :id id
                                              :port (util/random-port:random-port))))
          (machines (loop for peer in peers
                          for id from 0
                          collect (make-instance 'counter
                                                 :election-timeout 0.1
                                                 :directory (path:catdir dir (format nil "~a/" id))
                                                 :transport (make-instance transport)
                                                 :peers peers
                                                 :this-peer peer))))
     (fn peers machines))))


(def-fixture cluster (&key (num 3))
  (with-peer-and-machines (peers machines :num 3)
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

(test handle-append-entries-on-empty-state
  (with-fixture follower ()
    (is-false (term-at (log-file follower) 1))
    (handle-rpc follower
                (make-instance 'append-entries
                               :term 1
                               :leader-id 2
                               :prev-log-index 0
                               :prev-log-term 0
                               :entries (list
                                         (make-instance 'log-entry
                                                        :term 1
                                                        :data #(1 2 3)))
                               :leader-commit 0))
    (is (eql 1 (term-at (log-file follower) 1)))))
