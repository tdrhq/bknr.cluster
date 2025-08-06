;;;; Copyright 2018-Present Modern Interpreters Inc.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this
;;;; file, You can obtain one at https://mozilla.org/MPL/2.0/.

(defpackage :bknr.cluster/test-server
  (:use #:cl
        #:fiveam)
  (:import-from #:bknr.cluster/server
                #:transfer-to-random-peer
                #:list-peers
                #:activep
                #:with-closure-guard
                #:without-crashing
                #:*error-count*
                #:leader-id
                #:leader-term
                #:bknr-closure-run
                #:closure
                #:*lisp-closures*
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
  (:import-from #:fiveam-matchers/strings
                #:matches-regex
                #:starts-with)
  (:import-from #:fiveam-matchers/core
                #:has-typep
                #:assert-that)
  (:import-from #:fiveam-matchers/has-length
                #:has-length)
  (:import-from #:fiveam-matchers/lists
                #:contains-in-any-order)
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
                                                 &key (num 3)
                                                 lisp-thread-mode-p
                                                 &fn fn)
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
                                                    :lisp-thread-mode-p lisp-thread-mode-p
                                                    :data-path (path:catdir dir (format nil "~a/" id))
                                                    :config config
                                                    :group "foobar"
                                                    :port port))))
        (fn ports machines)))))


(def-fixture cluster (&key (num 3) (start-up t))
  (loop for lisp-thread-mode-p in (list nil t)
        do
           (with-peer-and-machines (peers machines :num num :lisp-thread-mode-p lisp-thread-mode-p)
             (when start-up
               (mapc #'start-up machines))
             (unwind-protect
                  (&body)
               (when start-up
                 (mapc #'shutdown machines))))))

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

(test activep
  (with-fixture cluster ()
    (let ((machine (wait-for-leader machines)))
      (is-true (activep machine)))))

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

(test leader-term-happy-path ()
  (with-fixture cluster ()
    (let ((leader (wait-for-leader machines)))
      (is (> (leader-term leader) -1)))))

(test leader-id-happy-path ()
  (with-fixture cluster ()
    (let ((leader (wait-for-leader machines)))
      (assert-that (leader-id leader)
                   (matches-regex "127.0.0.1:.*:0"))
      (let ((leader-id (leader-id leader)))
        (assert-that (mapcar #'leader-id machines)
                     (contains-in-any-order
                      leader-id
                      leader-id
                      (has-typep 'string)))))))

(test leader-id-responds-even-when-theres-no-leader
  (with-fixture cluster (:start-up nil)
    (start-up (first machines))
    (unwind-protect
         (is (equal nil (leader-id (first machines))))
      (shutdown (first machines)))))

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

(test cleans-up-state-properly
  (tmpdir:with-tmpdir (dir)
    (let* ((port (random-port)))
      (flet ((make-state-machine ()
               (make-instance 'my-state-machine
                              :port port
                              :config (format nil "127.0.0.1:~a:0" port)
                              :data-path dir
                              :group "foobar")))
        (let ((self (make-state-machine)))
          (LOG:info "Using port: ~a" port)
          (start-up self)
          (shutdown self)
          (let ((other (make-state-machine)))
            (start-up other)
            (shutdown other))
          (pass))))))

(test closures-are-deleted
  (clrhash *lisp-closures*)
  (let ((seen nil))
    (let ((closure (closure (res status err)
                     (declare (ignore res status err))
                     (setf seen t))))
      (is (eql 1 (hash-table-count *lisp-closures*)))
      (bknr-closure-run closure)
      (is-true seen)
      (is (eql 0 (hash-table-count *lisp-closures*))))))

(test without-crashing-updates-error-count
  (let ((*error-count* 0))
    (without-crashing ()
      (error "for test"))
    (is (eql 1 *error-count*))))

(test with-closure-guard
  (let* ((err 0)
         (success 0)
         (results nil)
         (closure (closure (result status msg)
                    (push status results)
                    (if status
                        (incf success)
                        (incf err)))))
    (with-closure-guard (closure)
      (values))
    (is (equal '(t) results))
    (is (eql 1 success))
    (is (eql 0 err))))

(test with-closure-guard-on-error
  (let* ((err 0)
         (success 0)
         (closure (closure (result status msg)
                    (if status
                        (incf success)
                        (incf err)))))
    (with-closure-guard (closure)
      (error "dummy"))
    (is (eql 1 err))
    (is (eql 0 success))))

(test list-peers
  (with-fixture cluster ()
    (let ((leader (wait-for-leader machines)))
      (assert-that (list-peers leader)
                   (has-length 3)))))

(test transfer-peer-happy-path
  (with-fixture cluster ()
    (let ((leader (wait-for-leader machines)))
      (transfer-to-random-peer leader)
      ;; the leader might not have immediately switched
      (loop for i below 1000
            while (equal leader (wait-for-leader machines))
            do (sleep 10))
      (let ((leader2 (wait-for-leader machines)))
        (is (not (equal leader2 leader)))))))
