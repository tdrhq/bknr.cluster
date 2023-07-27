(defpackage :bknr.cluster/test-store
  (:use #:cl
        #:fiveam)
  (:import-from #:bknr.datastore
                #:close-store-object
                #:*store*
                #:execute-transaction
                #:class-instances
                #:close-store
                #:persistent-class
                #:store-object)
  (:import-from #:fiveam-matchers/core
                #:is-equal-to
                #:assert-that)
  (:import-from #:fiveam-matchers/lists
                #:contains)
  (:import-from #:bknr.cluster/store
                #:cluster-store)
  (:import-from #:bknr.cluster/server
                #:leaderp
                #:with-logs-hidden)
  (:import-from #:util/store/store
                #:clear-indices-for-tests)
  (:import-from #:fiveam-matchers/has-length
                #:has-length))
(in-package :bknr.cluster/test-store)

(util/fiveam:def-suite)

(defclass foo (store-object)
  ((arg :initarg :arg
        :accessor arg))
  (:metaclass persistent-class))

(defclass my-test-store (cluster-store)
  ())

(defun safe-close-store ()
  (when (boundp '*store*)
   (let ((store *store*))
     (close-store)
     (close-store-object store))))


(def-fixture state ()
  (with-logs-hidden ()
   (tmpdir:with-tmpdir (dir)
     (let* ((port (util/random-port:random-port)))
       (unwind-protect
            (let (store)
              (labels ((restore ()
                         (safe-close-store)
                         (open-store))
                       (open-store ()
                         (setf store (make-instance 'my-test-store
                                        :election-timeout-ms 100
                                        :directory dir
                                        :group "dummy"
                                        :config (format nil "127.0.0.1:~a:0" port)
                                        :port port))
                         (loop while (not (leaderp store))
                               for i from 0 to 1000
                               do (sleep 0.1))))
                (open-store)
                (&body)))
         (safe-close-store))))))

(test simple-creation
  (with-fixture state ()
    (let ((obj (make-instance 'foo)))
      (assert-that (class-instances 'foo)
                   (contains obj))
      (setf (arg obj) 2)
      (assert-that (arg obj)
                   (is-equal-to 2)))))

(test simple-restore
  (with-fixture state ()
    (let ((obj (make-instance 'foo)))
      (assert-that (class-instances 'foo)
                   (contains obj))
      (safe-close-store)
      (assert-that (class-instances 'foo)
                   (contains))
      (open-store)
      (sleep 1) ;; TODO: remove
      (assert-that (class-instances 'foo)
                   (has-length 1)))))
