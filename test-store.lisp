(defpackage :bknr.cluster/test-store
  (:use #:cl
        #:fiveam
        #:fiveam-matchers)
  (:import-from #:bknr.datastore
                #:snapshot
                #:close-store-object
                #:*store*
                #:execute-transaction
                #:class-instances
                #:close-store
                #:persistent-class
                #:store-object)
  (:import-from #:bknr.cluster/store
                #:cluster-store)
  (:import-from #:bknr.cluster/server
                #:leaderp
                #:with-logs-hidden)
  (:import-from #:util/store/store
                #:clear-indices-for-tests))
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
   (close-store)))


(def-fixture state ()
  (with-logs-hidden ()
   (tmpdir:with-tmpdir (dir :prefix "test-store")
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
      (assert-that (class-instances 'foo)
                   (has-length 1)))))

(test snapshot-and-restore
  (with-fixture state ()
    (let ((obj (make-instance 'foo)))
      (assert-that (class-instances 'foo)
                   (contains obj))
      (snapshot)
      (is-false (path:-d (path:catdir dir "current/")) )
      (safe-close-store)
      (assert-that (mapcar #'namestring (directory (path:catdir dir "snapshot/")))
                   ;; in particular there shouldn't be a temp***:0 directory here.
                   (contains (matches-regex "snapshot_.*")))
      (assert-that (class-instances 'foo)
                   (contains))
      (open-store)
      (assert-that (class-instances 'foo)
                   (has-length 1)))))
