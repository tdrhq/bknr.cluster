(defpackage :bknr.cluster/test-store
  (:use #:cl
        #:fiveam)
  (:import-from #:bknr.datastore
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
                #:with-logs-hidden))
(in-package :bknr.cluster/test-store)

(util/fiveam:def-suite)

(defclass foo (store-object)
  ((arg :initarg :arg
        :accessor arg))
  (:metaclass persistent-class))

(defclass my-test-store (cluster-store)
  ())

(def-fixture state ()
  (with-logs-hidden ()
   (tmpdir:with-tmpdir (dir)
     (let* ((port (util/random-port:random-port)))
       (unwind-protect
            (let ((store (make-instance 'my-test-store
                                        :election-timeout-ms 100
                                        :directory dir
                                        :group "dummy"
                                        :config (format nil "127.0.0.1:~a:0" port)
                                        :port port)))
              (&body))
         (close-store))))))

(test simple-creation
  (with-fixture state ()
    (let ((obj (make-instance 'foo)))
      (assert-that (class-instances 'foo)
                   (contains obj))
      (setf (arg obj) 2)
      (assert-that (arg obj)
                   (is-equal-to 2)))))
