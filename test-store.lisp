(defpackage :bknr.cluster/test-store
  (:use #:cl
        #:fiveam
        #:fiveam-matchers)
  (:import-from #:bknr.datastore
                #:restore-subsystem
                #:store-object-subsystem
                #:snapshot-subsystem-async
                #:store-subsystem-snapshot-pathname
                #:deftransaction
                #:mp-store
                #:snapshot
                #:close-store-object
                #:*store*
                #:execute-transaction
                #:class-instances
                #:close-store
                #:persistent-class
                #:store-object)
  (:import-from #:bknr.cluster/store
                #:copy-snapshot
                #:snapshot-backup-dir
                #:cluster-store-mixin
                #:maybe-close-subsystems
                #:backward-compatibility-mixin
                #:cluster-store)
  (:import-from #:bknr.cluster/server
                #:*error-count*
                #:leaderp
                #:with-logs-hidden)
  (:import-from #:util/store/store
                #:clear-indices-for-tests)
  (:import-from #:easy-macros
                #:def-easy-macro)
  (:import-from #:util/misc
                #:safe-with-open-file)
  (:import-from #:fiveam-matchers/strings
                #:contains-string))
(in-package :bknr.cluster/test-store)

(util/fiveam:def-suite)

(defvar *value* 2)

(defclass foo (store-object)
  ((arg :initarg :arg
        :accessor arg))
  (:metaclass persistent-class))

(defclass my-test-store (cluster-store)
  ())

(defclass backward-compatible-store (backward-compatibility-mixin
                                     cluster-store)
  ())

(defun safe-close-store ()
  (when (boundp '*store*)
   (close-store)))


(def-fixture state (&key (class 'my-test-store)
                    dir
                    (subsystems (list (make-instance 'store-object-subsystem))))
  (unwind-protect
       (let ((old-error-count *error-count*))
         (with-logs-hidden ()
           (flet ((do-work (dir)
                    (let* ((port (util/random-port:random-port)))
                      (unwind-protect
                           (let (store)
                             (labels ((restore ()
                                        (safe-close-store)
                                        (open-store))
                                      (open-store ()
                                        (setf store
                                              (make-instance class
                                                             :subsystems subsystems
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
                        (safe-close-store)))))
             (cond
               (dir
                (do-work dir))
               (t
                (tmpdir:with-tmpdir (dir :prefix "test-store")
                  (do-work dir)))))))
    (setf *value* 2)))

(test simple-creation
  (with-fixture state ()
    (let ((obj (make-instance 'foo)))
      (assert-that (class-instances 'foo)
                   (contains obj))
      (setf (arg obj) 2)
      (assert-that (arg obj)
                   (is-equal-to 2)))))

(test maybe-close-subsystems-when-its-already-open
  (with-fixture state ()
    (maybe-close-subsystems *store*)
    (finishes
     (maybe-close-subsystems *store*))))

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
      (is-false (path:-e (path:catfile dir "current/random-state")) )
      (safe-close-store)
      (assert-that (mapcar #'namestring (directory (path:catdir dir "raft/snapshot/")))
                   ;; in particular there shouldn't be a temp***:0 directory here.
                   (contains (matches-regex "snapshot_.*")))
      (assert-that (class-instances 'foo)
                   (contains))
      (open-store)
      (assert-that (class-instances 'foo)
                   (has-length 1)))))

(def-easy-macro with-tmp-store-dir (&binding dir &fn fn)
  (tmpdir:with-tmpdir (dir)
    (unwind-protect
         (funcall fn dir)
      (safe-close-store))))

(test backward-compatible-store-happy-path
  (with-tmp-store-dir (dir)
    (make-instance 'mp-store
                   :directory dir)
    (bknr.datastore:delete-object (make-instance 'foo))
    (safe-close-store)
    (with-fixture state (:class 'backward-compatible-store
                         :dir dir)
      (make-instance 'foo)
      (safe-close-store))
    (make-instance 'mp-store
                   :directory dir)
    (assert-that (class-instances 'foo)
                 (has-length 1))))


(test backward-compatible-store-also-reads-snapshots
  (with-tmp-store-dir (dir)
    (make-instance 'mp-store
                   :directory dir)
    (make-instance 'foo)
    (bknr.datastore:snapshot)
    (safe-close-store)
    (with-fixture state (:class 'backward-compatible-store
                         :dir dir)
      (assert-that (class-instances 'foo)
                   (has-length 1))
      (make-instance 'foo)
      (assert-that (class-instances 'foo)
                   (has-length 2))
      (safe-close-store))
    (make-instance 'mp-store
                   :directory dir)
    (assert-that (class-instances 'foo)
                 (has-length 2))))

(test backward-compatible-store-also-reads-old-transaction-log
  (with-tmp-store-dir (dir)
    (make-instance 'mp-store
                   :directory dir)
    (make-instance 'foo)
    (bknr.datastore:snapshot)
    (make-instance 'foo)
    (safe-close-store)
    (with-fixture state (:class 'backward-compatible-store
                         :dir dir)
      (assert-that (class-instances 'foo)
                   (has-length 2))
      (make-instance 'foo)
      (assert-that (class-instances 'foo)
                   (has-length 3))
      (safe-close-store))
    (make-instance 'mp-store
                   :directory dir)
    (assert-that (class-instances 'foo)
                 (has-length 3))))

(define-condition my-error (error)
  ((name :initarg :name)))

(deftransaction tx-crash-now (name)
  (log:info "about to crash")
  (error 'my-error :name name))

(test crashing-transaction
  (with-fixture state ()
    (signals my-error
      (tx-crash-now "foo"))
    (is (> *error-count* old-error-count))))

(test restoring-crashing-transaction
  (with-fixture state ()
    (signals my-error
     (tx-crash-now "foo"))
    (make-instance 'foo)
    (safe-close-store)
    (assert-that (class-instances 'foo)
                 (has-length 0))
    ;; Open store should open the store without errors!
    (open-store)
    (assert-that (class-instances 'foo)
                 (has-length 1))))

(test adding-new-slot-with-initform
  (with-fixture state ()
    (eval `(defclass my-class (store-object)
             ()
             (:metaclass persistent-class)))
    (let ((obj (eval `(make-instance 'my-class))))
      (is-true obj)
      (eval `(defclass my-class (store-object)
               ((foor :initform "car"))
               (:metaclass persistent-class)))
      (loop for i from 0 to 300
            until (slot-boundp obj 'foor)
            do (sleep 0.1))
      (is (equal "car" (slot-value obj 'foor))))))


(defclass fake-async-subsystem ()
  ())

(defmethod snapshot-subsystem-async ((store cluster-store-mixin)
                                     (self fake-async-subsystem))
  (let ((pathname (store-subsystem-snapshot-pathname store self)))
    (lambda ()
      #+nil ;; We don't want this blocking in tests
      (sleep 10)
      (with-open-file (output  pathname :direction :output
                                        :if-does-not-exist :create
                                        :if-exists :supersede)
        (format output "~a" *value*)))))

(defmethod restore-subsystem (store (self fake-async-subsystem) &key until)
  (declare (ignore until))
  (let ((pathname (store-subsystem-snapshot-pathname store self)))
    (setf *value* (parse-integer (uiop:read-file-string pathname)))))

(test actual-async-subsystem
  (with-tmp-store-dir (dir)
    (with-fixture state (:subsystems (list (make-instance 'fake-async-subsystem))
                         :dir dir)
      (setf *value* 3)
      (bknr.datastore:snapshot)
      (setf *value* 2)
      (restore)
      (is (eql 3 *value*)))))

(test snapshot-backup-dir
  (with-fixture state ()
    (assert-that (namestring (snapshot-backup-dir store))
                 (contains-string "127.0.0.1:"))))

(test copy-snapshot
  (with-fixture state ()
    (finishes
     (copy-snapshot store))))
