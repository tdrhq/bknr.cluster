;;;; Copyright 2018-Present Modern Interpreters Inc.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this
;;;; file, You can obtain one at https://mozilla.org/MPL/2.0/.

(defpackage :bknr.cluster/store
  (:use #:cl
        #:bknr.datastore)
  (:import-from #:bknr.datastore
                #:close-subsystem
                #:encode
                #:store-transaction-log-stream
                #:close-transaction-log-stream
                #:ensure-store-current-directory
                #:ensure-store-random-state
                #:store-random-state-pathname
                #:restore-store
                #:store-subsystem-snapshot-pathname
                #:restore-subsystem
                #:store-subsystems
                #:snapshot-subsystem
                #:update-store-random-state
                #:with-log-guard
                #:with-store-guard
                #:with-store-state
                #:snapshot-store
                #:store-directory
                #:close-store-object
                #:execute-unlogged
                #:execute-transaction)
  (:import-from #:bknr.cluster/server
                #:snapshot-reader-get-path
                #:bknr-snapshot-writer-add-file
                #:on-snapshot-load
                #:snapshot-writer-get-path
                #:on-snapshot-save
                #:with-closure-guard
                #:data-path
                #:shutdown
                #:start-up
                #:lisp-state-machine
                #:commit-transaction
                #:apply-transaction)
  (:export
   #:backward-compatibility-mixin))
(in-package :bknr.cluster/store)

(defclass cluster-store-mixin (lisp-state-machine)
  ())

(defclass cluster-store (cluster-store-mixin
                         store)
  ())

(defclass backward-compatibility-mixin ()
  ())



(defmethod initialize-instance :after ((self cluster-store-mixin) &key)
  (setf (data-path self)
        (path:catdir (store-directory self) "raft/"))
  ;; TODO: maybe move to restore-store?
  (start-up self))

(defmethod close-store-object :before ((self cluster-store-mixin))
  (shutdown self))

(defmethod execute-transaction ((store cluster-store-mixin)
                                transaction)
  (log:info "executing transaction here")
  (apply-transaction store
                     transaction))

(defmethod execute-transaction :after ((store backward-compatibility-mixin)
                                       transaction)
  (let ((stream (store-transaction-log-stream store)))
    (encode transaction stream)))

(defmethod close-store-object :after ((store backward-compatibility-mixin))
  (close-transaction-log-stream store))

(defmethod commit-transaction ((store cluster-store-mixin)
                               transaction)
  (log:info "Commiting transaction here")
  (handler-bind ((error (lambda (e)
                          (dbg:output-backtrace e))))
    (execute-unlogged transaction)))

(defmethod commit-transaction ((store cluster-store-mixin)
                               transaction)
  (log:info "Commiting transaction here")
  (handler-bind ((error (lambda (e)
                          (dbg:output-backtrace e))))
    (execute-unlogged transaction)))

(defvar *current-snapshot-dir*)

(defmethod store-subsystem-snapshot-pathname ((store cluster-store-mixin)
                                              subsystem)
  (store-subsystem-snapshot-pathname *current-snapshot-dir* subsystem))

(defmethod store-random-state-pathname ((store cluster-store-mixin))
  (merge-pathnames #P"random-state" *current-snapshot-dir*))

(defmethod ensure-store-current-directory ((store cluster-store-mixin))
  ;; ignore
  (values))

(defmethod restore-store ((self cluster-store-mixin) &key)
  (values))

(defmethod on-snapshot-save ((store cluster-store-mixin)
                             snapshot-writer
                             done)
  (with-closure-guard (done)
    (let ((path (ensure-directories-exist
                 (snapshot-writer-get-path snapshot-writer))))
      (with-store-state (:read-only store)
        (with-store-guard ()
          (with-log-guard ()
            (with-store-state (:snapshot)
              (flet ((add-pathname (pathname)
                       (let ((name (pathname-name pathname)))
                         (log:info "Adding ~a to snapshot" name)
                         (assert
                          (= 0
                             (bknr-snapshot-writer-add-file
                              snapshot-writer
                              name))))))
                (let ((*current-snapshot-dir* path))
                  (log:info "Got random-state pathname: ~a" (store-random-state-pathname store))
                  (ensure-store-random-state store)
                  (update-store-random-state store)
                  (add-pathname (store-random-state-pathname store))

                  (dolist (subsystem (store-subsystems store))
                    (snapshot-subsystem store subsystem)
                    (add-pathname (store-subsystem-snapshot-pathname store subsystem))))))))))))

(defmethod on-snapshot-load ((store cluster-store-mixin)
                             snapshot-reader)
  (maybe-close-subsystems store)
  (load-from-dir store (snapshot-reader-get-path snapshot-reader)))

(defmethod maybe-close-subsystems ((store cluster-store-mixin))
  "This could be called either in the beginning, or even halfway through
   a follower's life. So we must close any open subsystems. Every
   subsystem should be able to handle close-subsystem even if isn't
   restored (since it could happen if on an empty store that is being
   closed.), so we don't need an explicit check to make sure this has
   been opened."

  (dolist (subsystem (reverse (store-subsystems store)))
    (close-subsystem store subsystem)))

(defmethod load-from-dir ((store cluster-store-mixin)
                          dir)
  (with-store-state (:restore)
    (let ((*current-snapshot-dir* dir))
      (ensure-store-random-state store)
      (dolist (subsystem (store-subsystems store))
        (restore-subsystem store subsystem)))))

(defmethod restore-store :after ((store backward-compatibility-mixin) &key)
  (cond
    ((path:-d (path:catdir (store-directory store) "raft/"))
     (log:info "We found state associated with raft, we will not load the old store."))
    (t
     (load-from-dir
      store
      (path:catdir (store-directory store) "current/"))
     (with-store-state (:restore)
       (read-old-transaction-log store)))))

(defmethod read-old-transaction-log ((store backward-compatibility-mixin))
  "Reads the OLD format of the transaction log"
  (let ((log-file (path:catfile (store-directory store) "current/transaction-log")))
    (when (path:-e log-file)
      (bknr.datastore::load-transaction-log
       log-file))))


(defmethod snapshot-store ((store cluster-store-mixin))
  (bknr.cluster/server:snapshot store))
