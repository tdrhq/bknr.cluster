;;;; Copyright 2018-Present Modern Interpreters Inc.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this
;;;; file, You can obtain one at https://mozilla.org/MPL/2.0/.

(defpackage :bknr.cluster/store
  (:use #:cl
        #:bknr.datastore)
  (:import-from #:bknr.datastore
                #:snapshot-subsystem-async
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
                #:lisp-state-machine-port
                #:lisp-state-machine-ip
                #:bknr-closure-run
                #:bknr-closure-set-error-from-error
                #:leaderp
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
   #:backward-compatibility-mixin
   #:on-snapshot-save-impl))
(in-package :bknr.cluster/store)

(pushnew :bknr.cluster *features*)

(defclass cluster-store-mixin (lisp-state-machine)
  ((file-lock :initarg :file-lock
              :accessor file-lock)))

(defclass cluster-store (cluster-store-mixin
                         store)
  ())

(defclass backward-compatibility-mixin (cluster-store-mixin)
  ())



(defmethod initialize-instance :around ((self cluster-store-mixin) &rest args &key data-path directory)
  (cond
    (data-path
     (call-next-method))
    (t
     ;; The raft data-path may not be in the store object directory! In
     ;; that case the user might have specified :data-path while
     ;; creating this object.
     (apply #'call-next-method self :data-path (path:catdir directory "raft/")
            args)))
  ;; TODO: maybe move to restore-store?
  (setf (file-lock self) (file-lock:make-file-lock :file
                                                   (ensure-directories-exist
                                                    (path:catfile
                                                     (data-path self)
                                                     "store.lock"))))
  (start-up self))

(defmethod close-store-object :before ((self cluster-store-mixin))
  (file-lock:release-file-lock (file-lock self))
  (shutdown self))

(defmethod execute-transaction ((store cluster-store-mixin)
                                transaction)
  #+nil(log:trace "executing transaction here")
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
  (execute-unlogged transaction))

(defvar *current-snapshot-dir*)

(defmethod store-subsystem-snapshot-pathname ((store cluster-store-mixin)
                                              subsystem)
  (store-subsystem-snapshot-pathname *current-snapshot-dir* subsystem))

(defmethod store-random-state-pathname ((store cluster-store-mixin))
  (merge-pathnames #P"random-state" *current-snapshot-dir*))

;; Not sure if I need to do this. store-version subsystem still uses
;; this directory, and I'm not sure if something else does too.
#+nil
(defmethod ensure-store-current-directory ((store cluster-store-mixin))
  ;; ignore
  (values))

(defmethod restore-store ((self cluster-store-mixin) &key)
  (values))

(defmethod leaderp ((store store))
  ;; a non fsm
  t)

(defmethod on-snapshot-save-impl (store snapshot-writer done)
  "If you need to add any instrumentation around snapshotting, use this
function instead of on-snapshot-save, since it will better handle errors"
  (let ((path (ensure-directories-exist
               (snapshot-writer-get-path snapshot-writer)))
        (callbacks nil)
        (start-time (get-universal-time)))
    (flet ((add-pathname (pathname)
             (let ((name (pathname-name pathname)))
               (log:info "Adding ~a to snapshot" name)
               (assert
                (= 0
                   (bknr-snapshot-writer-add-file
                    snapshot-writer
                    name))))))
      (with-store-state (:read-only store)
        (with-store-guard ()
          (with-log-guard ()
            (with-store-state (:snapshot)
              (let ((*current-snapshot-dir* path))
                (log:info "Got random-state pathname: ~a" (store-random-state-pathname store))
                (ensure-store-random-state store)
                (update-store-random-state store)
                (add-pathname (store-random-state-pathname store))

                (dolist (subsystem (store-subsystems store))
                  (let ((callback (snapshot-subsystem-async store subsystem)))
                    (push (list
                           callback
                           (store-subsystem-snapshot-pathname store subsystem))
                          callbacks))))))))
      (log:info "Synchronous part of the snapshot took ~as" (- (get-universal-time) start-time))
      (bt:make-thread
       (lambda ()
         (handler-case
             (progn
               (loop for (callback pathname) in (reverse callbacks)
                     do
                        (funcall callback)
                        ;; Some subsystems don't actually make a snapshot
                        (when (path:-e pathname)
                          (add-pathname pathname))))
           (error (e)
             (bknr-closure-set-error-from-error done e)))
         (bknr-closure-run done))))))

(defmethod on-snapshot-save ((store cluster-store-mixin)
                             snapshot-writer
                             done)
  (handler-case
      (on-snapshot-save-impl store snapshot-writer done)
    (error (e)
      (bknr-closure-set-error-from-error done e)
      (bknr-closure-run done))))

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
    ((path:-d (data-path store))
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
  (bknr.cluster/server:snapshot store)
  #+linux
  (copy-snapshot store))

(defun snapshot-backup-dir (store)
  (path:catdir (store-directory store) "snapshots/"
               (format nil "./~a:~a/"
                       (lisp-state-machine-ip store)
                       (lisp-state-machine-port store))))

(defmethod copy-snapshot ((store cluster-store-mixin))
  (let ((output (ensure-directories-exist
                 (snapshot-backup-dir store))))
    (uiop:run-program
     (list "tar" "czf"
           (namestring
            (make-pathname
             :type "tar.gz"
             :name (format nil "~a" (local-time:now))
             :defaults output))
           "-C" (namestring (data-path store))
           ;; Avoid having to tar something that's changing while
           ;; we're tarring.
           "--exclude" "raft_meta"
           "--exclude" "log"
           "."
           ".")
     :output t
     :error-output t)))
