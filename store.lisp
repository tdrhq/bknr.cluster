;;;; Copyright 2018-Present Modern Interpreters Inc.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this
;;;; file, You can obtain one at https://mozilla.org/MPL/2.0/.

(defpackage :bknr.cluster/store
  (:use #:cl
        #:bknr.datastore)
  (:import-from #:bknr.datastore
                #:store-directory
                #:close-store-object
                #:execute-unlogged
                #:execute-transaction)
  (:import-from #:bknr.cluster/server
                #:data-path
                #:shutdown
                #:start-up
                #:lisp-state-machine
                #:commit-transaction
                #:apply-transaction))
(in-package :bknr.cluster/store)

(defclass cluster-store-mixin (lisp-state-machine)
  ())

(defclass cluster-store (cluster-store-mixin
                         store)
  ())

(defmethod initialize-instance :after ((self cluster-store-mixin) &key)
  (setf (data-path self)
        (store-directory self))
  (start-up self))

(defmethod close-store-object :before ((self cluster-store-mixin))
  (shutdown self))


(defmethod execute-transaction ((store cluster-store-mixin)
                                transaction)
  (log:info "executing transaction here")
  (apply-transaction store
                     transaction))


(defmethod commit-transaction ((store cluster-store-mixin)
                               transaction)
  (log:info "Commiting transaction here")
  (handler-bind ((error (lambda (e)
                          (dbg:output-backtrace e))))
      (execute-unlogged transaction)))
