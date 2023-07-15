;;;; Copyright 2018-Present Modern Interpreters Inc.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this
;;;; file, You can obtain one at https://mozilla.org/MPL/2.0/.

(defpackage :bknr.cluster/rpc
  (:use #:cl)
  (:export
   #:request-vote
   #:request-vote-result
   #:append-entries
   #:append-entries-result
   #:term
   #:candidate-id
   #:last-log-index
   #:last-log-term
   #:leader-id
   #:successp))
(in-package :bknr.cluster/rpc)

(defclass base-rpc-object ()
  ())

(defclass request-vote (base-rpc-object)
  ((term :initarg :term
         :reader term)
   (candidate-id :initarg :candidate-id
                 :reader candidate-id)
   (last-log-index :initarg :last-log-index
                   :reader last-log-index)
   (last-log-term :initarg :last-log-term
                  :reader last-log-term)))

(defclass request-vote-result (base-rpc-object)
  ((term :initarg :term
         :reader term)
   (vote-granted-p :initarg :vote-granted-p)))

(defclass append-entries (base-rpc-object)
  ((term :initarg :term
         :reader term)
   (leader-id :initarg :leader-id
              :reader leader-id)
   (prev-log-index :initarg :prev-log-index)
   (prev-log-term :initarg :prev-log-term)
   (entries :initarg :entries)
   (leader-commit :initarg :leader-commit)))

(defclass append-entries-result (base-rpc-object)
  ((term :initarg :term
         :reader term)
   (successp :initarg :successp
             :reader successp)))
