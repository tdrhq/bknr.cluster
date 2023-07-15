;;;; Copyright 2018-Present Modern Interpreters Inc.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this
;;;; file, You can obtain one at https://mozilla.org/MPL/2.0/.

(defpackage :bknr.cluster/rpc
  (:use #:cl)
  (:import-from #:bknr.datastore
                #:%read-tag
                #:%write-tag
                #:decode
                #:encode
                #:%read-char
                #:decode-object
                #:%write-char
                #:encode-object)
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

(defmacro with-methods ((code) &body classes)
  (let* ((class (car classes))
         (class-name (second class))
         (slots (mapcar #'first (fourth class))))
    `(progn
       ,class
       (defmethod encode-rpc-object ((obj ,class-name) stream)
         (%write-tag ,code stream)
         (with-slots ,slots obj
           ,@ (loop for slot in slots
                    collect
                    `(encode
                      (cond
                        ((slot-boundp ',slot obj)
                         ,slot)
                        (t
                         nil))
                      stream))))

       (defmethod decode-rpc-object ((code (eql ,code))
                                     stream)
         (let ((obj (make-instance ',class-name)))
           (prog1
               obj
             (with-slots ,slots obj
               ,@ (loop for slot in slots
                        collect
                        `(setf ,slot (decode stream))))))))))

(with-methods (#\V)
  (defclass request-vote (base-rpc-object)
    ((term :initarg :term
           :initform nil
           :reader term)
     (candidate-id :initarg :candidate-id
                   :initform nil
                   :reader candidate-id)
     (last-log-index :initarg :last-log-index
                     :initform nil
                     :reader last-log-index)
     (last-log-term :initarg :last-log-term
                    :initform nil
                    :reader last-log-term))))

(with-methods (#\v)
 (defclass request-vote-result (base-rpc-object)
   ((term :initarg :term
          :initform nil
          :reader term)
    (vote-granted-p :initarg :vote-granted-p
                    :initform nil))))

(with-methods (#\A)
 (defclass append-entries (base-rpc-object)
   ((term :initarg :term
          :initform nil
          :reader term)
    (leader-id :initarg :leader-id
               :initform nil
               :reader leader-id)
    (prev-log-index :initarg :prev-log-index
                    :initform nil)
    (prev-log-term :initarg :prev-log-term
                   :initform nil)
    (entries :initarg :entries
             :initform nil)
    (leader-commit :initarg :leader-commit
                   :initform nil))))

(with-methods (#\a)
  (defclass append-entries-result (base-rpc-object)
    ((term :initarg :term
           :initform nil
           :reader term)
     (successp :initarg :successp
               :initform nil
               :reader successp))))

(defmethod encode-object ((self base-rpc-object)
                          stream)
  (%write-tag #\R stream)
  (encode-rpc-object self stream))

(defmethod decode-object ((code (eql #\R))
                          stream)
  (decode-rpc-object (%read-tag stream)
                     stream))
