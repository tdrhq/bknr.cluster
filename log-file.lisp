;;;; Copyright 2018-Present Modern Interpreters Inc.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this
;;;; file, You can obtain one at https://mozilla.org/MPL/2.0/.

(defpackage :bknr.cluster/log-file
  (:use #:cl)
  (:import-from #:bknr.datastore
                #:decode
                #:encode
                #:%write-tag
                #:decode-object)
  (:export
   #:open-log-file
   #:append-log-entry
   #:entry-at
   #:close-log-file))
(in-package :bknr.cluster/log-file)

(defconstant +tag+ #\L
  "A fake tag. We don't really need this, but this lets us read the log
file in a manner compatible with the old datastore. We would just have
to disregard the term.")

(defmethod decode-object ((tag (eql +tag+)) stream)
  (error "unimpl tag #\L"))

(defstruct entry
  term
  pos)

(defclass log-file ()
  ((pathname :initarg :pathname
             :reader log-file-pathname)
   (stream :initarg :stream
           :reader log-file-stream)
   (lock :initform (bt:make-lock)
         :reader lock)
   (entries :initform (make-array 0 :adjustable 0
                                    :fill-pointer 0)
            :reader entries)))

(defun goto-end (self)
  (file-position (log-file-stream self) :end))

(defun goto-pos (self pos)
  (file-position (log-file-stream self) pos))

(defmethod append-log-entry ((self log-file)
                             term
                             data)
  (let ((stream (log-file-stream self)))
    (bt:with-lock-held ((lock self))
      (goto-end self)
      (%write-tag +tag+ stream )
      (encode term stream)
      (vector-push-extend (make-entry :term term :pos (file-position stream))
                          (entries self))
      (encode data stream))))

(defmethod entry-at ((self log-file)
                     index)
  (let ((stream (log-file-stream self)))
   (bt:with-lock-held ((lock self))
     (let ((entry (aref (entries self) (1- index))))
       (goto-pos self (entry-pos entry))
       (values
        (decode stream)
        (entry-term entry))))))

(defun open-log-file (&key pathname)
  (make-instance 'log-file
                 :pathname pathname
                 :stream (open pathname :direction :io
                                        :element-type '(unsigned-byte 8)
                                        :if-exists :append
                                        :if-does-not-exist :create)))

(defmethod close-log-file ((self log-file))
  (close (log-file-stream self)))
