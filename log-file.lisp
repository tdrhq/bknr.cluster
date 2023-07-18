;;;; Copyright 2018-Present Modern Interpreters Inc.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this
;;;; file, You can obtain one at https://mozilla.org/MPL/2.0/.

(defpackage :bknr.cluster/log-file
  (:use #:cl)
  (:import-from #:bknr.datastore
                #:%read-tag
                #:decode
                #:encode
                #:%write-tag
                #:decode-object)
  (:import-from #:bknr.cluster/rpc
                #:log-entry)
  (:export
   #:open-log-file
   #:append-log-entry
   #:entry-at
   #:close-log-file
   #:end-index))
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
   (start-index :initform 1
                :reader start-index)
   (end-index :initform 1
              :reader end-index)
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
      (add-pos-to-entries self term)
      (encode data stream)
      (finish-output stream))))

(defun add-pos-to-entries (self term)
  (let ((stream (log-file-stream self)))
   (vector-push-extend (make-entry :term term :pos (file-position stream))
                       (entries self))))

(defmethod entry-at ((self log-file)
                     index)
  (let ((stream (log-file-stream self)))
   (bt:with-lock-held ((lock self))
     (let ((entry (aref (entries self) (- index (start-index self)))))
       (goto-pos self (entry-pos entry))
       (make-instance
        'log-entry
        :data (decode stream)
        :term (entry-term entry))))))

(defmethod term-at ((self log-file)
                    index)
  (cond
    ((= index 0)
     0)
    (t
     (when (and
            (<= (start-index self) index)
            (< index (end-index self)))
       (let ((entry (aref (entries self) (- index (start-index self)))))
         (entry-term entry))))))

(defun open-log-file (&key pathname (type 'log-file))
  (let ((log-file
          (make-instance type
                         :pathname (ensure-directories-exist pathname)
                         :stream (open pathname :direction :io
                                                :element-type '(unsigned-byte 8)
                                                :if-exists :append
                                                :if-does-not-exist :create))))
    (read-log-entries log-file)
    log-file))

(defmethod ignore-decoding-errors ((self log-file)
                                   fn)
  (ignore-errors
   (funcall fn)))

(defmethod read-log-entries ((self log-file))
  (unwind-protect
       (let ((stream (log-file-stream self)))
         (file-position stream :start)
         ;; TODO: we could ignore-errors this to handle incompletely
         ;; written log files.
         (ignore-decoding-errors
          self
          (lambda ()
           (loop
             (let ((tag (%read-tag stream nil)))
               (unless tag
                 (return-from read-log-entries nil))
               (assert (eql +tag+ tag)))
             (let ((term (decode stream)))
               (add-pos-to-entries self term))
             ;; Decode and discard the the log data
             (decode stream)))))
    (goto-end self)))

(defmethod close-log-file ((self log-file))
  (close (log-file-stream self)))

(defmethod read-entries ((self log-file) start &optional end)
  (loop for i from start below (or end (end-index self))
        collect (entry-at self i)))
