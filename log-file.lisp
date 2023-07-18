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
                #:entry-data
                #:term
                #:log-entry)
  (:import-from #:bknr.cluster/util
                #:safely-ignore-errors)
  (:export
   #:open-log-file
   #:append-log-entry
   #:entry-at
   #:close-log-file
   #:end-index
   #:append-log-entries-overwriting-stale))
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
   (lock :initform (bt:make-recursive-lock)
         :reader lock)
   (start-index :initform 1
                :reader start-index)
   (end-index :initform 1
              :accessor end-index)
   (entries :initform (make-array 0 :adjustable 0
                                    :fill-pointer 0)
            :reader entries)))

(defun goto-end (self)
  (file-position (log-file-stream self) :end))

(defun goto-pos (self pos)
  (file-position (log-file-stream self) pos))

(defmethod append-log-entry ((self log-file)
                             term
                             data &key (at-end-p t))
  (bt:with-recursive-lock-held ((lock self))
    (let ((stream (log-file-stream self)))
      (when at-end-p
        (goto-end self))
      (log:debug "Appending entry ~a at ~a" term (file-position stream))
      (%write-tag +tag+ stream )
      (encode term stream)
      (add-pos-to-entries self term)
      (encode data stream)
      (finish-output stream)
      (incf (end-index self)))))

(defun add-pos-to-entries (self term)
  (let ((stream (log-file-stream self)))
   (vector-push-extend (make-entry :term term :pos (file-position stream))
                       (entries self))))

(defmethod goto-entry-data-pos ((self log-file)
                                index)
  "moves the stream to the position for the data, and returns the term
of the associated index as a convenience."
  (let ((entry (aref (entries self) (- index (start-index self)))))
    (goto-pos self (entry-pos entry))
    (entry-term entry)))

(defmethod entry-at ((self log-file)
                     index)
  (let ((stream (log-file-stream self)))
   (bt:with-recursive-lock-held ((lock self))
     (let ((term (goto-entry-data-pos self index)))
       (make-instance
        'log-entry
        :data (decode stream)
        :term term)))))

(defmethod term-at ((self log-file)
                    index)
  (bt:with-recursive-lock-held ((lock self))
   (cond
     ((= index 0)
      0)
     (t
      (when (and
             (<= (start-index self) index)
             (< index (end-index self)))
        (let ((entry (aref (entries self) (- index (start-index self)))))
          (entry-term entry)))))))

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
  (safely-ignore-errors ()
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

(defun goto-end-of-index (self index)
  "Move the file-position to the end of the item at index."
  (cond
    ((= 0 index)
     (goto-pos self :start))
    (t
     (goto-entry-data-pos self index)
     (decode (log-file-stream self)))))

(defmethod append-log-entries-overwriting-stale ((self log-file)
                                                 prev-log-index
                                                 entries)
  (block nil
   (bt:with-recursive-lock-held ((lock self))
     (loop for index from (1+ prev-log-index)
           for entry in entries
           for rest-entries on entries
           do
              (when (or
                     (= index (end-index self))
                     (not (eql (term-at self index) (term entry))))
                (goto-end-of-index self (1- index))
                (adjust-array
                 (entries self)
                 (1- index)
                 :fill-pointer (1- index))
                (%write-entries-now self
                                    rest-entries)
                (return-from nil nil))))))


(defun %write-entries-now (self entries)
  (loop for entry in entries
        do
           (append-log-entry
            self
            (term entry)
            (entry-data entry)
            :at-end-p nil)))
