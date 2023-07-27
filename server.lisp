;;;; Copyright 2018-Present Modern Interpreters Inc.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this
;;;; file, You can obtain one at https://mozilla.org/MPL/2.0/.

(defpackage :bknr.cluster/server
  (:use #:cl)
  (:import-from #:bknr.datastore
                #:decode
                #:%write-char
                #:%decode-char
                #:encode
                #:encode-object
                #:encode-char)
  (:import-from #:util/threading
                #:ignore-and-log-errors
                #:make-thread)
  (:import-from #:easy-macros
                #:def-easy-macro)
  (:import-from #:flexi-streams
                #:vector-output-stream))
(in-package :bknr.cluster/server)

(defconstant +append-entries+ #\A)
(defconstant +request-vote+ #\V)
(defconstant +test-broadcast+ #\T)

(defconstant +protocol-version+ 1)

(defparameter *so-file*  #. (asdf:output-file 'asdf:compile-op (asdf:find-component :bknr.cluster "braft_compat")))

(fli:register-module
 :braft
 :connection-style :immediate
 :real-name
 "/usr/local/lib/libbraft.so")

(fli:register-module
 :braft-compat
 :connection-style :immediate
 :file-name
 *so-file*)

(defvar *lock* (bt:make-lock))

(fli:define-foreign-converter lisp-state-machine ()
  h
  :foreign-type '(:pointer bknr-state-machine)
  :foreign-to-lisp `(gethash ,h *state-machine-reverse-hash*)
  :lisp-to-foreign `(c-state-machine ,h))


(defvar *lisp-callback-counter* 0)

(defclass lisp-callback ()
  ((fn :initarg :fn
     :reader lisp-callback-fn)
   (id :initarg :id
       :initform (atomics:atomic-incf *lisp-callback-counter*)
       :reader lisp-callback-id)))

(defvar *lisp-callbacks* (make-hash-table))

(defmethod initialize-instance :after ((self lisp-callback) &key)
  (setf (gethash (lisp-callback-id self) *lisp-callbacks*)
        self))

(defmethod destroy-lisp-callback ((self lisp-callback))
  (remhash (lisp-callback-id self) *lisp-callbacks*))

(fli:define-foreign-converter lisp-callback ()
  h
  :foreign-type :int
  :foreign-to-lisp `(gethash ,h *lisp-callbacks*)
  :lisp-to-foreign `(lisp-callback-id ,h))

(defun reload-native ()
  (progn
    (fli:disconnect-module :braft-compat)
    (asdf:compile-system :bknr.cluster)))

(fli:define-c-struct bknr-state-machine
    (foo :int))

(fli:define-c-struct io-buf
    (foo :int))

(fli:define-c-struct closure
    (foo :int))

(fli:define-c-struct snapshot-writer
    (foo :int))

(fli:define-c-struct snapshot-reader
    (foo :int))

(fli:define-foreign-callable funcall-lisp-callback-with-str
    ((lisp-callback lisp-callback)
     (str (:pointer :char)))
  (let ((str (fli:convert-from-foreign-string str)))
    (funcall lisp-callback str)))

(fli:define-foreign-function make-bknr-state-machine
    ((on-apply-callback :pointer)
     (on-snapshot-save :pointer)
     (on-snapshot-load :pointer)
     (on-leader-start :pointer)
     (on-leader-stop :pointer)
     (funcall-lisp-callback-with-str :pointer))
  :result-type (:pointer bknr-state-machine)
  :module :braft-compat)

(fli:define-foreign-function destroy-bknr-state-machine
    ((fsm (:pointer bknr-state-machine)))
  :result-type :void)

(fli:define-foreign-function bknr-set-log-level
    ((level :int))
  :result-type :void)

;; For tests
(def-easy-macro with-logs-hidden (&fn fn)
  (bknr-set-log-level 3)
  (unwind-protect
       (fn)
    (bknr-set-log-level 1)))

(fli:define-foreign-function bknr-iobuf-copy-to
    ((iobuf (:pointer io-buf))
     (ptr :lisp-simple-1d-array)
     (len :int))
  :result-type :void)

(fli:define-foreign-function bknr-closure-run
    ((closure (:pointer closure)))
  :result-type :void)

(fli:define-foreign-callable
    (bknr-on-leader-start :result-type :void)
    ((fsm lisp-state-machine))
  (on-leader-start fsm))

(defmethod on-leader-start (fsm))

(fli:define-foreign-callable
    (bknr-on-leader-stop :result-type :void)
    ((fsm lisp-state-machine))
  (on-leader-stop fsm))

(defmethod on-leader-stop (fsm))

(fli:define-foreign-callable
    (bknr-on-apply-callback :result-type :void)
    ((fsm lisp-state-machine)
     (iobuf (:pointer io-buf))
     (data-len :int)
     (lisp-callback lisp-callback))
  (log:info "in on-apply-callable")
  (let ((arr (make-array data-len
                         :element-type '(unsigned-byte 8)
                         :allocation :static)))
    (log:info "going to copy")
    (bknr-iobuf-copy-to
     iobuf
     arr
     data-len)
    (log:info "calling commit transaction")

    (let ((res (commit-transaction
                fsm
                (decode (flex:make-in-memory-input-stream arr)))))
    (when lisp-callback
      (funcall (lisp-callback-fn lisp-callback) res)))))

(defvar *next-handle* 1)

(defclass lisp-state-machine ()
  ((c-state-machine
    :accessor c-state-machine)
   (ip :initarg :ip
       :initform "127.0.0.1")
   (port :initarg :port
         :initform 9090)
   (config :initarg :config)
   (election-timeout-ms :initarg :election-timeout-ms
                        :initform 1000)
   (snapshot-interval :initarg :snapshot-interval
                       :initform (* 24 30 60))
   (data-path :initarg :data-path
              :accessor data-path)
   (group :initarg :group
          :reader group)))

(defvar *state-machine-reverse-hash* (make-hash-table :test #'equalp))


(defmethod initialize-instance :after ((self lisp-state-machine) &key))

(fli:define-foreign-function start-bknr-state-machine
    ((sm (:pointer bknr-state-machine))
     (ip (:reference-pass :ef-mb-string))
     (port :int)
     (config (:reference-pass :ef-mb-string))
     (election-timeout-ms :int)
     (snapshot-interval :int)
     (data-path (:reference-pass :ef-mb-string))
     (group (:reference-pass :ef-mb-string)))
  :result-type :int)

(fli:define-foreign-function stop-bknr-state-machine
    ((sm (:pointer bknr-state-machine)))
  :result-type :void)

(fli:define-foreign-function bknr-is-leader
    ((sm (:pointer bknr-state-machine)))
  :result-type :int)

(defmethod leaderp ((self lisp-state-machine))
  (not (zerop (bknr-is-leader (c-state-machine self)))))

(defmethod start-up ((self lisp-state-machine))
  (allocate-fli self)
  (let ((res (apply #'start-bknr-state-machine
                    (c-state-machine self)
                    (loop for slot in '(ip
                                        port
                                        config
                                        election-timeout-ms
                                        snapshot-interval
                                        data-path
                                        group)
                          collect
                          (let ((res (slot-value self slot)))
                            (cond
                              ((pathnamep res)
                               (namestring res))
                              (t
                               res)))))))
    (unless (= 0 res)
      (error "Failed to start, got: ~a" res))))

(defun allocate-fli (self)
  (let ((fli (apply #'make-bknr-state-machine
                    (loop for callback in '(bknr-on-apply-callback
                                            bknr-snapshot-save
                                            bknr-snapshot-load
                                            bknr-on-leader-start
                                            bknr-on-leader-stop
                                            funcall-lisp-callback-with-str)
                          collect (fli:make-pointer :symbol-name callback)))))
    (setf (c-state-machine self) fli)
    (setf (gethash fli *state-machine-reverse-hash*)
          self)))


(defmethod shutdown ((self lisp-state-machine))
  (stop-bknr-state-machine
   (c-state-machine self))
  (remhash (c-state-machine self) *state-machine-reverse-hash*)
  (destroy-bknr-state-machine (c-state-machine self)))

(defvar *cv-map* (make-hash-table :weak-kind :value))
(defvar *next-cv-handle* 0)

(fli:define-foreign-function bknr-apply-transaction
    ((sm lisp-state-machine)
     (data :lisp-simple-1d-array )
     (data-len :int)
     (lisp-callback lisp-callback)
     (error-callback lisp-callback))
  :result-type :void)


(fli:define-foreign-callable (bknr-snapshot-save
                              :result-type :void)
    ((sm lisp-state-machine)
     (snapshot-writer (:pointer snapshot-writer))
     (done (:pointer closure))))

(fli:define-foreign-callable (bknr-snapshot-load
                              :result-type :void)
    ((sm lisp-state-machine)
     (snapshot-reader (:pointer snapshot-reader))))

(defgeneric commit-transaction (state-machine transaction))

(defmethod commit-transaction :around (sm trans)
  (handler-bind ((error (lambda (e)
                          (log:info "Got error: ~a" e)
                          (dbg:output-backtrace :verbose e))))
    (call-next-method)))

(defgeneric apply-transaction (state-machine transaction))

(defmethod apply-transaction ((self lisp-state-machine)
                              transaction)
  (let* ((stream (flex:make-in-memory-output-stream)))
    (encode transaction stream)

    (let ((data (flex:get-output-stream-sequence stream)))
      (let ((copy (make-array (length data)
                              :element-type '(unsigned-byte 8)
                              :adjustable nil
                              :initial-contents data
                              :allocation :static))
            (cv-handle (atomics:atomic-incf *next-cv-handle*))
            (cv (bt:make-condition-variable))
            (result nil))
        (setf (gethash cv-handle *cv-map*)
              cv)

        (log:info "Calling from thread: ~a" (bt:current-thread))
        (let ((lisp-callback
                (make-instance 'lisp-callback
                               :fn (lambda (this-result)
                                     (setf result this-result)
                                     (log:info "Result on thread: ~a" (bt:current-thread))
                                     (bt:with-lock-held (*lock*)
                                       (log:info "Got lock")
                                       (when cv
                                         (log:info "Got cv: ~a" cv)
                                         (bt:condition-notify cv))))))
              (error-callback
                (make-instance 'lisp-callback
                               :fn (lambda (msg)
                                     (log:error "Got result: ~a" msg)))))
          (unwind-protect
               (bt:with-lock-held (*lock*)
                 (bknr-apply-transaction
                  self
                  copy
                  (length copy)
                  lisp-callback
                  error-callback)
                 (unless (mp:condition-variable-wait cv *lock* :timeout 30)
                   (error "Transaction failed to apply in time"))
                 result)
            (destroy-lisp-callback lisp-callback)))))))
