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
                #:vector-output-stream)
  (:export
   #:snapshot
   #:with-closure-guard
   #:snapshot-writer-get-path))
(in-package :bknr.cluster/server)

(defconstant +append-entries+ #\A)
(defconstant +request-vote+ #\V)
(defconstant +test-broadcast+ #\T)

(defconstant +protocol-version+ 1)

;; Sync with util/misc
(eval-when (:compile-toplevel)
  (defun relpath (path start)
    (assert (fad:pathname-absolute-p path))
    (assert (fad:pathname-absolute-p start))

    (labels ((compute (path start)
               (cond
                 ((equal (car path) (car start))
                  (compute (cdr path) (cdr start)))
                 ((not start)
                  path)
                 (t
                  (compute (list* ".." path)
                           (cdr start))))))
      (make-pathname
       :directory
       (list* :relative
              (compute (pathname-directory path) (pathname-directory start)))
       :defaults path))))


(defparameter *so-file*  #. (relpath (asdf:output-file 'asdf:compile-op (asdf:find-component :bknr.cluster "braft_compat"))
                                               (uiop:getcwd)))

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
(defvar *leader-cv* (bt:make-condition-variable))

(fli:define-foreign-converter lisp-state-machine ()
  h
  :foreign-type '(:pointer bknr-state-machine)
  :foreign-to-lisp `(gethash ,h *state-machine-reverse-hash*)
  :lisp-to-foreign `(c-state-machine ,h))


(defvar *lisp-closure-counter* 0)

(defclass lisp-closure ()
  ((fn :initarg :fn
     :reader lisp-closure-fn)
   (result :accessor lisp-closure-result
           :initform nil)
   (%error :accessor lisp-closure-error
           :initform nil)
   (foreign :initarg :foreign
            :accessor lisp-closure-foreign)))

(defclass transient-lisp-closure ()
  ((foreign :initarg :foreign
            :accessor lisp-closure-foreign))
  (:documentation "If braft creates a closure and passes it to us, then this is what we
do. In this case this closure is only valid in the dynamic extent, and maybe even less."))

(defvar *lisp-closures* (make-hash-table :test #'equalp))

(fli:define-foreign-function bknr-make-closure
    ((invoke-closure :pointer)
     (delete-closure :pointer))
  :result-type :pointer)

(def-easy-macro closure (&binding result &binding status &binding msg &fn fn)
  (let ((foreign
          (bknr-make-closure
           (fli:make-pointer :symbol-name 'bknr-invoke-closure)
           (fli:make-pointer :symbol-name 'bknr-delete-closure))))
    (make-instance 'lisp-closure
                   :foreign foreign
                   :fn fn)))

(defmethod initialize-instance :after ((self lisp-closure) &key)
  (setf (gethash (fli:pointer-address (lisp-closure-foreign self)) *lisp-closures*)
        self))

(defun find-lisp-closure-for-pointer (h)
  (log:info "Finding lisp closure for: ~a" h)
  (gethash (fli:pointer-address h) *lisp-closures*))

(fli:define-foreign-converter lisp-closure ()
  h
  :foreign-type `(:pointer closure)
  :foreign-to-lisp `(find-lisp-closure-for-pointer ,h)
  :lisp-to-foreign `(lisp-closure-foreign ,h))


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

(fli:define-foreign-callable bknr-invoke-closure
    ((bknr-closure (:pointer closure))
     (status :int)
     (str (:pointer :char)))
  (let ((lisp-closure (find-lisp-closure-for-pointer bknr-closure)))
   (cond
     ((not lisp-closure)
      (log:warn "invoke-closure called on deleted closure: ~a" bknr-closure))
     (t
      (handler-bind ((error (lambda (e)
                              (dbg:output-backtrace :verbose t))))
        (let ((str (fli:convert-from-foreign-string str)))
          (funcall (lisp-closure-fn lisp-closure)
                   (lisp-closure-result lisp-closure)
                   (> status 0)
                   str)))))))

(fli:define-foreign-callable bknr-delete-closure
    ((bknr-closure :pointer))
  (log:info "Deleteing closure at :~s" bknr-closure)
  (remhash (fli:pointer-address bknr-closure) *lisp-closures*))

(fli:define-foreign-function make-bknr-state-machine
    ((on-apply-callback :pointer)
     (on-snapshot-save :pointer)
     (on-snapshot-load :pointer)
     (on-leader-start :pointer)
     (on-leader-stop :pointer))
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
    ((closure lisp-closure))
  :result-type :void)

(fli:define-foreign-function bknr-closure-set-error
    ((closure lisp-closure)
     (err :int)
     (msg (:reference-pass :ef-mb-string)))
  :result-type :void)

(fli:define-foreign-callable
    (bknr-on-leader-start :result-type :void)
    ((fsm lisp-state-machine))
  (on-leader-start fsm))

(defmethod on-leader-start (fsm)
  (log:info "on-leader-start called")
  (bt:with-lock-held (*lock*)
    (setf (%leaderp fsm) t)
    (bt:condition-notify *leader-cv*)))

(fli:define-foreign-callable
    (bknr-on-leader-stop :result-type :void)
    ((fsm lisp-state-machine))
  (on-leader-stop fsm))

(defmethod on-leader-stop (fsm)
  (log:info "on-leader-stop called")
  (bt:with-lock-held (*lock*)
    (setf (%leaderp fsm) nil)
    (bt:condition-notify *leader-cv*)))

(def-easy-macro without-crashing (&key (error 0) (success 1) tag &fn fn)
  (handler-case
      (handler-bind ((error (lambda (e)
                              (format t "Got error for ~a: ~a " tag e)
                              (dbg:output-backtrace :brief t))))
        (fn)
        success)
    (error ()
      (log:info "ignoring crash for in ~a" tag)
      error)))


(defvar *next-handle* 1)

(defclass lisp-state-machine ()
  ((c-state-machine
    :accessor c-state-machine)
   (ip :initarg :ip
       :initform "127.0.0.1")
   (port :initarg :port
         :initform 9090)
   (leaderp :initform nil
            :reader leaderp
            :writer (setf %leaderp))
   (config :initarg :config)
   (election-timeout-ms :initarg :election-timeout-ms
                        :initform 1000)
   (snapshot-interval :initarg :snapshot-interval
                       :initform (* 24 30 60))
   (data-path :initarg :data-path
              :initform nil
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
                               (namestring (ensure-directories-exist res)))
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
                                            bknr-on-leader-stop)
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
     (closure lisp-closure))
  :result-type :void)


(fli:define-foreign-callable (bknr-snapshot-save
                              :result-type :void)
    ((sm lisp-state-machine)
     (snapshot-writer (:pointer snapshot-writer))
     (done (:pointer :closure) #| this may not be a lisp closure! |#))
  (on-snapshot-save sm snapshot-writer
                    (make-instance
                     'transient-lisp-closure
                     :foreign done)))

(fli:define-foreign-callable (bknr-snapshot-load
                              :result-type :int)
    ((sm lisp-state-machine)
     (snapshot-reader (:pointer snapshot-reader)))
  ;; returns 0 for success, 1 for fail
  (without-crashing (:error 1 :success 0 :tag "snapshot-load")
   (on-snapshot-load sm snapshot-reader)))

(def-easy-macro with-closure-guard (closure &fn fn)
  (assert closure)
  (unwind-protect
       (handler-case
           (funcall fn)
         (error (e)
           (bknr-closure-set-error closure 1
                                   (format nil "Failed with: ~a" e))))
    (log:info "Running closure guard run")
    (bknr-closure-run closure)))

(fli:define-foreign-callable
    (bknr-on-apply-callback :result-type :int)
    ((fsm lisp-state-machine)
     (iobuf (:pointer io-buf))
     (data-len :int)
     (closure lisp-closure))
  (log:info "in on-apply-callable")
  (flet ((run ()
           (let ((arr (make-array data-len
                                  :element-type '(unsigned-byte 8)
                                  :allocation :static)))
             (log:info "going to copy")
             (bknr-iobuf-copy-to
              iobuf
              arr
              data-len)
             (log:info "calling commit transaction")

             (let ((transaction (decode (flex:make-in-memory-input-stream arr))))
               (log:info "Going to commit: ~s" transaction)
               (let ((res (commit-transaction
                           fsm
                           transaction)))
                 (log:info "commit transaction callback done")
                 res)))))
    (cond
      (closure
          (with-closure-guard (closure)
            (handler-case
                (setf (lisp-closure-result closure)
                      (run))
              (error (e)
                (bknr-closure-set-error closure 1 (format nil "~a" e))
                (setf (lisp-closure-error closure) e)))))
      (t
       (ignore-errors
           (run))))
    1))


(defgeneric on-snapshot-load (fsm snapshot-reader))

(defgeneric on-snapshot-save (fsm snapshot-writer done-closure))

(defgeneric commit-transaction (state-machine transaction))

(defmethod commit-transaction :around (sm trans)
  (call-next-method))

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
            (result nil)
            (error-msg nil))
        (setf (gethash cv-handle *cv-map*)
              cv)

        (log:info "Calling from thread: ~a" (bt:current-thread))
        (let ((closure
                (closure (this-result status msg)
                  (cond
                    (status
                     (setf result this-result)
                     (log:info "Result on thread: ~a" (bt:current-thread))
                     (bt:with-lock-held (*lock*)
                       (log:info "Got lock")
                       (when cv
                         (log:info "Got cv: ~a" cv)
                         (bt:condition-notify cv))))
                    (t
                     (log:info "Got error: ~a" msg)
                     (setf error-msg msg)
                     (bt:condition-notify cv))))))
          (unwind-protect
               (bt:with-lock-held (*lock*)
                 (bknr-apply-transaction
                  self
                  copy
                  (length copy)
                  closure)
                 (unless (mp:condition-variable-wait cv *lock* :timeout 30)
                   (error "Transaction failed to apply in time"))
                 (when (lisp-closure-error closure)
                   (error (lisp-closure-error closure)))
                 (when error-msg
                   (error "~a" error-msg))
                 result)))))))

(fli:define-foreign-function bknr-snapshot
    ((fsm lisp-state-machine)
     (closure lisp-closure))
  :result-type :void)

(defmethod snapshot ((self lisp-state-machine))
  (let ((lock (bt:make-lock))
        (cv (bt:make-condition-variable))
        (msg)
        (success))
    (let ((closure (closure (res this-success this-msg)
                     (declare (ignore res))
                     (setf msg this-msg)
                     (setf success this-success)
                     (bt:with-lock-held (lock)
                       (bt:condition-notify cv)))))
      (bt:with-lock-held (lock)
        (bknr-snapshot self closure)
        (log:info "Waiting for snapshot to be done")
        (bt:condition-wait cv lock)
        (unless success
          (error "Background snapshot failed with: ~a" msg))
        (log:info "Snapshot done")))))

(fli:define-foreign-function free
    ((data :pointer))
  :result-type :void)

(defmacro def-get-path (type)
  (let ((fli-name (intern (format nil "BKNR-~a-GET-PATH" (string type))))
        (lisp-name (intern (format nil "~a-GET-PATH" (string type)))))
   `(progn
      (fli:define-foreign-function ,fli-name
          ((writer (:pointer ,type)))
        :result-type (:pointer :char))

      (defun ,lisp-name (,type)
        (let ((ptr (,fli-name ,type)))
          (unwind-protect
               (pathname (format nil "~a/" (fli:convert-from-foreign-string ptr)))
            (free ptr)))))))

(def-get-path snapshot-writer)
(def-get-path snapshot-reader)

(fli:define-foreign-function bknr-snapshot-writer-add-file
    ((sw (:pointer snapshot-writer))
     (file (:reference-pass :ef-mb-string)))
  :result-type :int)
