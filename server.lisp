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
                #:def-easy-macro))
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

(defun reload-native ()
  (progn
    (fli:disconnect-module :braft-compat)
    (asdf:compile-system :bknr.cluster)))

(fli:define-c-struct bknr-state-machine
  (foo :int))

(fli:define-foreign-function make-bknr-state-machine
    ()
  :result-type (:pointer bknr-state-machine)
  :module :braft-compat)

(fli:define-foreign-function destroy-bknr-state-machine
    ((fsm (:pointer bknr-state-machine)))
  :result-type :void)

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
              :reader data-path)
   (group :initarg :group
          :reader group)))

(defvar *state-machine-reverse-hash* (make-hash-table :test #'equalp :weak-kind :value))


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
  :result-type :boolean)

(defmethod leaderp ((self lisp-state-machine))
  (bknr-is-leader (c-state-machine self)))

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
  (let ((fli (make-bknr-state-machine)))
    (setf (c-state-machine self) fli)
    (setf (gethash fli *state-machine-reverse-hash*)
          self)))


(defmethod shutdown ((self lisp-state-machine))
  (stop-bknr-state-machine
   (c-state-machine self))
  (remhash (c-state-machine self) *state-machine-reverse-hash*)
  (destroy-bknr-state-machine (c-state-machine self)))


(fli:define-foreign-converter lisp-state-machine ()
  h
  :foreign-type :int
  :foreign-to-lisp `(gethash ,h *state-machine-reverse-hash*)
  :lisp-to-foreign `(c-state-machine ,h))


(defgeneric commit-transaction (state-machine transaction))

(defgeneric apply-transaction (state-machine transaction)
  )
