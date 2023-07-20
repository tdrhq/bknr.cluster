;;;; Copyright 2018-Present Modern Interpreters Inc.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this
;;;; file, You can obtain one at https://mozilla.org/MPL/2.0/.

(defpackage :bknr.cluster/consensus-state
  (:use #:cl)
  (:import-from #:easy-macros
                #:def-easy-macro)
  (:export
   #:consensus-state-lock
   #:consensus-state))
(in-package :bknr.cluster/consensus-state)

(defclass consensus-state ()
  ((lock :initform (bt:make-recursive-lock)
         :reader consensus-state-lock)
   (cv :initform (bt:make-condition-variable)
       :reader consensus-state-cv)))

(def-easy-macro with-consensus-lock (stat))
