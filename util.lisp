(defpackage :bknr.cluster/util
  (:use #:cl)
  (:import-from #:easy-macros
                #:def-easy-macro))
(in-package :bknr.cluster/util)

(defvar *errors* nil)

(def-easy-macro safely-ignore-errors (&fn fn)
  (handler-case
      (funcall fn)
    (error (e)
      (push e *errors*))))
