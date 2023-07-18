;;;; Copyright 2018-Present Modern Interpreters Inc.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this
;;;; file, You can obtain one at https://mozilla.org/MPL/2.0/.

(defpackage :bknr.cluster/test-log-file
  (:use #:cl
        #:fiveam)
  (:import-from #:bknr.cluster/log-file
                #:log-file
                #:ignore-decoding-errors
                #:close-log-file
                #:entry-at
                #:append-log-entry
                #:open-log-file)
  (:import-from #:bknr.cluster/rpc
                #:term
                #:entry-data))
(in-package :bknr.cluster/test-log-file)


(util/fiveam:def-suite)

(defclass test-log-file (log-file)
  ())

(defmethod ignore-decoding-errors ((self test-log-file) fn)
  (funcall fn))

(def-fixture state ()
  (tmpdir:with-tmpdir (dir)
    (let ((log-file (open-log-file :pathname (path:catfile dir "foo.log")
                                   :type 'test-log-file)))
      (unwind-protect
           (&body)
        (close-log-file log-file)))))

(test preconditions
  (with-fixture state ()
    (pass)))

(test append-log-entry
  (with-fixture state ()
    (append-log-entry log-file 10 #(1 2 3))
    (let ((entry
            (entry-at log-file 1)))
      (is (equalp #(1 2 3) (entry-data entry)))
      (is (eql 10 (term entry))))))

(test re-read-log-entries
  (with-fixture state ()
    (append-log-entry log-file 10 #(1 2 3))
    (append-log-entry log-file 11 #(3 2 1))
    (let ((other (open-log-file :pathname (path:catfile dir "foo.log")
                                :type 'test-log-file)))
      (let ((entry
              (entry-at other 1)))
        (is (equalp #(1 2 3) (entry-data entry)))
        (is (eql 10 (term entry))))

      (let ((entry
             (entry-at other 2)))
        (is (equalp #(3 2 1) (entry-data entry)))
        (is (eql 11 (term entry)))))))
