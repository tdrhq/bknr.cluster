;;;; Copyright 2018-Present Modern Interpreters Inc.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this
;;;; file, You can obtain one at https://mozilla.org/MPL/2.0/.

(defpackage :bknr.cluster/test-log-file
  (:use #:cl
        #:fiveam)
  (:import-from #:bknr.cluster/log-file
                #:close-log-file
                #:entry-at
                #:append-log-entry
                #:open-log-file))
(in-package :bknr.cluster/test-log-file)


(util/fiveam:def-suite)

(def-fixture state ()
  (tmpdir:with-tmpdir (dir)
    (let ((log-file (open-log-file :pathname (path:catfile dir "foo.log"))))
      (unwind-protect
           (&body)
        (close-log-file log-file)))))

(test preconditions
  (with-fixture state ()
    (pass)))

(test append-log-entry
  (with-fixture state ()
    (append-log-entry log-file 10 #(1 2 3))
    (multiple-value-bind (data term)
        (entry-at log-file 1)
      (is (equalp #(1 2 3) data))
      (is (eql 10 term)))))
