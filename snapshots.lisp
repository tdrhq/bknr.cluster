;;;; Copyright 2018-Present Modern Interpreters Inc.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this
;;;; file, You can obtain one at https://mozilla.org/MPL/2.0/.

(defpackage :bknr.cluster/snapshots
  (:use #:cl)
  (:import-from #:serapeum
                #:collecting))
(in-package :bknr.cluster/snapshots)

(defun snapshot-timestamps-to-delete (timestamps &key (now (local-time:now))
                                                   (keep-all 30)
                                                   (keep-none 90))
  "Figures out which timestamps in the list of timestamps to delete.

NOW is the current timestamp, for testing.

KEEP-ALL indicates that all timestamps within those number of days will always be kept.

KEEP-NONE indicates that all timestamps beyond those days will always be deleted.

Between KEEP-NONE and KEEP-ALL, we'll keep at least one timestamp per
week, where weeks are calculated from 1-1-1990 (so each time this is
called, the week interval will be identical)"
  (assert (<= keep-all keep-none))
  (let ((timestamps (sort (copy-list timestamps)
                          #'local-time:timestamp>))
        ;; We number weeks based on days before NOW. This map keeps
        ;; track of weeks for which we've already accounted a snapshot
        ;; that we're keeping.
        (weeks-seen (make-hash-table)))
    (collecting
      (dolist (ts timestamps)
        (let ((week-num (floor (local-time:timestamp-difference ts (local-time:universal-to-timestamp 0))
                               (* 7 24 3600))))
          (cond
            ((local-time:timestamp>= ts (local-time:timestamp- now
                                                              keep-all :day))
             (values))
            ((local-time:timestamp<= ts (local-time:timestamp- now keep-none :day))
             (collect ts))
            ((not (gethash week-num  weeks-seen))
             (setf (gethash week-num weeks-seen) t))
            (t
             (collect ts))))))))
