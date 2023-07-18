;;;; Copyright 2018-Present Modern Interpreters Inc.
;;;;
;;;; This Source Code Form is subject to the terms of the Mozilla Public
;;;; License, v. 2.0. If a copy of the MPL was not distributed with this
;;;; file, You can obtain one at https://mozilla.org/MPL/2.0/.

(defpackage :bknr.cluster/test-leader
  (:use #:cl
        #:fiveam)
  (:import-from #:bknr.cluster/test-server
                #:with-peer-and-machines)
  (:import-from #:bknr.cluster/leader
                #:append-entries-from)
  (:import-from #:bknr.cluster/rpc
                #:log-entry)
  (:import-from #:bknr.cluster/log-file
                #:append-log-entry)
  (:import-from #:bknr.cluster/server
                #:start-up
                #:shutdown
                #:log-file)
  (:import-from #:bknr.cluster/transport
                #:noop-transport))
(in-package :bknr.cluster/test-leader)


(util/fiveam:def-suite)

(def-fixture leader (&key (num 3))
  (with-peer-and-machines (peer machines :num num :transport 'noop-transport)
    (let ((leader (first machines)))
      (start-up leader)
      (unwind-protect
           (&body)
        (shutdown leader)))))

(test append-entries-from-happy-path
  (with-fixture leader ()
    (append-log-entry
     (log-file leader)
     10
     #(1 2 3))
    (append-entries-from leader
                         1)))
