(defpackage :bknr.cluster/bknr.cluster.asd
  (:use #:cl #:asdf))
(in-package :bknr.cluster/bknr.cluster.asd)

(defclass braft-cpp-library (asdf:c-source-file)
  ()
  (:default-initargs :type "cpp"))

(defmethod output-files ((o compile-op) (cpp braft-cpp-library))
  (list
   (make-pathname
    :name (component-name cpp)
    :type "so")))

(defmethod perform ((o compile-op) (cpp braft-cpp-library))
  (uiop:run-program
   (list
    "g++"
    "-shared"
    "-fPIC"
    "-lbrpc" "-lbraft" "-lgflags"
    (namestring (component-pathname cpp))
    "-o"
    (namestring (output-file o cpp)))
   :error-output t
   :standard-output t))

(defmethod perform ((o load-op) (cpp braft-cpp-library))
  nil)


#+lispworks
(defsystem #:bknr.cluster
  :serial t
  :depends-on (:bknr.datastore
               :cl-store
               :util/threading
               :easy-macros
               :bordeaux-threads)
  :components ((braft-cpp-library "braft_compat")
               (:file "util")
               (:file "rpc")
               (:file "transport")
               (:file "log-file")
               (:file "server")
               (:file "leader")))


#+lispworks
(defsystem #:bknr.cluster/tests
  :serial t
  :depends-on (#:bknr.cluster
               #:util/fiveam)
  :components ((:file "test-rpc")
               (:file "test-log-file")
               (:file "test-server")))
