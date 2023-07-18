#+lispworks
(defsystem #:bknr.cluster
  :serial t
  :depends-on (:bknr.datastore
               :cl-store
               :util/threading
               :easy-macros
               :bordeaux-threads)
  :components ((:file "rpc")
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
