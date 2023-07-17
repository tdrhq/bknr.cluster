#+lispworks
(defsystem #:bknr.cluster
  :serial t
  :depends-on (:bknr.datastore
               :cl-store
               :util/threading
               :bordeaux-threads)
  :components ((:file "rpc")
               (:file "transport")
               (:file "log-file")
               (:file "server")))

#+lispworks
(defsystem #:bknr.cluster/tests
  :serial t
  :depends-on (#:bknr.cluster)
  :components ((:file "test-rpc")
               (:file "test-log-file")
               (:file "test-server")))
