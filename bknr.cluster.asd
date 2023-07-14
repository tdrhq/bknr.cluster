#+lispworks
(defsystem #:bknr.cluster
  :serial t
  :depends-on (:bknr.datastore
               :cl-store
               :util/threading
               :bordeaux-threads)
  :components ((:file "server")))

#+lispworks
(defsystem #:bknr.cluster/tests
  :serial t
  :depends-on (#:bknr.cluster)
  :components ())
