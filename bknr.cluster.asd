#+lispworks
(defsystem #:bknr.cluster
  :serial t
  :depends-on (:bknr.datastore
               :bordeaux-threads)
  :components ((:file "server")))

#+lispworks
(defsystem #:bknr.cluster/tests
  :serial t
  :depends-on (#:bknr.cluster)
  :components ())
