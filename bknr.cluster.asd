(defsystem #:bknr.cluster
  :serial t
  :depends-on (:bknr.datastore)
  :components ((:file "server")))

(defsystem #:bknr.cluster/tests
  :serial t
  :depends-on (#:bknr.cluster)
  :components ())
