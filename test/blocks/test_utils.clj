(ns blocks.test-utils)


(defn quiet-exception
  "Construct a runtime exception which elides stacktrace data. Useful for
  throwing inside error handling test cases."
  ([]
   (quiet-exception "BOOM"))
  ([message]
   (doto (RuntimeException. ^String message)
     (.setStackTrace (into-array StackTraceElement [])))))
