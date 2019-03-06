(ns blocks.test-utils
  (:require
    [manifold.deferred :as d]))


(defn quiet-exception
  "Construct a runtime exception which elides stacktrace data. Useful for
  throwing inside error handling test cases."
  ([]
   (quiet-exception "BOOM"))
  ([message]
   (doto (RuntimeException. ^String message)
     (.setStackTrace (into-array StackTraceElement [])))))


(defn quiet-error-deferred
  []
  (doto (d/error-deferred (quiet-exception))
    (d/error-value nil)))
