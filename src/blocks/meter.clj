(ns blocks.meter
  "Instrumentation for block stores to measure data flows, call latencies, and
  other metrics.

  The logic in this namespace is built around the notion of a _metric event_ and
  an associated _recording function_ on the store which the events are passed
  to. Each event has at least a namespaced `:type` keyword, a `:label`
  associated with the store, and a numeric `:value`.

  Events may contain other information like the block id or method name as
  well, and it is up to the receiver to interpret them."
  (:require
    [blocks.data :as data]
    [clojure.tools.logging :as log]
    [manifold.deferred :as d])
  (:import
    java.io.InputStream
    org.apache.commons.io.input.ProxyInputStream))


(defn- meter-label
  "Construct a string to label the metered store."
  [store]
  (or (::label store) (.getSimpleName (class store))))


(defn- format-bytes
  "Format a byte value as a string with the given suffix."
  [value unit]
  (loop [value value
         prefixes ["" "K" "M" "G"]]
    (if (and (< 1024 value) (seq prefixes))
      (recur (/ value 1024) (next prefixes))
      (if (integer? value)
        (format "%d %s%s" value (first prefixes) unit)
        (format "%.1f %s%s" (double value) (first prefixes) unit)))))


(defn- record!
  "Helper to record an event to the metered store if a recording function is
  present."
  [store metric-type value attrs]
  (when-let [recorder (::recorder store)]
    (try
      (recorder
        store
        (assoc attrs
               :type metric-type
               :label (meter-label store)
               :value value))
      (catch Exception ex
        (log/warn ex "Failure while recording metric")))))



;; ## Stream Metering

(def ^:dynamic *io-report-period*
  "Record incremental IO metrics every N seconds."
  10)


(defn- metering-input-stream
  "Wrap the given input stream in a proxy which will record metric events with
  the given type and number of bytes read."
  [store metric-type block-id ^InputStream input-stream]
  (let [meter (volatile! [(System/nanoTime) 0])]
    (letfn [(flush!
              []
              (let [[last-time sum] @meter
                    elapsed (/ (- (System/nanoTime) last-time) 1e9)
                    label (meter-label store)]
                (when (pos? sum)
                  (log/tracef "Metered %s of %s block %s: %s (%s)"
                              (name metric-type) label block-id
                              (format-bytes sum "B")
                              (format-bytes (/ sum elapsed) "Bps"))
                  (record! store metric-type sum {:block block-id})
                  (vreset! meter [(System/nanoTime) 0]))))]
      (proxy [ProxyInputStream] [input-stream]

        (afterRead
          [n]
          (when (pos? n)
            (let [[last-time sum] (vswap! meter update 1 + n)
                  elapsed (/ (- (System/nanoTime) last-time) 1e9)]
              (when (<= *io-report-period* elapsed)
                (flush!)))))

        (close
          []
          (flush!)
          (.close input-stream))))))


(deftype MeteredContentReader
  [store metric-type block-id content]

  data/ContentReader

  (read-all
    [this]
    (metering-input-stream
      store metric-type block-id
      (data/read-all content)))


  (read-range
    [this start end]
    (metering-input-stream
      store metric-type block-id
      (data/read-range content start end))))


(alter-meta! #'->MeteredContentReader assoc :private true)


(defn metered-block
  "Wrap the block with a lazy constructor for a metered input stream which will
  report metrics for the given type. If the store does not have a recorder, the
  block will be returned unchanged."
  [store metric-type block]
  (when block
    (if (::recorder store)
      (data/wrap-content
        block
        (partial ->MeteredContentReader
                 store
                 metric-type
                 (:id block)))
      block)))



;; ## Latency Measurement

(defn- stopwatch
  "Create a delay expression which will return the number of milliseconds
  elapsed between its creation and dereference."
  []
  (let [start (System/nanoTime)]
    (delay (/ (- (System/nanoTime) start) 1e6))))


(defn measure-method*
  "Helper function for the `measure-method` macro."
  [store method-kw args body-fn]
  (let [elapsed (stopwatch)]
    (cond-> (body-fn)
      (::recorder store)
      (d/finally
        (fn record-elapsed
          []
          (log/tracef "Method %s of %s block store on %s took %.1f ms"
                      (name method-kw)
                      (meter-label store)
                      args
                      @elapsed)
          (record! store ::method-time @elapsed
                   {:method method-kw
                    :args args}))))))
