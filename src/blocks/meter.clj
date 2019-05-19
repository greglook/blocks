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
    #_[manifold.deferred :as d]
    #_[manifold.stream :as s])
  (:import
    java.io.InputStream
    java.util.concurrent.atomic.AtomicLong
    org.apache.commons.io.input.ProxyInputStream))


;; ## Utilities

(defn- stopwatch
  "Create a delay expression which will return the number of milliseconds
  elapsed between its creation and dereference."
  []
  (let [start (System/nanoTime)]
    (delay (/ (- (System/nanoTime) start) 1e6))))


(defn- format-bytes
  "Format a byte value as a string with the given suffix."
  [value unit]
  (loop [value value
         prefixes ["" "K" "M" "G"]]
    (if (and (< 1024 value) (seq prefixes))
      (recur (/ value 1024) (next prefixes))
      (if (nat-int? value)
        (format "%d %s%s" value (first prefixes) unit)
        (format "%.1f %s%s" (double value) (first prefixes) unit)))))


(defn- meter-label
  "Construct a string to label the metered store."
  [store]
  (str (or (::label store) (.getSimpleName (class store)))))


(defn- enabled?
  "True if the store has metering enabled and a valid recorder."
  [store]
  (boolean (::recorder store)))


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


#_
(defn- metering-block-stream
  "Wrap the given stream in an intermediate stream which will record metric
  events with the number of blocks which passed through the stream."
  [store metric-type attrs stream]
  (let [counter (AtomicLong. 0)
        period *io-report-period*
        label (meter-label store)
        out (s/map #(do (.incrementAndGet counter) %) stream)
        reports (s/periodically
                  (* period 1000)
                  #(.getAndSet counter 0))
        flush! (fn flush!
                 [sum]
                 (when (pos? sum)
                   (log/tracef "Metered %s of %d blocks through stream %s (%.2f/sec)"
                               (name metric-type) sum label
                               (double (/ sum period)))
                   (record! store metric-type sum nil)))]
    (s/consume flush! reports)
    (s/on-closed
      stream
      (fn report-final
        []
        (flush! (.getAndSet counter 0))
        (s/close! reports)))
    out))


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



;; ## Metered Content

(defn metered-block
  "Wrap the block with a lazy constructor for a metered input stream which will
  report metrics for the given type. If the store does not have a recorder, the
  block will be returned unchanged."
  [store metric-type block]
  (when block
    (if (enabled? store)
      (with-meta
        (data/lazy-block
          (:id block)
          (:size block)
          (fn reader
            []
            (metering-input-stream
              store metric-type (:id block)
              (data/content-stream block nil nil))))
        (meta block))
      block)))



;; ## Method Wrappers

#_
(defn measure-stream
  "Measure the flow of blocks through a manifold stream. Returns the wrapped
  stream, or the original if the store does not have metering enabled."
  [store method-kw attrs stream]
  (cond->> stream
    (enabled? store)
    (metering-block-stream
      store ::list-stream
      (assoc attrs :method method-kw))))


(defn measure-method*
  "Internal helper for `measure-method`."
  [store method-kw attrs body-fn]
  (let [elapsed (stopwatch)]
    (try
      (body-fn)
      (finally
        (when (enabled? store)
          (log/tracef "Method %s of %s block store on %s took %.1f ms"
                      (name method-kw)
                      (meter-label store)
                      attrs
                      @elapsed)
          (record!
            store ::method-time @elapsed
            (assoc attrs :method method-kw)))))))


(defmacro measure-method
  "Measure the end-to-end elapsed time for a block store method. Returns the
  result of calling `body-fn` after measurement if the store has metering
  enabled."
  [store method-kw attrs & body]
  `(measure-method*
     ~store ~method-kw ~attrs
     (fn ~'method-body [] ~@body)))
