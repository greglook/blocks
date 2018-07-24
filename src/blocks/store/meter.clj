(ns blocks.store.meter
  "Meta block store which instruments another backing store to measure the data
  flows, call latencies, and other metrics.

  The logic in this store is built around the notion of a _metric event_ and an
  associated _recording function_ which the events are passed to. Each event
  has at least a namespaced `:type` keyword, a `:label` associated with the
  store, and a numeric `:value`.

  Events may contain other information like the block id or method name as
  well, and it is up to the receiver to interpret them."
  (:require
    [blocks.core :as block]
    [blocks.data :as data]
    [blocks.store :as store]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [multihash.core :as multihash])
  (:import
    (java.io
      InputStream
      OutputStream)
    (org.apache.commons.io.input
      ProxyInputStream)
    (org.apache.commons.io.output
      ProxyOutputStream)))


(def ^:dynamic *meter-store*
  "Thread-bound inner metering store to report events up to."
  nil)


(defn- meter-label
  "Construct a string to label the metered store."
  [store]
  (or (:label store) (.getSimpleName (class (:store store)))))


(defn record-metric!
  "Record a metric to the current thread-bound meter store. Does nothing if no
  store is bound."
  [metric-type value & {:as attrs}]
  {:pre [(qualified-keyword? metric-type) (number? value)]}
  (when-let [store *meter-store*]
    (let [event (assoc attrs :type metric-type :value value)
          record! (:record-fn store)]
      (record! store event)))
  nil)



;; ## Stream Metering

(def ^:dynamic *io-report-period*
  "Record incremental IO metrics every N seconds."
  10)


(defn- format-bytes
  "Format a byte value as a string with the given suffix."
  [value unit]
  (loop [prefixes ["" "K" "M" "G"]
         value value]
    (if (and (< 1024 value) (seq prefixes))
      (recur (/ value 1024) (next prefixes))
      (if (integer? value)
        (format "%d %s%s" value (first prefixes) unit)
        (format "%.1f %s%s" (double value) (first prefixes) unit)))))


(defn- metering-input-stream
  "Wrap the given input stream in a proxy which will record metric events with
  the given type and number of bytes read."
  [store metric-type block-id ^InputStream input-stream]
  (let [meter (volatile! [(System/nanoTime) 0])]
    (letfn [(flush!
              []
              (let [[last-time sum] @meter
                    elapsed (/ (- (System/nanoTime) last-time) 1e9)
                    record! (:recording-fn store)
                    label (meter-label store)]
                (log/tracef "Metered %s of %s block %s: %s (%s)"
                            (name metric-type) label block-id
                            (format-bytes sum "B")
                            (format-bytes (/ sum elapsed) "Bps"))
                (record!
                  store
                  {:type metric-type
                   :label label
                   :block block-id
                   :value sum})
                (vreset! [(System/nanoTime) 0])))]
      (proxy [ProxyInputStream] [input-stream]

        (afterRead
          [n]
          (let [[last-time sum] (vswap! meter update 1 + n)
                elapsed (/ (- (System/nanoTime) last-time) 1e9)]
            (when (<= *io-report-period* elapsed)
              (flush!))))

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


(defn- metered-block
  "Wrap the given block with a lazy constructor for a metered input stream
  which will report metrics."
  [store metric-type block]
  (when block
    (data/wrap-block
      block
      (partial ->MeteredContentReader
               store
               metric-type
               (:id block)))))



;; ## Latency Measurement

(defn- stopwatch
  "Creates a delay expression which will return the number of milliseconds
  elapsed between its creation and dereference."
  []
  (let [start (System/nanoTime)]
    (delay (/ (- (System/nanoTime) start) 1e6))))


(defmacro ^:private measure-method
  "Anaphoric macro to wrap a form in metric recording."
  [[method-kw args] & body]
  `(let [label# (meter-label ~'this)
         elapsed# (stopwatch)]
     (try
       (binding [*meter-store* ~'this]
         ~@body)
       (finally
         (log/tracef "Method %s of %s block store on %s took %.1f ms"
                     (name ~method-kw)
                     label#
                     ~args
                     @elapsed#)
         (~'recording-fn
           ~'this
           {:type ::method-time
            :label label#
            :method ~method-kw
            :value @elapsed#})))))



;; ## Store Record

;; One downside of the approach taken here is that if a store which does _not_
;; implement the more efficient batch protocols is wrapped, the resulting
;; composition will appear to be efficient. This may change program behavior in
;; some cases if care is not taken.

(defrecord MeteredBlockStore
  [store label recording-fn]

  store/BlockStore

  (-stat
    [this id]
    (measure-method [:stat id]
      (store/-stat store id)))


  (-list
    [this opts]
    (measure-method [:list opts]
      (store/-list store opts)))


  (-get
    [this id]
    (measure-method [:get id]
      (metered-block this ::io-read (store/-get store id))))


  (-put!
    [this block]
    (measure-method [:put! (:id block)]
      (->> block
           (metered-block this ::io-write)
           (store/-put! store)
           (metered-block this ::io-read))))


  (-delete!
    [this id]
    (measure-method [:delete! id]
      (store/-delete! store id)))


  store/BatchingStore

  (-get-batch
    [this ids]
    (measure-method [:get-batch ids]
      (->> ids
           (block/get-batch store)
           (map (partial metered-block this ::io-read)))))


  (-put-batch!
    [this blocks]
    (measure-method [:put-batch! (mapv :id blocks)]
      (->> blocks
           (map (partial metered-block this ::io-write))
           (block/put-batch! store)
           (map (partial metered-block this ::io-read)))))


  (-delete-batch!
    [this ids]
    (measure-method [:delete-batch! ids]
      (block/delete-batch! store ids)))


  store/ErasableStore

  (-erase!
    [this]
    (measure-method [:erase! nil]
      (block/erase!! store))))



;; ## Store Construction

(store/privatize-constructors! MeteredBlockStore)


(defn metered-block-store
  "Creates a new metered block store. The `recording-fn` function will be
  called once for each measurement, with the meter store record and a map
  containing the metric name string, a numeric value, and possibly other data."
  [recording-fn & {:as opts}]
  (when-not (fn? recording-fn)
    (throw (IllegalArgumentException.
             (str "First argument must be a metric recording function: "
                  (pr-str recording-fn)))))
  (map->MeteredBlockStore
    (assoc opts :recording-fn recording-fn)))
