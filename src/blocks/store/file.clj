(ns blocks.store.file
  "Block storage backed by files in nested directories. Each block is stored in
  a separate file. File block stores may be constructed using the
  `file://<path-to-root>` URI form.

  In many filesystems, performance degrades as the number of files in a
  directory grows. In order to reduce this impact and make navigating the
  blocks a bit more efficient, block files are stored in directories under the
  store root. All path elements are lower-case hex-encoded bytes from the
  multihash.

  The directories under the root consist of the first four bytes of the
  multihashes of the blocks stored in them. Within each directory, blocks are
  stored in files whose names consist of the rest of their digests.

  Thus, a block containing the content `foobar` would have the sha1 digest
  `97df3501149...` and be stored under the root directory at:

      root/111497df/35011497df3588b5a3...

  In addition to the blocks, a `meta.properties` file at the root holds
  information about the current storage layout for future-proofing. This
  currently holds a single property, the layout version, which is always
  `\"1\"`."
  (:require
    [blocks.data :as data]
    [blocks.store :as store]
    [byte-streams :as bytes]
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [com.stuartsierra.component :as component]
    [manifold.deferred :as d]
    [manifold.stream :as s]
    [multiformats.base.b16 :as hex]
    [multiformats.hash :as multihash])
  (:import
    (java.io
      File
      FileInputStream)
    java.time.Instant))


;; ## FileSystem Utilities

(def ^:private meta-properties-file
  "Name of the properties file holding store-level metadata."
  "meta.properties")


(defn- initialize-meta
  "Initialize the store by inspecting the metadata file for the current layout
  version. If the version is not recognized, an error is thrown, otherwise this
  returns the metadata properties as a map. If the metadata file does not
  exist, it is created with the current version."
  [^File root]
  (let [meta-file (io/file root meta-properties-file)
        supported "1"]
    (if (.exists meta-file)
      ; Check for correct format/version.
      (let [props (doto (java.util.Properties.)
                    (.load (io/reader meta-file)))
            version (.getProperty props "version")]
        (when (not= version supported)
          (throw (ex-info
                   (str "Unknown storage layout version " (pr-str version)
                        " does not match supported version "
                        (pr-str supported))
                   {:supported supported
                    :meta props})))
        {:version version})
      ; No file yet, probably a new store.
      (let [version supported
            props (doto (java.util.Properties.)
                    (.setProperty "version" version))]
        (with-open [out (io/writer meta-file)]
          (.store props out " blocks.store.file"))
        {:version version}))))


(defn- seek-marker
  "Given a marker string, determine whether the file should be skipped, recursed
  into with a substring, or listed in full. Returs nil if the file should be
  skipped, otherwise a vector of the file and a marker to recurse with."
  [marker ^File file]
  (if (empty? marker)
    [file nil]
    (let [fname (.getName file)
          len (min (count marker) (count fname))
          cmp (compare (subs fname  0 len)
                       (subs marker 0 len))]
      (when-not (neg? cmp)
        (if (zero? cmp)
          [file (subs marker len)]
          [file nil])))))


(defn- find-files
  "Walks a directory tree depth first, returning a sequence of files found in
  lexical order. Intelligently skips directories based on the given marker."
  [^File file marker]
  (if (.isDirectory file)
    (->> (.listFiles file)
         (keep (partial seek-marker marker))
         (sort-by first)
         (keep (partial apply find-files))
         (flatten))
    (when (.isFile file)
      [file])))


(defn- rm-r
  "Recursively removes a directory of files."
  [^File path]
  (when (.isDirectory path)
    (run! rm-r (.listFiles path)))
  (.delete path))



;; ## File Block Functions

(def ^:private dir-prefix-length
  "Number of characters to use as a prefix for top-level directory names."
  8)


(defn- file-stats
  "Calculate a map of statistics about a block file."
  [^File file]
  (with-meta
    {:size (.length file)
     :stored-at (Instant/ofEpochMilli (.lastModified file))}
    {::source (.toURI file)}))


(defn- id->file
  "Determine the filesystem path for a block of content with the given hash
  identifier."
  ^java.io.File
  [root id]
  (let [hex (multihash/hex id)]
    (io/file
      root
      (subs hex 0 dir-prefix-length)
      (subs hex dir-prefix-length))))


(defn- file->id
  "Reconstruct the hash identifier represented by the given file path. Returns
  nil if the file is not a proper block."
  [root ^File file]
  (let [root (str root)
        path (.getPath file)]
    (cond
      (= meta-properties-file (.getName file))
      nil

      (not (str/starts-with? path root))
      (log/warnf "File %s is not a child of root directory %s" file root)

      :else
      (let [hex (str/replace (subs path (inc (count root))) "/" "")]
        (if (re-matches #"[0-9a-fA-F]+" hex)
          (multihash/decode (hex/parse hex))
          (log/warnf "File %s did not form valid hex entry: %s" file hex))))))


(defn- file->block
  "Creates a lazy block to read from the given file."
  [id ^File file]
  (let [stats (file-stats file)]
    (with-meta
      (data/create-block
        id (:size stats) (:stored-at stats)
        (fn file-reader
          []
          (FileInputStream. file)))
      (meta stats))))


(defn- temp-file
  "Create an empty temporary file to land block data into. Marks the resulting
  file for automatic cleanup."
  ^File
  [^File root]
  (.mkdirs root)
  (doto (File/createTempFile "block" ".tmp" root)
    (.deleteOnExit)))



;; ## File Store

;; Block content is stored as files in a multi-level hierarchy under the given
;; root directory.
(defrecord FileBlockStore
  [^File root]

  component/Lifecycle

  (start
    [this]
    (let [meta-props (initialize-meta root)
          version (:version meta-props)]
      (log/debug "Using storage layout version" version)
      (assoc this :version version)))


  (stop
    [this]
    this)


  store/BlockStore

  (-list
    [this opts]
    (let [out (s/stream)]
      (d/future
        (try
          (loop [files (find-files root (:after opts))]
            (when-let [file (first files)]
              (if-let [id (file->id root file)]
                ; Check that the id is still before the marker, if set.
                (when (or (nil? (:before opts))
                          (pos? (compare (:before opts) (multihash/hex id))))
                  ; Process next block.
                  (when @(s/put! out (file->block id file))
                    (recur (next files))))
                ; Not a valid block file, skip.
                (recur (next files)))))
          (catch Exception ex
            (log/error ex "Failure listing file blocks")
            (s/put! out ex))
          (finally
            (s/close! out))))
      (s/source-only out)))


  (-stat
    [this id]
    (d/future
      (let [file (id->file root id)]
        (when (.exists file)
          (assoc (file-stats file) :id id)))))


  (-get
    [this id]
    (d/future
      (let [file (id->file root id)]
        (when (.exists file)
          (file->block id file)))))


  (-put!
    [this block]
    (d/future
      (let [id (:id block)
            file (id->file root id)]
        (when-not (.exists file)
          (io/make-parents file)
          (let [tmp (temp-file root)]
            (with-open [content (data/content-stream block nil nil)]
              (io/copy content tmp))
            (.setWritable tmp false false)
            (.renameTo tmp file)))
        (file->block id file))))


  (-delete!
    [this id]
    (d/future
      (let [file (id->file root id)]
        (if (.exists file)
          (do (.delete file) true)
          false))))


  store/ErasableStore

  (-erase!
    [this]
    (d/future
      (rm-r root)
      true)))



;; ## Constructors

(store/privatize-constructors! FileBlockStore)


(defn file-block-store
  "Creates a new local file-based block store."
  [root & {:as opts}]
  (map->FileBlockStore
    (assoc opts :root (io/file root))))


(defmethod store/initialize "file"
  [location]
  (let [uri (store/parse-uri location)]
    (file-block-store
      (if (:host uri)
        (io/file (:host uri) (subs (:path uri) 1))
        (io/file (:path uri))))))
