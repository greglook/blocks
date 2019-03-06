(ns blocks.store.file
  "This store provides block storage backed by files in local directories. Each
  block is stored in a separate file. File block stores may be constructed
  using the `file://<path-to-root-dir>` URI form. Under the root directory, the
  store keeps a block files in a subdirectory, alongside some layout metadata
  and a directory.

      $ROOT/meta.properties
      $ROOT/blocks/111497df/35011497df3588b5a3...
      $ROOT/landing/block.123456789.tmp

  In many filesystems, performance degrades as the number of files in a
  directory grows. In order to reduce this impact and make navigating the
  blocks more efficient, block files are stored in multiple subdirectories
  consisting of the first four bytes of the multihashes of the blocks stored in
  them. Within each directory, blocks are stored in files whose names consist
  of the rest of their digests.

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


;; ## Storage Layout

(def layout-version
  "The current supported storage layout version."
  "v1")


;; ### Metadata

(defn- meta-file
  "Construct the store-level metadata properties file from the store root."
  ^File
  [root]
  (io/file root "meta.properties"))


(defn- read-meta-properties
  "Read the store's metadata file if it exists."
  [^File root]
  (let [props-file (meta-file root)]
    (when (.exists props-file)
      (into {}
            (map (juxt (comp keyword key) val))
            (doto (java.util.Properties.)
              (.load (io/reader props-file)))))))


(defn- write-meta-properties
  "Write a metadata properties file and returns the data map."
  [^File root]
  (let [props-file (meta-file root)
        props (doto (java.util.Properties.)
                (.setProperty "version" layout-version))]
    (.mkdirs root)
    (with-open [out (io/writer props-file)]
      (.store props out " blocks.store.file"))
    {:version layout-version}))


;; ### Landing Area

(defn- landing-dir
  "Construct the landing directory from the store root."
  ^File
  [root]
  (io/file root "landing"))


(defn- landing-file
  "Create an empty temporary file to land block data into. Marks the resulting
  file for automatic cleanup if it is not moved."
  ^File
  [^File root]
  (let [tmp-dir (landing-dir root)]
    (.mkdirs tmp-dir)
    (doto (File/createTempFile "block" ".tmp" tmp-dir)
      (.deleteOnExit))))


;; ### Block Files

(def ^:private prefix-length
  "Number of characters to use as a prefix for top-level directory names."
  8)


(defn- blocks-dir
  "Construct the block directory from the store root."
  ^File
  [root]
  (io/file root "blocks"))


(defn- block-files
  "Walks a block directory tree depth first, returning a sequence of files
  found in lexical order. Intelligently skips subdirectories based on the given
  marker."
  [^File root after]
  (->
    (.listFiles (blocks-dir root))
    (sort)
    (cond->>
      after
      (drop-while
        #(let [subdirname (.getName ^File %)
               len (min (count after) (count subdirname))]
           (pos? (compare (subs after 0 len) (subs subdirname 0 len))))))
    (->>
      (mapcat
        (fn list-blocks
          [^File subdir]
          (sort (.listFiles subdir)))))))


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
  ^File
  [^File root id]
  (let [hex (multihash/hex id)
        len (min (dec (count hex)) prefix-length)
        subdir (subs hex 0 len)
        fragment (subs hex len)]
    (io/file (blocks-dir root) subdir fragment)))


(defn- file->id
  "Reconstruct the hash identifier represented by the given file path. Returns
  nil if the file is not a proper block."
  [^File root ^File file]
  (let [prefix (str (blocks-dir root))
        path (.getPath file)
        hex (str/replace (subs path (inc (count prefix))) "/" "")]
    (if (re-matches #"[0-9a-fA-F]+" hex)
      (multihash/decode (hex/parse hex))
      (log/warnf "File %s did not form valid hex entry: %s" file hex))))


(defn- file->block
  "Creates a lazy block to read from the given file."
  [id ^File file]
  (let [stats (file-stats file)]
    (with-meta
      (data/create-block
        id (:size stats) (:stored-at stats)
        ; OPTIMIZE: use java.io.RandomAccessFile to read subranges
        (fn reader [] (FileInputStream. file)))
      (meta stats))))


;; ### Initialization

(defn- v0-subdir?
  "True if the given directory is a v0 block subdirectory."
  [^File subdir]
  (and (.isDirectory subdir)
       (= prefix-length (count (.getName subdir)))
       (re-matches #"[0-9a-f]+" (.getName subdir))))


(defn- migrate-v0!
  "Migrate an existing v0 layout to v1."
  [^File root]
  (let [blocks (blocks-dir root)]
    (.mkdirs blocks)
    (run!
      (fn move-block-dir
        [^File subdir]
        (when (v0-subdir? subdir)
          (.renameTo subdir (io/file blocks (.getName subdir)))))
      (.listFiles root))))


(defn- initialize-layout!
  "Initialize the block store layout by writing out metadata and pre-creating
  some directories. Returns the layout meta-properties."
  [store]
  (let [^File root (:root store)]
    (if (empty? (.listFiles root))
      ; Root doesn't exist or is empty, so initialize the storage layout.
      (write-meta-properties root)
      ; Try loading store metadata.
      (let [properties (read-meta-properties root)]
        (if (nil? properties)
          ; No meta-properties file; check for v0 layout.
          (do
            ; Check for unknown file content in root.
            (when-not (every? v0-subdir? (.listFiles root))
              (throw (ex-info
                       (str "Detected unknown files in block store at " root)
                       {:files (vec (.listFiles root))})))
            ; Possible v0 store. Abort unless configured to migrate.
            (when-not (:auto-migrate? store)
              (throw (ex-info
                       (str "Detected v0 file block store layout at " root)
                       {:root root})))
            ; Migrate to v1 layout.
            (log/warn "Automatically migrating file block store layout at"
                      (.getPath root) "from v0 ->" layout-version)
            (migrate-v0! root)
            (write-meta-properties root))
          ; Check for known layout version.
          (let [version (:version properties)]
            (when (not= layout-version version)
              (throw (ex-info
                       (str "Unknown storage layout version " (pr-str version)
                            " does not match supported version "
                            (pr-str layout-version))
                       {:supported layout-version
                        :properties properties})))
            ; Layout matches the expected version.
            properties))))))


(defn- rm-r
  "Recursively removes a directory of files."
  [^File path]
  (when (.isDirectory path)
    (run! rm-r (.listFiles path)))
  (.delete path))



;; ## File Store

;; Block content is stored as files in a multi-level hierarchy under the given
;; root directory.
(defrecord FileBlockStore
  [^File root]

  component/Lifecycle

  (start
    [this]
    (let [properties (initialize-layout! this)
          version (:version properties)]
      ;(log/debug "Using storage layout version" version)
      (assoc this :version version)))


  (stop
    [this]
    this)


  store/BlockStore

  (-list
    [this opts]
    (let [out (s/stream 1000)]
      (store/future'
        (try
          (loop [files (block-files root (:after opts))]
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
    (store/future'
      (let [file (id->file root id)]
        (when (.exists file)
          (assoc (file-stats file) :id id)))))


  (-get
    [this id]
    (store/future'
      (let [file (id->file root id)]
        (when (.exists file)
          (file->block id file)))))


  (-put!
    [this block]
    (store/future'
      (let [id (:id block)
            file (id->file root id)]
        (when-not (.exists file)
          (let [tmp (landing-file root)]
            (with-open [content (data/content-stream block nil nil)]
              (io/copy content tmp))
            (io/make-parents file)
            (.setWritable tmp false false)
            (.renameTo tmp file)))
        (file->block id file))))


  (-delete!
    [this id]
    (store/future'
      (let [file (id->file root id)]
        (if (.exists file)
          (do (.delete file) true)
          false))))


  store/ErasableStore

  (-erase!
    [this]
    (store/future'
      (rm-r (landing-dir root))
      (rm-r (blocks-dir root))
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
