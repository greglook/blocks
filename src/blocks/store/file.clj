(ns blocks.store.file
  "Block storage backed by files in nested directories. Each block is stored in
  a separate file.

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

  `root/111497df/35011497df3588b5a3...`

  This implementation tries to match the IPFS fs-repo behavior so that the
  on-disk representations remain compatible."
  (:require
    (blocks
      [core :as block]
      [data :as data]
      [store :as store])
    [blocks.store.util :as util]
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [multihash.core :as multihash])
  (:import
    java.io.File
    java.util.Date))


;; ## File System Utilities

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
      (if-not (neg? cmp)
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
    (dorun (map rm-r (.listFiles path))))
  (.delete path))



;; ## File Block Functions

(def ^:private dir-prefix-length
  "Number of characters to use as a prefix for top-level directory names."
  8)


(defn- file-stats
  "Calculates storage stats for a block file."
  [^File file]
  {:stored-at (Date. (.lastModified file))
   :source (.toURI file)})


(defn- block-stats
  "Calculates a merged stat map for a block."
  [id ^File file]
  (when id
    (merge (file-stats file)
           {:id id, :size (.length file)})))


(defn- id->file
  "Determines the filesystem path for a block of content with the given hash
  identifier."
  ^java.io.File
  [root id]
  (let [hex (multihash/hex id)]
    (io/file
      root
      (subs hex 0 dir-prefix-length)
      (subs hex dir-prefix-length))))


(defn- file->id
  "Reconstructs the hash identifier represented by the given file path."
  [root ^File file]
  (let [root (str root)]
    (some->
      file
      (.getPath)
      (util/check #(.startsWith ^String % root)
        (log/warnf "File %s is not a child of root directory %s" file root))
      (subs (inc (count root)))
      (str/replace "/" "")
      (util/check util/hex?
        (log/warnf "File %s did not form valid hex entry: %s" file value))
      (multihash/decode))))


(defn- file->block
  "Creates a lazy block to read from the given file."
  [id ^File file]
  (block/with-stats
    (data/lazy-block
      id (.length file)
      (fn file-reader [] (io/input-stream file)))
    (file-stats file)))


(defmacro ^:private when-block
  "An anaphoric macro which binds the block file to `file` and executes `body`
  only if it exists. Assumes that the root directory is bound to `root`."
  [id & body]
  `(let [~(with-meta 'file {:tag 'java.io.File})
         (id->file ~'root ~id)]
     (when (.exists ~'file)
       ~@body)))



;; ## File Store

;; Block content is stored as files in a multi-level hierarchy under the given
;; root directory.
(defrecord FileStore
  [^File root]

  store/BlockStore

  (-stat
    [this id]
    (when-block id
      (block-stats id file)))


  (-list
    [this opts]
    (->> (find-files root (:after opts))
         (keep #(block-stats (file->id root %) %))
         (util/select-stats opts)))


  (-get
    [this id]
    (when-block id
      (file->block id file)))


  (-put!
    [this block]
    (let [id (:id block)
          file (id->file root id)]
      (locking this
        (when-not (.exists file)
          (io/make-parents file)
          (with-open [content (block/open block)]
            (io/copy content file))
          (.setWritable file false false)))
      (data/merge-blocks
        block
        (file->block id file))))


  (-delete!
    [this id]
    (when-block id
      (locking this
        (.delete file)))))


(defn erase!
  "Clears all contents of the file store by recursively deleting the root
  directory."
  [store]
  (locking store
    (rm-r (:root store))))


(defn file-store
  "Creates a new local file-based block store."
  [root]
  (FileStore. (io/file root)))


;; Remove automatic constructor functions.
(ns-unmap *ns* '->FileStore)
(ns-unmap *ns* 'map->FileStore)
