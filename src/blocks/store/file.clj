(ns blocks.store.file
  "Block storage backed by files in nested directories. Each block is stored in
  a separate file.

  In many filesystems, directories are limited to 4,096 entries. In order to
  avoid this limit (and make navigating the blocks a bit more efficient), block
  content is stored in nested directories three levels deep. All path elements
  are lower-case hex-encoded bytes from the multihash.

  The first level is the two-byte multihash prefix, which designates the
  algorithm and digest length. There will usually only be one or two of these
  directories. The second and third levels are formed by the first two bytes of
  the hash digest. Finally, the block is stored in a file containing the rest of
  the digest.

  Thus, a block containing the content `foobar` would have the sha1 digest
  `97df3501149...` and be stored under the root directory at:

  `root/1114/97/df/35011497df3588b5a3...`

  Using this scheme, leaf directories should start approaching the limit once the
  user has 2^28 entries, or about 268 million blocks."
  (:require
    [blocks.core :as block]
    [blocks.data :as data]
    [clojure.java.io :as io]
    [clojure.string :as str]
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
         (filter #(re-matches #"^[0-9a-f]+$" (.getName ^File %)))
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

(defn- file-stats
  "Calculates storage stats for a block file."
  [^File file]
  {:stored-at (Date. (.lastModified file))
   :source (.toURI file)})


(defn- block-stats
  "Calculates a merged stat map for a block."
  [id ^File file]
  (merge (file-stats file)
         {:id id, :size (.length file)}))


(defn- id->file
  "Determines the filesystem path for a block of content with the given hash
  identifier."
  ^java.io.File
  [root id]
  (let [hex (multihash/hex id)]
    (io/file
      root
      (subs hex 0 4)
      (subs hex 4 6)
      (subs hex 6 8)
      (subs hex 8))))


(defn- file->id
  "Reconstructs the hash identifier represented by the given file path."
  [root file]
  (let [root (str root)
        path (str file)]
    (when-not (.startsWith path root)
      (throw (IllegalStateException.
               (str "File " path " is not a child of root directory " root))))
    (-> path
        (subs (inc (count root)))
        (str/replace "/" "")
        (multihash/decode))))


(defn- file->block
  "Creates a lazy block to read from the given file."
  [id ^File file]
  (block/with-stats
    (data/lazy-block id (.length file) #(io/input-stream file))
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
(defrecord FileBlockStore
  [^File root]

  block/BlockStore

  (stat
    [this id]
    (when-block id
      (block-stats id file)))


  (-list
    [this opts]
    (->> (find-files root (:after opts))
         (map #(block-stats (file->id root %) %))
         (block/select-stats opts)))


  (-get
    [this id]
    (when-block id
      (file->block id file)))


  (put!
    [this block]
    (let [id (:id block)
          file (id->file root id)]
      (when-not (.exists file)
        (io/make-parents file)
        (with-open [content (block/open block)]
          (io/copy content file))
        (.setWritable file false false))
      (data/merge-blocks
        block
        (file->block id file))))


  (delete!
    [this id]
    (when-block id
      (.delete file))))


(defn erase!
  "Clears all contents of the file store by recursively deleting the root
  directory."
  [store]
  (rm-r (:root store)))


(defn file-store
  "Creates a new local file-based block store."
  [root]
  (FileBlockStore. (io/file root)))


;; Remove automatic constructor functions.
(ns-unmap *ns* '->FileBlockStore)
(ns-unmap *ns* 'map->FileBlockStore)
