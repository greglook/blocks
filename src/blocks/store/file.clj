(ns blocks.store.file
  "Content storage backed by a local filesystem.

  In many filesystems, directories are limited to 4,096 entries. In order to
  avoid this limit (and make navigating the filesystem a bit more efficient),
  block content is stored in a nested hierarchy three levels deep.

  The first level is the algorithm used in the block's multihash. The second
  and third levels are formed by the first three characters and next three
  characters from the id's digest. Finally, the block is stored in a file named
  by the multihashes' hex string.

  Thus, a file path for the content \"foobar\" might be:

  `root/sha1/97d/f35/011497df3588b5a3...`

  Using this scheme, leaf directories should start approaching the limit once the
  user has 2^(3*12) entries, or about 68.7 billion blocks."
  (:require
    [blocks.core :as block]
    [blocks.data :as data]
    [clojure.java.io :as io]
    [clojure.string :as string]
    [multihash.core :as multihash]
    [multihash.hex :as hex])
  (:import
    java.io.File
    java.util.Date))


;; ## File System Utilities

(defn- block-stats
  "Calculates storage stats for a block file."
  [^File file]
  {:stored-at (Date. (.lastModified file))
   :origin (.toURI file)})


(defn- id->file
  "Determines the filesystem path for a block of content with the given hash
  identifier."
  ^File
  [root id]
  (let [algorithm (:algorithm id)
        digest (hex/encode (:digest id))]
    (io/file
      root
      (name algorithm)
      (subs digest 0 3)
      (subs digest 3 6)
      (multihash/hex id))))


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
        (string/split #"/")
        (last)
        (multihash/decode))))


(defn- file->block
  "Creates a lazy block to read from the given file."
  [id ^File file]
  (block/with-stats
    (data/lazy-block id (.length file) #(io/input-stream file))
    (block-stats file)))


(defn- find-files
  "Walks a directory tree depth first, returning a sequence of files found in
  lexical order."
  [^File file]
  (cond
    (.isFile file)
      [file]
    (.isDirectory file)
      (->> (.listFiles file) (sort) (map find-files) (flatten))
    :else
      []))


(defn- rm-r
  "Recursively removes a directory of files."
  [^File path]
  (when (.isDirectory path)
    (dorun (map rm-r (.listFiles path))))
  (.delete path))


(defmacro ^:private when-block-file
  "An anaphoric macro which binds the block file to `file` and executes `body`
  only if it exists."
  [store id & body]
  `(let [~(with-meta 'file {:tag 'java.io.File})
         (id->file (:root ~store) ~id)]
     (when (.exists ~'file)
       ~@body)))



;; ## File Store

;; Block content is stored as files in a multi-level hierarchy under the given
;; root directory.
(defrecord FileBlockStore
  [^File root]

  block/BlockStore

  (-list
    [this opts]
    (->> (find-files root)
         (map (partial file->id root))
         (block/select-hashes opts)))


  (stat
    [this id]
    (when-block-file this id
      (merge (block-stats file)
             {:id id, :size (.length file)})))


  (-get
    [this id]
    (when-block-file this id
      (file->block id file)))


  (put!
    [this block]
    (let [id (:id block)
          file (id->file root id)]
      (when-not (.exists file)
        (io/make-parents file)
        ; For some reason, io/copy is much faster than byte-streams/transfer here.
        (io/copy (block/open block) file)
        (.setWritable file false false))
      (file->block id file)))


  (delete!
    [this id]
    (when-block-file this id
      (.delete file))))


(defn erase!
  "Clears all contents of the file store."
  [store]
  (rm-r (:root store)))


(defn file-store
  "Creates a new local file-based block store."
  [root]
  (FileBlockStore. (io/file root)))


;; Remove automatic constructor functions.
(ns-unmap *ns* '->FileBlockStore)
(ns-unmap *ns* 'map->FileBlockStore)
