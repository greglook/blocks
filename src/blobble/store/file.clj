(ns blobble.store.file
  "Content storage backed by a local filesystem.

  In many filesystems, directories are limited to 4,096 entries. In order to
  avoid this limit (and make navigating the filesystem a bit more efficient),
  blob content is stored in a nested hierarchy three levels deep.

  The first level is the algorithm used in the blob's multihash. The second
  and third levels are formed by the first three characters and next three
  characters from the id's digest. Finally, the blob is stored in a file named
  by the multihashes' hex string.

  Thus, a file path for the content \"foobar\" might be:

  `root/sha1/97d/f35/011497df3588b5a3...`

  Using this scheme, leaf directories should start approaching the limit once the
  user has 2^(3*12) entries, or about 68.7 billion blobs."
  (:require
    [blobble.core :as blob]
    [clojure.java.io :as io]
    [clojure.string :as string]
    [multihash.core :as multihash])
  (:import
    java.io.File
    java.util.Date))


;; ## File System Utilities

(defn- id->file
  "Determines the filesystem path for a blob of content with the given hash
  identifier."
  ^File
  [root id]
  (let [{:keys [algorithm digest]} id]
    (io/file
      root
      (name algorithm)
      (subs digest 0 3)
      (subs digest 3 6)
      (multihash/encode-hex id))))


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


(defn- find-files
  "Walks a directory tree depth first, returning a sequence of files found in
  lexical order."
  [^File file]
  (cond
    (.isFile file)
      [file]
    (.isDirectory file)
      (->> file (.listFiles) (sort) (map find-files) (flatten))
    :else
      []))


(defn- rm-r
  "Recursively removes a directory of files."
  [^File path]
  (when (.isDirectory path)
    (->> path (.listFiles) (map rm-r) (dorun)))
  (.delete path))


(defmacro ^:private when-blob-file
  "An unhygenic macro which binds the blob file to `file` and executes the body
  only if it exists."
  [store id & body]
  `(let [~(with-meta 'file {:tag 'java.io.File})
         (id->file (:root ~store) ~id)]
     (when (.exists ~'file)
       ~@body)))


(defn- blob-stats
  "Calculates storage stats for a blob file."
  [^File file]
  {:stat/size (.length file)
   :stat/stored-at (Date. (.lastModified file))
   :stat/origin (.toURI file)})



;; ## File Store

;; Blob content is stored as files in a multi-level hierarchy under the given
;; root directory.
(defrecord FileBlobStore
  [^File root]

  blob/BlobStore

  (enumerate
    [this opts]
    (->> (find-files root)
         (map (partial file->id root))
         (blob/select-ids opts)))


  (stat
    [this id]
    (when-blob-file this id
      (merge (blob/empty-blob id)
             (blob-stats file))))


  (get*
    [this id]
    (when-blob-file this id
      (-> file
          (io/input-stream)
          (blob/read!)
          (merge (blob-stats file)))))


  (put!
    [this blob]
    (let [{:keys [id content]} blob
          file (id->file root id)]
      (when-not (.exists file)
        (io/make-parents file)
        ; For some reason, io/copy is much faster than byte-streams/transfer here.
        (io/copy content file)
        (.setWritable file false false))
      (merge blob (blob-stats file))))


  (delete!
    [this id]
    (when-blob-file this id
      (.delete file)))


  (erase!!
    [this]
    (rm-r root)))


(defn file-store
  "Creates a new local file-based blob store."
  [root]
  (FileBlobStore. (io/file root)))
