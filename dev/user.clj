(ns user
  (:require
    [byte-streams :as bytes :refer [bytes=]]
    (blocks
      [core :as block]
      [data :as data])
    (blocks.store
      [memory :refer [memory-store]]
      [file :refer [file-store]]
      [tests :as test :refer [random-bytes]])
    [clojure.java.io :as io]
    [clojure.repl :refer :all]
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.string :as str]
    [clojure.tools.namespace.repl :refer [refresh]]
    [multihash.core :as multihash])
  (:import
    blocks.data.Block
    multihash.core.Multihash))


(def tbs
  "Temporary block store in target."
  (file-store "target/blocks"))
