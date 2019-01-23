(ns user
  "Custom repl customization for local development."
  (:require
    [byte-streams :as bytes :refer [bytes=]]
    [blocks.core :as block]
    [blocks.data :as data]
    [blocks.store :as store]
    [blocks.store.buffer :refer [buffer-block-store]]
    [blocks.store.cache :refer [caching-block-store]]
    [blocks.store.file :refer [file-block-store]]
    [blocks.store.memory :refer [memory-block-store]]
    [blocks.store.replica :refer [replica-block-store]]
    [blocks.store.test-harness :as tests]
    [clojure.java.io :as io]
    [clojure.repl :refer :all]
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.string :as str]
    ;[clojure.test.check :as check]
    ;[clojure.test.check.generators :as gen]
    ;[clojure.test.check.properties :as prop]
    [clojure.tools.namespace.repl :refer [refresh]]
    [manifold.deferred :as d]
    [manifold.stream :as s]
    [multiformats.hash :as multihash])
  (:import
    blocks.data.Block
    multiformats.hash.Multihash))


(def test-blocks
  (tests/generate-blocks! 10 1024))


(def tbs
  "Temporary block store in target."
  (file-block-store "target/blocks"))
