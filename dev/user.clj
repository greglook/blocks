(ns user
  "Custom repl customization for local development."
  (:require
    [blocks.core :as block]
    [blocks.data :as data]
    [blocks.store :as store]
    [blocks.store.buffer :refer [buffer-block-store]]
    [blocks.store.cache :refer [caching-block-store]]
    [blocks.store.file :refer [file-block-store]]
    [blocks.store.memory :refer [memory-block-store]]
    [blocks.store.replica :refer [replica-block-store]]
    [blocks.store.tests :as tests]
    [byte-streams :as bytes :refer [bytes=]]
    [clojure.java.io :as io]
    [clojure.repl :refer :all]
    [clojure.stacktrace :refer [print-cause-trace]]
    [clojure.string :as str]
    [clojure.tools.namespace.repl :refer [refresh]]
    [com.stuartsierra.component :as component]
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
  (component/start (file-block-store "target/blocks" :auto-migrate? true)))
