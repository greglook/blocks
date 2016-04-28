(ns user
  "Custom repl customization for local development."
  (:require
    [byte-streams :as bytes :refer [bytes=]]
    (blocks
      [core :as block]
      [data :as data])
    (blocks.store
      [memory :refer [memory-store]]
      [file :refer [file-store]]
      [tests :as tests])
    [clojure.java.io :as io]
    [clojure.repl :refer :all]
    [clojure.string :as str]
    [clojure.test.check :as check]
    [clojure.test.check.generators :as gen]
    [clojure.test.check.properties :as prop]
    [multihash.core :as multihash])
  (:import
    blocks.data.Block
    multihash.core.Multihash))


; Conditional imports from clj-stacktrace and clojure.tools.namespace:
(try (require '[clojure.stacktrace :refer [print-cause-trace]]) (catch Exception e nil))
(try (require '[clojure.tools.namespace.repl :refer [refresh]]) (catch Exception e nil))


(def test-blocks
  (tests/generate-blocks! 10 1024))


(def tbs
  "Temporary block store in target."
  (file-store "target/blocks"))
