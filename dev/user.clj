(ns user
  "Custom repl customization for local development."
  (:require
    [byte-streams :as bytes :refer [bytes=]]
    (blocks
      [core :as block]
      [data :as data]
      [util :as util])
    (blocks.store
      [memory :refer [memory-store]]
      [file :refer [file-store]]
      [tests :as tests])
    [clojure.java.io :as io]
    [clojure.repl :refer :all]
    [clojure.string :as str]
    [multihash.core :as multihash])
  (:import
    blocks.data.Block
    multihash.core.Multihash))


; Conditional imports from clj-stacktrace and clojure.tools.namespace:
(try (require '[clojure.stacktrace :refer [print-cause-trace]]) (catch Exception e nil))
(try (require '[clojure.tools.namespace.repl :refer [refresh]]) (catch Exception e nil))


(def tbs
  "Temporary block store in target."
  (file-store "target/blocks"))
