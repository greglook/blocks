(ns user
  "Custom repl customization for local development. To use this, add the
  following to the `:repl` profile in `profiles.clj`:

  ```
  :source-paths [\"dev\"]
  :dependencies
  [[clj-stacktrace \"RELEASE\"]
   [org.clojure/tools.namespace \"RELEASE\"]]
  ```
  "
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
    [clojure.tools.namespace.repl :refer [refresh]]
    [multihash.core :as multihash])
  (:import
    blocks.data.Block
    multihash.core.Multihash))


(def tbs
  "Temporary block store in target."
  (file-store "target/blocks"))
