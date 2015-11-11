(ns ^:no-doc blocks.data.conversions
  "Conversions for PersistentBytes values.

  This is mostly in a separate namespace to appease Cloverage."
  (:require
    [byte-streams :as bytes])
  (:import
    blocks.data.PersistentBytes
    java.io.InputStream
    java.nio.ByteBuffer))


(bytes/def-conversion [PersistentBytes ByteBuffer]
  [data options]
  (.toBuffer data))


(bytes/def-conversion [PersistentBytes InputStream]
  [data options]
  (.open data))


(bytes/def-conversion [bytes PersistentBytes]
  [data options]
  (PersistentBytes/copyFrom data))
