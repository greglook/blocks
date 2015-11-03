(defproject mvxcvi/blocks "0.1.0-SNAPSHOT"
  :description "Content-addressed data storage interface."
  :url "https://github.com/greglook/blobble"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :deploy-branches ["master"]

  :plugins
  [[lein-cloverage "1.0.2"]]

  :dependencies
  [[byte-streams "0.1.13"]
   [mvxcvi/multihash "0.2.0"]
   [org.clojure/clojure "1.7.0"]
   [org.clojure/tools.logging "0.3.1"]])
