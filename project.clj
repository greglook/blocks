(defproject mvxcvi/blocks "0.3.0-SNAPSHOT"
  :description "Content-addressed data storage interface."
  :url "https://github.com/greglook/blobble"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :deploy-branches ["master"]

  :java-source-paths ["src"]

  :plugins
  [[lein-cloverage "1.0.2"]]

  :dependencies
  [[byte-streams "0.2.0"]
   [mvxcvi/multihash "1.0.0"]
   [org.clojure/clojure "1.7.0"]
   [org.clojure/tools.logging "0.3.1"]])
