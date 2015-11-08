(defproject mvxcvi/blocks "0.4.0-SNAPSHOT"
  :description "Content-addressed data storage interface."
  :url "https://github.com/greglook/blobble"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :aliases {"coverage" ["cloverage"
                        "--ns-exclude-regex" "blocks.data.conversions"
                        "--ns-exclude-regex" "blocks.store.tests"]}

  :deploy-branches ["master"]

  :java-source-paths ["src"]

  :plugins
  [[lein-cloverage "1.0.6"]]

  :dependencies
  [[byte-streams "0.2.0"]
   [mvxcvi/multihash "1.0.0"]
   [org.clojure/clojure "1.7.0"]
   [org.clojure/tools.logging "0.3.1"]]

  :codox
  {:metadata {:doc/format :markdown}
   :source-uri "https://github.com/greglook/blocks/blob/master/{filepath}#L{line}"
   :doc-paths ["doc/extra"]
   :output-path "doc/api"}

  :whidbey
  {:tag-types {'multihash.core.Multihash {'data/hash 'multihash.core/base58}}}

  :profiles
  {:repl {:source-paths ["dev"]
          :dependencies [[org.clojure/tools.namespace "0.2.10"]]}})
