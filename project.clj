(defproject mvxcvi/blocks "0.5.0-SNAPSHOT"
  :description "Content-addressed data storage interface."
  :url "https://github.com/greglook/blocks"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :aliases {"doc-lit" ["marg" "--dir" "doc/pages/marginalia"]
            "coverage" ["cloverage"
                        "--ns-exclude-regex" "blocks.data.conversions"
                        "--ns-exclude-regex" "blocks.store.tests"]}

  :deploy-branches ["master"]

  :java-source-paths ["src"]

  :plugins
  [[lein-cloverage "1.0.6"]]

  :dependencies
  [[byte-streams "0.2.0"]
   [mvxcvi/multihash "1.1.0"]
   [org.clojure/clojure "1.7.0"]]

  :hiera
  {:cluster-depth 1
   :show-external true
   :ignore-ns #{clojure user}}

  :codox
  {:metadata {:doc/format :markdown}
   :source-uri "https://github.com/greglook/blocks/blob/master/{filepath}#L{line}"
   :doc-paths ["doc/extra"]
   :output-path "doc/pages/api"}

  :whidbey
  {:tag-types {'multihash.core.Multihash {'data/hash 'multihash.core/base58}
               'blocks.data.Block {'blocks.data.Block (partial into {})}}}

  :profiles
  {:repl {:source-paths ["dev"]
          :dependencies [[org.clojure/tools.namespace "0.2.11"]]}})
