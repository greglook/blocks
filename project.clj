(defproject mvxcvi/blocks "0.5.0-SNAPSHOT"
  :description "Content-addressed data storage interface."
  :url "https://github.com/greglook/blocks"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :deploy-branches ["master"]
  :java-source-paths ["src"]

  :dependencies
  [[byte-streams "0.2.0"]
   [com.stuartsierra/component "0.3.0"]
   [mvxcvi/multihash "1.1.0"]
   [org.clojure/clojure "1.7.0"]
   [org.clojure/data.priority-map "0.0.7"]
   [org.clojure/tools.logging "0.3.1"]]

  :aliases {"doc-lit" ["marg" "--dir" "doc/marginalia"]
            "coverage" ["with-profile" "+test,+coverage" "cloverage"
                        "--ns-exclude-regex" "blocks.data.conversions"
                        "--ns-exclude-regex" "blocks.store.tests"]}

  :test-selectors {:unit (complement :integration)
                   :integration :integration}

  :hiera
  {:cluster-depth 1
   :show-external true
   :ignore-ns #{clojure user}}

  :codox
  {:metadata {:doc/format :markdown}
   :source-uri "https://github.com/greglook/blocks/blob/master/{filepath}#L{line}"
   :doc-paths [""]
   :output-path "doc/api"}

  :whidbey
  {:tag-types {'multihash.core.Multihash {'data/hash 'multihash.core/base58}
               'blocks.data.Block {'blocks.data.Block (partial into {})}}}

  :profiles
  {:test {:dependencies [[commons-logging "1.2"]]
          :jvm-opts ["-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.NoOpLog"]}
   :coverage {:plugins [[lein-cloverage "1.0.6"]]
              :jvm-opts ["-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.SimpleLog"
                         "-Dorg.apache.commons.logging.simplelog.defaultlog=trace"]}})
