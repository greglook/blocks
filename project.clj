(defproject mvxcvi/blocks "1.1.0-SNAPSHOT"
  :description "Content-addressed data storage interface."
  :url "https://github.com/greglook/blocks"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :aliases
  {"coverage" ["with-profile" "+coverage" "cloverage"
               "--ns-exclude-regex" "blocks.store.tests"]}

  :deploy-branches ["master"]
  :java-source-paths ["src"]
  :pedantic? :abort

  :dependencies
  [[org.clojure/clojure "1.9.0"]
   [org.clojure/data.priority-map "0.0.7"]
   [org.clojure/test.check "0.9.0" :scope "test"]
   [org.clojure/tools.logging "0.4.0"]
   [bigml/sketchy "0.4.1"]
   [byte-streams "0.2.3"]
   [com.stuartsierra/component "0.3.2"]
   [commons-io "2.6"]
   [mvxcvi/multihash "2.0.2"]
   [mvxcvi/puget "1.0.2" :scope "test"]
   [mvxcvi/test.carly "0.4.1" :scope "test"]]

  :test-selectors
  {:default (complement :integration)
   :integration :integration}

  :hiera
  {:cluster-depth 2
   :vertical false
   :show-external false
   :ignore-ns #{blocks.store.tests}}

  :codox
  {:metadata {:doc/format :markdown}
   :source-uri "https://github.com/greglook/blocks/blob/master/{filepath}#L{line}"
   :output-path "target/doc/api"}

  :whidbey
  {:tag-types {'multihash.core.Multihash {'data/hash 'multihash.core/base58}
               'blocks.data.Block {'blocks.data.Block (partial into {})}}}

  :profiles
  {:repl
   {:source-paths ["dev"]}

   :test
   {:dependencies [[commons-logging "1.2"]]
    :jvm-opts ["-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.NoOpLog"]}

   :coverage
   {:plugins [[lein-cloverage "1.0.10"]]
    :dependencies [[commons-logging "1.2"]
                   [riddley "0.1.14"]]
    :jvm-opts ["-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.SimpleLog"
               "-Dorg.apache.commons.logging.simplelog.defaultlog=trace"]}})
