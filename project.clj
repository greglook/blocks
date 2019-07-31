(defproject mvxcvi/blocks "2.0.2"
  :description "Content-addressed data storage interface."
  :url "https://github.com/greglook/blocks"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :aliases
  {"coverage" ["with-profile" "+coverage" "cloverage"]}

  :deploy-branches ["master"]
  :java-source-paths ["src"]
  :pedantic? :abort

  :dependencies
  [[org.clojure/clojure "1.10.1"]
   [org.clojure/data.priority-map "0.0.10"]
   [org.clojure/tools.logging "0.5.0"]
   [byte-streams "0.2.4"]
   [com.stuartsierra/component "0.4.0"]
   [commons-io "2.6"]
   [manifold "0.1.8"]
   [mvxcvi/multiformats "0.2.0"]]

  :test-selectors
  {:default (complement :integration)
   :integration :integration}

  :cloverage
  {:ns-exclude-regex [#"blocks\.store\.tests"]}

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
  {:tag-types {'blocks.data.Block {'blocks.data.Block
                                   #(array-map :id (:id %)
                                               :size (:size %)
                                               :stored-at (:stored-at %))}
               'multiformats.hash.Multihash {'multi/hash str}}}

  :profiles
  {:dev
   {:source-paths ["blocks-tests/src"]
    :dependencies
    [[org.clojure/test.check "0.9.0"]
     [commons-logging "1.2"]
     [mvxcvi/puget "1.1.2"]
     [mvxcvi/test.carly "0.4.1"]]}

   :repl
   {:source-paths ["dev"]}

   :test
   {:jvm-opts ["-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.NoOpLog"]}

   :coverage
   {:plugins
    [[org.clojure/clojure "1.10.1"]
     [lein-cloverage "1.0.13"]]
    :jvm-opts ["-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.SimpleLog"
               "-Dorg.apache.commons.logging.simplelog.defaultlog=trace"]}})
