(defproject mvxcvi/blocks "2.1.0-SNAPSHOT"
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
  [[org.clojure/clojure "1.11.2"]
   [org.clojure/data.priority-map "1.2.0"]
   [org.clojure/tools.logging "1.3.0"]
   [com.stuartsierra/component "1.1.0"]
   [commons-io "2.15.1"]
   [manifold "0.4.2"]
   [mvxcvi/multiformats "0.3.107"]]

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
    [[org.clojure/test.check "1.1.1"]
     [commons-logging "1.3.0"]
     [mvxcvi/puget "1.3.4"]
     [mvxcvi/test.carly "0.4.1"]]}

   :repl
   {:source-paths ["dev"]}

   :test
   {:jvm-opts ["-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.NoOpLog"]}

   :coverage
   {:plugins
    [[org.clojure/clojure "1.11.2"]
     [lein-cloverage "1.2.4"]]
    :jvm-opts ["-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.SimpleLog"
               "-Dorg.apache.commons.logging.simplelog.defaultlog=trace"]}})
