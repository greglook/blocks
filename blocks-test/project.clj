(defproject mvxcvi/blocks-test "2.0.0"
  :description "Generative tests for block storage implementations."
  :url "https://github.com/greglook/blocks"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :deploy-branches ["master"]
  :pedantic? :abort

  :dependencies
  [[org.clojure/clojure "1.10.0"]
   [org.clojure/test.check "0.9.0"]
   [mvxcvi/blocks "2.0.0-SNAPSHOT"]
   [mvxcvi/test.carly "0.4.1"]
   [mvxcvi/puget "1.1.0"]])
