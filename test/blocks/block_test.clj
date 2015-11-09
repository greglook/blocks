(ns blocks.block-test
  (:require
    [blocks.data :as data]
    [byte-streams :as bytes :refer [bytes=]]
    [clojure.test :refer :all]))


(deftest block-type
  (let [block (data/read-literal-block "howdy frobblenitz" :sha1)]
    
    ))
