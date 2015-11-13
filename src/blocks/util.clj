(ns blocks.util
  "Various utility functions for handling and testing blocks."
  (:require
    [multihash.core :as multihash]))


(defmacro check
  "Utility macro for validating values in a threading fashion. The predicate
  `pred?` will be called with the current value; if the result is truthy, the
  value is returned. Otherwise, any forms passed in the `on-err` list are
  executed with the symbol `value` bound to the value, and the function returns
  nil."
  [value pred? & on-err]
  `(let [value# ~value]
     (if (~pred? value#)
       value#
       (let [~'value value#]
         ~@on-err
         nil))))


(defn hex?
  "Predcate which checks whether a string is valid hexadecimal encoding."
  [string]
  (boolean (and (string? string)
                (even? (count string))
                (re-matches #"^[0-9a-f]+$" string))))


(defn random-bytes
  "Returns a byte array between one and `max-len` bytes long with random
  content."
  [max-len]
  (let [size (inc (rand-int max-len))
        data (byte-array size)]
    (.nextBytes (java.security.SecureRandom.) data)
    data))


(defn random-hex
  "Returns a random hex string between one and `max-len` bytes long."
  [max-len]
  (->> (repeatedly #(rand-nth "0123456789abcdef"))
       (take (* 2 (inc (rand-int max-len))))
       (apply str)))


(defn select-stats
  "Selects block stats from a sequence based on the criteria spported in
  `blocks.core/list`. Helper for block store implementers."
  [opts stats]
  (let [{:keys [algorithm after limit]} opts]
    (cond->> stats
      algorithm
        (filter (comp #{algorithm} :algorithm :id))
      after
        (drop-while #(pos? (compare after (multihash/hex (:id %)))))
      limit
        (take limit))))
