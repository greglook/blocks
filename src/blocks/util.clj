(ns blocks.util
  "Various utility functions for handling and testing blocks.")


(defn valid-hex?
  "Predcate which checks whether a string is valid hexadecimal encoding."
  [string]
  (boolean (and (even? (count string))
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
