;; Base 64 Encoding
(ns encoder
  {:author "Shotaro Ikeda"})

(def base64-keyspace
  "Map of the base64 numeric keyspace"
  (map char (concat (range (int \A) (+ (int \Z) 1))
                    (range (int \a) (+ (int \z) 1))
                    (range (int \0) (+ (int \9) 1))
                    [\+ \/])))

;; Plumbing functions for
(defn to-num-seq
  "Converts a String into a char-numeric sequence"
  [s]
  (map int (seq (char-array s))))

(defn apply-padding
  "Applies padding len-missing times"
  [bin-str len-missing]
  (if (= len-missing 0)
    bin-str
    (apply-padding (cons \0 bin-str) (- len-missing 1))))

(defn to-binary-str-with-padding
  "Converts a number, n to a binary string with padding applied, up to m."
  [m n]
  (let [bin-str (Integer/toBinaryString n)]
    (apply-padding bin-str (- m (count bin-str)))))

(defn to-bin-str
  "Converts a numeric seq to one large binary string"
  [n]
  (apply concat (map (partial to-binary-str-with-padding 8) n)))

(defn cont-split
  "Keeps splitting the array into elements n until no longer possible"
  [r n]
  (loop [rr r
         blt []]
    (let [[head tail] (split-at n rr)]
      (if (or (empty? tail) (> n (count tail)))
        [blt tail]
        (recur tail (conj blt (clojure.string/join "" head)))))))
;; TODO: return (bits, number of pads required) format
;; The number of pads can be found by finding the LCM of the char bit (8)
;; and the encoding char index bit length (6) then doing (mod STR_LEN LCM)

;; Modify me for the last function call
(defn str-to-bin
  "Converts a string into a large binary string"
  [ss]
  (-> ss
      to-num-seq
      to-bin-str))
