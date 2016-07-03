;; Base 64 Encoding
(ns encoder
  {:author "Shotaro Ikeda"})

(def base64-keyspace
  "Map of the base64 numeric keyspace"
  (map char (concat (range (int \A) (+ (int \Z) 1))
                    (range (int \a) (+ (int \z) 1))
                    (range (int \0) (+ (int \9) 1))
                    [\+ \/])))

;; Euclidean Algorithm
(defn find-gcd
  "Finds the Greatest Common Divisor of n1 and n2"
  [n1 n2]
  (loop [div n1
         rep n2]
    (if (= rep 0)
      div
      (recur rep (mod div rep)))))

(defn find-lcm
  "Finds the Least Common Multiple between n1 and n2"
  [n1 n2]
  (/ (* n1 n2) (find-gcd n1 n2)))

(defn find-cycle
  "Find the constant to determine how much padding needs to be applied."
  [char-bits encoder-bits]
  (/ (encoder/find-lcm 8 6) char-bits))

(defn find-padding
  "Finds the number of padding required, given how many bits per char and how many
  bits per encoding char"
  [ss char-bits encoder-bits]
  (mod ss (find-cycle char-bits encoder-bits)))

;; Plumbing functions for converting a string to base64 encoding
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
  "Converts a string into a large binary string -- assumes 8 bits per char and 6 bits per encoding"
  [ss]
  (let [st-len (count ss)]
    (-> (ss)
        to-num-seq
        to-bin-str
        )))
