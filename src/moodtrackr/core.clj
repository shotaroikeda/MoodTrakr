(ns moodtrackr.core
  (:require [http.async.client :as http]))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))

(def ps-base-url
  "https://stream.twitter.com/1.1/statuses/filter.json")
(def ps-params
  {:locations "-122.75,36.8,-121.75,37.8"})

(defn gen-twitter-url
  "Generates a Twitter API url for the given params."
  [base params]
  (str base "?" (clojure.string/join "&" (map (fn [[k v _]] (str (name k) "=" v)) params))))

;; Plumbing for http.async
(defn reponse-handler [res]
  (print res))

(defn post-test [url]
  (println "Connecting...")
  (with-open [client (http/create-client)]
    (let [response (http/POST client url)]
      (-> response
          http/await))))

