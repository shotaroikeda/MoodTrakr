(defproject moodtrackr "0.1.0-SNAPSHOT"
  :description "Keep track of the moods around you in real time!"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [http.async.client "1.1.0"]]
  :main ^:skip-aot moodtrackr.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
