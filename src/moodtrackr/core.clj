(ns moodtrackr.core
  (:gen-class))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))

(def application-constants
  {:port 8099
   :service "http://localhost"})

(def twitter-oauth-permissions
  {:consumer-key  "MY6CSHxk7cfXahiX16M96cBIT"
   :consumer-secret "T0UiZC9uvoKL5Jag1lPK1L8XVSj5b9C4KYtX0IOJFZcqCBH4TO"
   :client-access-token "3100780247-sdiqtUMDZorEXHjD1q57wUUkelYnuWCnRhoq7gQ"
   :client-access-token-secret "VNRYhs3W3EfHux2KFTufyuhYEzoPAIiJWo8f5tygU1M82"})

(defn generate-callback-url [] (str (:service application-constants) ":" (:port application-constants) "/"))
