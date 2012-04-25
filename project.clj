(defproject zilch "0.1.0-SNAPSHOT"
  :description "Zilch: A Clojure 0MQ Messaging Library"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.zeromq/zmq "2.0-SNAPSHOT"]]
  :dev-dependencies [[swank-clojure "1.2.1"]]
  :native-path "/usr/local/lib:/usr/lib"
  :jvm-opts ["-Djava.library.path=/usr/local/lib"])
