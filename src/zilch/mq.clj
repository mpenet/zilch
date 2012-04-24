(ns zilch.mq
  (:refer-clojure :exclude [send])
  (:import [org.zeromq ZMQ ZMQ$Context ZMQ$Socket]
           [java.util Random]))

(defn context [threads]
  (ZMQ/context threads))

(defmacro with-context
  [id threads & body]
  `(let [~(with-meta id {:tag "org.zeromq.ZMQ$Context"}) (context ~threads)]
     (try ~@body
          (finally (.term ~id)))))

(def sndmore ZMQ/SNDMORE)

(def router ZMQ/XREP)
(def dealer ZMQ/XREQ)
(def req ZMQ/REQ)
(def rep ZMQ/REP)
(def xreq ZMQ/XREQ)
(def xrep ZMQ/XREP)
(def pub ZMQ/PUB)
(def sub ZMQ/SUB)
(def pair ZMQ/PAIR)
(def push ZMQ/PUSH)
(def pull ZMQ/PULL)

(defn socket
  [^ZMQ$Context context type]
  (.socket context type))

(defn bind
  [^ZMQ$Socket socket url]
  (doto socket
    (.bind url)))

(defn connect
  [^ZMQ$Socket socket url]
  (doto socket
    (.connect url)))

(defn subscribe
  ([^ZMQ$Socket socket ^String topic]
     (doto socket
       (.subscribe (.getBytes topic))))
  ([^ZMQ$Socket socket]
     (subscribe socket "")))

(defn unsubscribe
  ([^ZMQ$Socket socket ^String topic]
     (doto socket
       (.unsubscribe (.getBytes topic))))
  ([^ZMQ$Socket socket]
     (unsubscribe socket "")))

(defprotocol PZMessage
  (encode [message]))

(extend-protocol PZMessage
  String
  (encode [message]
    (.getBytes message)))

(defn send
  ([^ZMQ$Socket socket message flags]
     (.send socket (encode message) flags))
  ([^ZMQ$Socket socket message]
     (send socket message ZMQ/NOBLOCK)))

(defn recv
  ([^ZMQ$Socket socket flags]
     (.recv socket flags))
  ([^ZMQ$Socket socket]
     (recv socket 0)))

(defn recv-all
  ([^ZMQ$Socket socket flags]
     (loop [acc []]
       (let [msg (recv socket flags)]
         (if (.hasReceiveMore socket)
           (recur (conj acc msg))
           (conj acc msg)))))
  ([^ZMQ$Socket socket]
     (recv-all socket 0)))

(defn identify
  [^ZMQ$Socket socket ^String name]
  (.setIdentity socket (.getBytes name)))

(defn set-id
  ([^ZMQ$Socket socket n]
    (let [rdn (Random. (System/currentTimeMillis))]
      (identify socket (str (.nextLong rdn) "-" (.nextLong rdn) (int n)))))
  ([^ZMQ$Socket socket]
     (set-id socket 0)))