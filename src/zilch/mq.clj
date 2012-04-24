(ns zilch.mq
  (:refer-clojure :exclude [send])
  (:import [org.zeromq ZMQ ZMQ$Context ZMQ$Socket]))

(defn context [threads]
  (ZMQ/context threads))

(defmacro with-context
  [id threads & body]
  `(let [~id (context ~threads)]
     (try ~@body
          (finally (.term ~id)))))

(def sndmore ZMQ/SNDMORE)

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

(defmulti send (fn [^ZMQ$Socket socket message & flags]
                 (class message)))

(defmethod send String
  ([^ZMQ$Socket socket ^String message flags]
     (.send socket (.getBytes message) flags))
  ([^ZMQ$Socket socket ^String message]
     (send socket message ZMQ/NOBLOCK)))

(defn recv
  ([^ZMQ$Socket socket flags]
     (.recv socket flags))
  ([^ZMQ$Socket socket]
     (recv socket 0)))
