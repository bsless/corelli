(ns corelli.core
  (:require
   [clojure.spec.alpha :as s]
   [clojure.core.async :as a]))

(defn kw->fn
  [kw]
  (when kw (resolve (symbol kw))))

(defn produce
  "Puts the contents repeatedly calling f into the supplied channel.

  By default the channel will be closed after the items are copied,
  but can be determined by the close? parameter.

  Returns a channel which will close after the items are copied.

  Based on clojure.core.async/onto-chan.
  Equivalent to (onto-chan ch (repeatedly f)) but cuts out the seq."
  ([ch f] (produce ch f true))
  ([ch f close?]
   (a/go-loop [v (f)]
     (if (and v (a/>! ch v))
       (recur (f))
       (when close?
         (a/close! ch))))))

(defn consume
  "Takes values repeatedly from channels and applies f to them.

  The opposite of produce.

  Stops consuming values when the channel is closed."
  [ch f]
  (a/go-loop [v (a/<! ch)]
    (when v
      (f v)
      (recur (a/<! ch)))))

(defn split*
  "Takes a channel, function f :: v -> k and a map of keys to channels k -> ch,
  routing the values v from the input channel to the channel such that
  (f v) -> ch.

  (get m (f v)) must be non-nil for every v! "
  ([f ch m]
   (a/go-loop []
     (let [v (a/<! ch)]
       (if (nil? v)
         (doseq [c (vals m)] (a/close! c))
         (if-let [o (get m (f v))]
           (when (a/>! o v)
             (recur))
           (throw (Exception. "Channel does not exist"))))))))

(defn split-maybe
  "Takes a channel, function f :: v -> k and a map of keys to channels k -> ch,
  routing the values v from the input channel to the channel such that
  (f v) -> ch.

  If (f v) is not in m, the value is dropped"
  ([f ch m]
   (a/go-loop []
     (let [v (a/<! ch)]
       (if (nil? v)
         (doseq [c (vals m)] (a/close! c))
         (if-let [o (get m (f v))]
           (when (a/>! o v)
             (recur))
           (recur)))))))

(defn kfn? [kw] (ifn? (kw->fn kw)))

(s/def ::from keyword?)
(s/def ::to keyword?)
(s/def ::to+ (s/+ ::to))
(s/def ::jobs integer?)
(s/def ::close? boolean?)
(s/def ::type
  #{:pipe
    :pipeline
    :pipeline-async
    :pipeline-blocking
    :producer
    :consumer
    :mult})
(s/def ::xf kfn?)
(s/def ::name keyword?)
(s/def ::buffn kfn?)
(s/def ::size integer?)
(s/def ::buffn-or-n (s/or :buffer-fn kfn? :size integer?))
(s/def ::ex-handler fn?)

(s/def ::mult
  (s/keys :req [::name ::from ::to+]
          :opt [::close?]))

(s/def ::pipe
  (s/keys :req [::name ::from ::to]
          :opt [::close?]))

(s/def ::pipeline
  (s/keys :req [::name ::from ::to ::xf]
          :opt [::close? ::jobs]))

(s/def ::pipeline-async
  (s/keys :req [::name ::from ::to ::xf]
          :opt [::close? ::jobs]))

(s/def ::pipeline-blocking
  (s/keys :req [::name ::from ::to ::xf]
          :opt [::close? ::jobs]))

(s/def ::producer
  (s/keys :req [::name ::to ::xf]
          :opt [::close? ::jobs]))

(s/def ::consumer
  (s/keys :req [::name ::from ::xf]
          :opt [::jobs]))

(s/def ::unbuffered-channel
  (s/keys :req [::name]
          :opt [::size]))

(s/def ::xf-channel
  (s/keys :req [::name ::size ::xf]
          :opt [::ex-handler]))

(s/def ::buffered-channel
  (s/keys :req [::name ::buffn ::size]
          :opt [::buffn-args]))


(s/def ::buffered-xf-channel
  (s/keys :req [::name ::buffn ::size ::xf]
          :opt [::buffn-args ::ex-handler]))


(defn compile-channel
  [m]
  (let [buffer (kw->fn (get m ::buffn))
        size (get m ::size)
        args (get m ::buffn-args [])
        xf (get m ::xf)
        exh (get m ::ex-handler)]
    (cond
      (s/valid? ::buffered-xf-channel m)
      (a/chan (apply buffer size args) xf exh)
      (s/valid? ::buffered-channel m)
      (a/chan (apply buffer size args))
      (s/valid? ::unbuffered-channel m)
      (a/chan size))))

(defn compile-node
  [node edges]
  (let [from (get edges (get node ::from))
        to (get edges (get node ::to))
        xf (get node ::xf)
        close? (get node ::close? true)
        n (get node ::jobs 1)]
    (cond
      (s/valid? ::pipe node) (a/pipe from to close?)
      (s/valid? ::pipeline node) (a/pipeline n to xf from close?)
      (s/valid? ::pipeline-async node) (a/pipeline-async n to xf from close?)
      (s/valid? ::pipeline-blocking node) (a/pipeline-blocking n to xf from close?)
      (s/valid? ::producer node) (produce to xf close?)
      (s/valid? ::consumer node) (consume from xf)
      (s/valid? ::mult node)
      (let [m (a/mult from)]
        (doseq [k (get node ::to+)]
          (when-let [ch (get edges k)]
            (a/tap m ch close?)))
        m))))

(defn compile-topology
  [edges nodes]
  (let [edges
        (reduce
         (fn [m e]
           (assoc m (::name e) (compile-channel e)))
         edges)
        nodes
        (reduce
         (fn [m n]
           (assoc m (::name n) (compile-node n))))]
    {:edges edges :nodes nodes}))
