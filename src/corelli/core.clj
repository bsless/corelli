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

(defn produce-blocking
  "Like `produce` but blocking in a thread."
  ([ch f] (produce ch f true))
  ([ch f close?]
   (a/thread
     (loop [v (f)]
       (if (and v (a/>!! ch v))
         (recur (f))
         (when close?
           (a/close! ch)))))))

(defn produce-bound-blocking
  "Like `produce-blocking`, but calls `pre` and `post` in the context
  of the thread.
  The value returned by `pre` is passed to `f` and `post`.
  `pre` is called before the loop, `post` after it exhausts.

  Useful for non thread safe objects which throw upon being accessed from
  different threads."
  [ch f close? pre post]
   (a/thread
     (let [pv (pre)]
       (loop [v (f pv)]
         (if (and v (a/>!! ch v))
           (recur (f pv))
           (when close?
             (a/close! ch))))
       (post pv))))

(defn consume
  "Takes values repeatedly from channels and applies f to them.

  The opposite of produce.

  Stops consuming values when the channel is closed."
  [ch f]
  (a/go-loop [v (a/<! ch)]
    (when v
      (f v)
      (recur (a/<! ch)))))

(defn consume?
  "Takes values repeatedly from channels and applies f to them.
  Recurs only when f returns a non false-y value.

  The opposite of produce.

  Stops consuming values when the channel is closed."
  [ch f]
  (a/go-loop [v (a/<! ch)]
    (when v
      (when (f v)
        (recur (a/<! ch))))))

(defn consume-blocking
  "Takes values repeatedly from channels and applies f to them.

  The opposite of produce.

  Stops consuming values when the channel is closed."
  [ch f]
  (a/thread
    (loop [v (a/<!! ch)]
      (when v
        (f v)
        (recur (a/<!! ch))))))

(defn consume-blocking?
  "Takes values repeatedly from channels and applies f to them.
  Recurs only when f returns a non false-y value.

  The opposite of produce.

  Stops consuming values when the channel is closed."
  [ch f]
  (a/thread
    (loop [v (a/<!! ch)]
      (when v
        (when (f v)
          (recur (a/<!! ch)))))))


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

(def node-specs
  [::mult
   ::pipe
   ::pipeline
   ::pipeline-blocking
   ::pipeline-async
   ::producer
   ::consumer])

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

(def channel-specs
  [::unbuffered-channel
   ::xf-channel
   ::buffered-channel
   ::buffered-xf-channel])

(defn- explain-specs-data
  [v specs]
  (let [explanations (map #(s/explain-data % v) specs)]
    (zipmap specs explanations)))

(defn- explain-specs-str
  [v specs]
  (let [explanations (map #(s/explain-str % v) specs)]
    (zipmap specs explanations)))

(defn- one-of-specs?
  "Checks v satisfies one of specs.
  Returns nil and reports result if not

  Workaround for lacking `or` types in spec.alpha.
  waiting for spec2"
  [v specs]
  (some #(if (s/valid? % v) %) specs))

(defn compile-channel
  [m]
  (if-let [spec (one-of-specs? m channel-specs)]
    (let [buffer (kw->fn (get m ::buffn))
          size (get m ::size) ;; (chan nil) == (chan)
          args (get m ::buffn-args [])
          xf (kw->fn (get m ::xf))
          exh (kw->fn (get m ::ex-handler))]
      (case spec
        ::buffered-xf-channel (a/chan (apply buffer size args) xf exh)
        ::buffered-channel (a/chan (apply buffer size args))
        ::unbuffered-channel (a/chan size)))
    (throw (new Exception "channel does not match any spec."))))

(defn explain-channel
  [m]
  (doseq [[k s] (explain-specs-str m channel-specs)
          :let [o (str k " failed:\n" s)]]
    (println o)))

(defn compile-node
  [node edges]
  (if-let [spec (one-of-specs? node node-specs)]
    (let [from (get edges (get node ::from))
          to (get edges (get node ::to))
          xf (kw->fn (get node ::xf))
          close? (get node ::close? true)
          n (get node ::jobs 1)]
      (case spec
        ::pipe (a/pipe from to close?)
        ::pipeline (a/pipeline n to xf from close?)
        ::pipeline-async (a/pipeline-async n to xf from close?)
        ::pipeline-blocking (a/pipeline-blocking n to xf from close?)
        ::producer (produce to xf close?)
        ::consumer (consume from xf)
        ::mult (let [m (a/mult from)]
                 (doseq [k (get node ::to+)]
                   (when-let [ch (get edges k)]
                     (a/tap m ch close?)))
                 m)))
    (throw (new Exception "node does not match any spec."))))

(defn explain-node
  [m]
  (doseq [[k s] (explain-specs-str m node-specs)
          :let [o (str k " failed:\n" s)]]
    (println o)))

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
