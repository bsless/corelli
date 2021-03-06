(ns corelli.core
  (:require
   [clojure.spec.alpha :as s]
   [clojure.core.async :as a]
   [more.async :as ma]
   [clojure.data]))

(defn kw->fn
  [kw]
  (when kw (resolve (symbol kw))))

(defn kfn? [kw] (ifn? (kw->fn kw)))

(s/def ::name (s/or :keyword keyword?
                    :string string?
                    :number number?
                    :symbol symbol?))

(s/def :chan/name ::name)

(comment
  (s/valid? :chan/name 1)
  (s/valid? :chan/name [])
  (s/explain-data :chan/name []))

(s/def :chan/size int?)

(s/def ::simple-chan
  (s/keys
   :req [:chan/name]))

(s/def ::sized-chan
  (s/keys
   :req [:chan/size
         :chan/name]))

(s/def :buffer/size int?)

(s/def ::fixed-buffer (s/keys :req [:buffer/size]))

(s/def :buffer/fn fn?)
(s/def :buffer.fn/args (s/* any?))

(defmulti buffer-type :buffer/type)
(defmethod buffer-type :buffer.type/blocking [_] (s/keys :req [:buffer/size]))
(defmethod buffer-type :buffer.type/sliding [_] (s/keys :req [:buffer/size]))
(defmethod buffer-type :buffer.type/dropping [_] (s/keys :req [:buffer/size]))

(s/def :chan/buffer (s/multi-spec buffer-type :buffer/type))

(defmulti compile-buffer :buffer/type)

(defmethod compile-buffer :buffer.type/blocking
  [{:keys [:buffer/size]}]
  (a/buffer size))

(defmethod compile-buffer :buffer.type/sliding
  [{:keys [:buffer/size]}]
  (a/sliding-buffer size))

(defmethod compile-buffer :buffer.type/dropping
  [{:keys [:buffer/size]}]
  (a/dropping-buffer size))

(comment
  (compile-buffer {:buffer/type :buffer.type/blocking
                   :buffer/size 1}))

(s/def ::buffered-chan
  (s/keys
   :req [:chan/name
         :chan/buffer]))

(defmulti chan-type :chan/type)

(defmethod chan-type :chan.type/simple [_] ::simple-chan)
(defmethod chan-type :chan.type/sized [_] ::sized-chan)
(defmethod chan-type :chan.type/buffered [_] ::buffered-chan)

(s/def ::chan (s/multi-spec chan-type :chan/type))

(defmulti compile-chan :chan/type)

(defmethod compile-chan :chan.type/simple [_] (a/chan))

(defmethod compile-chan :chan.type/sized
  [{:keys [:chan/size]}]
  (a/chan size))

(defmethod compile-chan :chan.type/buffered
  [{:keys [:chan/buffer]}]
  (a/chan (compile-buffer buffer)))

(comment
  (compile-chan {:chan/type :chan.type/buffered
                 :chan/buffer {:buffer/type :buffer.type/sliding
                               :buffer/size 2}}))

(comment

  (s/explain-data ::chan {:chan/type :chan.type/simple
                          :chan/name :in})

  (s/explain-data ::chan {:chan/type :chan.type/sized
                          :chan/size 1
                          :chan/name :in})

  (s/explain-data ::sized-chan {:chan/type :chan.type/sized
                                :chan/size 1
                                :chan/name :in})

  (s/explain-data ::chan {:chan/type :chan.type/sized
                          :chan/name :in
                          :chan/size 1})

  (s/explain-data :chan/buffer {:buffer/size 1
                                :buffer/type :buffer.type/blocking})

  (s/conform ::chan {:chan/name :out
                     :chan/type :chan.type/buffered
                     :chan/buffer
                     {:buffer/size 1
                      :buffer/type :buffer.type/blocking}})
  )

(s/def :worker/name ::name)

(defmulti worker-type :worker/type)

(defmulti compile-worker (fn [worker _env] (get worker :worker/type)))

(defmulti ports :worker/type)

;;; PIPELINE

(s/def :pipeline/to :chan/name)
(s/def :pipeline/from :chan/name)
(s/def :pipeline/size int?)
(s/def :pipeline/xf fn?)
(s/def :worker/pipeline
  (s/keys :req [:pipeline/to
                :pipeline/from
                :pipeline/size
                :pipeline/xf]))

(defmethod worker-type :worker.type/pipeline [_]
  (s/keys :req [:worker/name :worker/pipeline]))

(defmethod compile-worker :worker.type/pipeline
  [{{to :pipeline/to
     from :pipeline/from
     size :pipeline/size
     xf :pipeline/xf} :worker/pipeline} env]
  (a/pipeline size (env to) xf (env from)))

(defmethod worker-type :worker.type/pipeline-blocking [_]
  (s/keys :req [:worker/name :worker/pipeline]))

(defmethod compile-worker :worker.type/pipeline-blocking
  [{{to :pipeline/to
     from :pipeline/from
     size :pipeline/size
     xf :pipeline/xf} :worker/pipeline} env]
  (a/pipeline-blocking size (env to) xf (env from)))

(defmethod worker-type :worker.type/pipeline-async [_]
  (s/keys :req [:worker/name :worker/pipeline]))

(defmethod compile-worker :worker.type/pipeline-async
  [{{to :pipeline/to
     from :pipeline/from
     size :pipeline/size
     af :pipeline/xf} :worker/pipeline} env]
  (a/pipeline-async size (env to) af (env from)))

(doseq [t [:worker.type/pipeline :worker.type/pipeline-blocking :worker.type/pipeline-async]]
  (defmethod ports t
    [{{to :pipeline/to
       from :pipeline/from} :worker/pipeline}]
    #{{:port/name from
       :port/direction :port.direction/in}
      {:port/name to
       :port/direction :port.direction/out}}))

;;; BATCH

(s/def :batch/from :chan/name)
(s/def :batch/to :chan/name)
(s/def :batch/size int?)
(s/def :batch/timeout int?)
(s/def :batch/rf fn?)
(s/def :batch/init fn?)
(s/def :batch/async? boolean?)

(s/def :worker/batch
  (s/keys :req [:batch/from
                :batch/to
                :batch/size
                :batch/timeout]
          :opt [:batch/rf
                :batch/init
                :batch/async?]))

(defmethod worker-type :worker.type/batch [_]
  (s/keys :req [:worker/name :worker/batch]))

(defmethod compile-worker :worker.type/batch
  [{{from :batch/from
     to :batch/to
     size :batch/size
     timeout :batch/timeout
     rf :batch/rf
     init :batch/init
     async? :batch/async?
     :or {rf conj
          init (constantly [])}} :worker/batch} env]
  (let [from (env from)
        to (env to)]
    (if async?
      (ma/batch! from to size timeout rf init)
      (a/thread (ma/batch!! from to size timeout rf init)))))

(defmethod ports :worker.type/batch
  [{{to :batch/to
     from :batch/from} :worker/batch}]
  #{{:port/name from
     :port/direction :port.direction/in}
    {:port/name to
     :port/direction :port.direction/out}})

;;; MULT

(s/def :mult/from :chan/name)
(s/def :mult/to (s/* :chan/name))

(s/def :worker/mult
  (s/keys :req [:mult/from]
          :opt [:mult/to]))

(defmethod worker-type :worker.type/mult [_]
  (s/keys :req [:worker/name :worker/mult]))

(defmethod compile-worker :worker.type/mult
  [{{from :mult/from
     to :mult/to} :worker/mult} env]
  (let [mult (a/mult (env from))]
    (doseq [ch to]
      (a/tap mult (env ch)))
    mult))

(defmethod ports :worker.type/mult
  [{{to :mult/to
     from :mult/from} :worker/mult}]
  (into
   #{{:port/name from
      :port/direction :port.direction/in}}
   (map (fn [to] {:port/name to
                 :port/direction :port.direction/out}))
   to))

(comment
  (ports
   {:worker/type :worker.type/mult
    :worker/name :cbm
    :worker/mult {:mult/from   :in
                  :mult/to     [:cbp-in :user-in]}}))

;;; TODO :worker.type/mix

;;; PUBSUB

(s/def :pubsub/pub :chan/name)
(s/def :sub/topic any?)
(s/def :sub/chan :chan/name)
(s/def :pubsub/sub (s/* (s/keys :req [:sub/topic :sub/chan])))
(s/def :pubsub/topic-fn fn?)

(s/def :worker/pubsub
  (s/keys :req [:pubsub/pub :pubsub/topic-fn]
          :opt [:pubsub/sub ]))

(defmethod worker-type :worker.type/pubsub [_]
  (s/keys :req [:worker/name :worker/pubsub]))

(defmethod compile-worker :worker.type/pubsub
  [{{pub :pubsub/pub
     sub :pubsub/sub
     tf  :pubsub/topic-fn} :worker/pubsub} env]
  (let [p (a/pub (env pub) tf)]
    (doseq [{:keys [:sub/topic :sub/chan]} sub]
      (a/sub p topic (env chan)))
    p))

(defmethod ports :worker.type/pubsub
  [{{to :pubsub/sub
     from :pubsub/pub} :worker/pubsub}]
  (into
   #{{:port/name from
      :port/direction :port.direction/in}}
   (map (fn [to] {:port/name to
                  :port/direction :port.direction/out}))
   to))

;;; PRODUCER

(s/def :produce/chan :chan/name)
(s/def :produce/fn fn?)
(s/def :produce/async? boolean?)
(s/def :worker/produce (s/keys :req [:produce/chan :produce/fn]
                               :opt [:produce/async?]))

(defmethod worker-type :worker.type/produce [_]
  (s/keys :req [:worker/name :worker/produce]))

(defmethod compile-worker :worker.type/produce
  [{{ch :produce/chan
     f  :produce/fn
     async? :produce/async?} :worker/produce} env]
  (let [ch (env ch)]
    (if async?
      (ma/produce-call! ch f)
      (a/thread (ma/produce-call!! ch f)))))

(defmethod ports :worker.type/produce
  [{{to :produce/chan} :worker/produce}]
  #{{:port/name to
     :port/direction :port.direction/out}})

;;; CONSUMER

(s/def :consume/chan :chan/name)
(s/def :consume/fn fn?)
(s/def :consume/checked? boolean?)
(s/def :consume/async? boolean?)
(s/def :worker/consume (s/keys :req [:consume/chan :consume/fn]
                               :opt [:consume/checked? :consume/async?]))

(defmethod worker-type :worker.type/consume [_]
  (s/keys :req [:worker/name :worker/consume]))

(defmethod compile-worker :worker.type/consume
  [{{ch :consume/chan
     f  :consume/fn
     async? :consume/async?
     checked? :consume/checked?} :worker/consume} env]
  (let [ch (env ch)]
    (if async?
      ((if checked?
         ma/consume-checked-call!
         ma/consume-call!) ch f)
      (a/thread ((if checked?
                   ma/consume-checked-call!!
                   ma/consume-call!!) ch f)))))

(defmethod ports :worker.type/consume
  [{{from :consume/chan} :worker/consume}]
  #{{:port/name from
     :port/direction :port.direction/in}})

;;; SPLIT

(s/def :split/from :chan/name)
(s/def :split/to (s/map-of any? :chan/name))
(s/def :split/fn fn?)
(s/def :split/dropping? boolean?)

(s/def :worker/split (s/keys :req [:split/from :split/to :split/fn]
                             :opt [:split/dropping?]))

(defmethod worker-type :worker.type/split [_]
  (s/keys :req [:worker/name :worker/split]))

(defmethod compile-worker :worker.type/split
  [{{from :split/from
     to :split/to
     f :split/fn
     dropping? :split/dropping?} :worker/split} env]
  ((if dropping? ma/split?! ma/split!) f (env from) (env to)))

(defmethod ports :worker.type/split
  [{{to :split/to
     from :split/from} :worker/split}]
  (into
   #{{:port/name from
      :port/direction :port.direction/in}}
   (map (fn [to] {:port/name to
                  :port/direction :port.direction/out}))
   to))

;;; REDUCTIONS

(s/def :reductions/from :chan/name)
(s/def :reductions/to :chan/name)
(s/def :reductions/rf fn?)
(s/def :reductions/init fn?)
(s/def :reductions/async? boolean?)
(s/def :worker/reductions
  (s/keys :req [:reductions/from
                :reductions/to
                :reductions/rf
                :reductions/init]
          :opt [:reductions/async?]))

(defmethod worker-type :worker.type/reductions [_]
  (s/keys :req [:worker/name :worker/type :worker/reductions]))

(defmethod compile-worker :worker.type/reductions
  [{{from :reductions/from
     to :reductions/to
     rf :reductions/rf
     init :reductions/rf
     async? :reductions/async?} :worker/reductions} env]
  (let [from (env from)
        to (env to)]
    (if async?
      (ma/reductions! rf init from to)
      (a/thread
        (ma/reductions!! rf init from to)))))

(defmethod ports :worker.type/reductions
  [{{to :reductions/to
     from :reductions/from} :worker/reductions}]
  #{{:port/name from
     :port/direction :port.direction/in}
    {:port/name to
     :port/direction :port.direction/out}})

;;; MODEL

(defn connected?
  [node chans]
  (every?
   #(contains? chans (:port/name %))
   (ports node)))

(defn connected-model?
  [{:keys [channels workers]}]
  (let [chans (into #{} (map :chan/name) channels)]
    (every? #(connected? % chans) workers)))

(s/def ::connected connected-model?)

(s/def ::worker (s/multi-spec worker-type :worker/type))

(s/def :model/channels (s/+ ::chan))
(s/def :model/workers (s/+ ::worker))

(s/def ::model (s/keys :req-un [:model/channels :model/workers]))

(s/def ::correct-model (s/and ::connected))

(defn compile-model
  [{:keys [channels workers]}]
  (let [chans (reduce
               (fn [m spec]
                 (assoc m (:chan/name spec) (compile-chan spec)))
               {}
               channels)
        env (fn [lookup]
              (if-some [ch (get chans lookup)]
                ch
                (throw (ex-info "Channel not found" chans))))
        workers (reduce
                 (fn [m spec]
                   (assoc m (:worker/name spec) (compile-worker spec env)))
                 {}
                 workers)]
    {:chans chans :workers workers}))

(s/fdef compile-model
  :args (s/cat :model ::model))

(comment
  (def model
    {:channels [{:chan/name :in
                 :chan/type :chan.type/sized
                 :chan/size 1}
                {:chan/name :out
                 :chan/type :chan.type/sized
                 :chan/size 1}]
     :workers [{:worker/name :producer
                :worker/type :worker.type/produce
                :worker/produce
                {:produce/chan :in
                 :produce/async? true
                 :produce/fn (let [a (atom 0)]
                               (fn drive []
                                 (Thread/sleep 1000)
                                 (swap! a inc)))}}
               {:worker/name :pipeline
                :worker/type :worker.type/pipeline-blocking
                :worker/pipeline
                {:pipeline/from :in
                 :pipeline/to :out
                 :pipeline/size 4
                 :pipeline/xf (map (fn [x] (println x) (Thread/sleep 2500) x))}}
               {:worker/name :consumer
                :worker/type :worker.type/consume
                :worker/consume
                {:consume/chan :out
                 :consume/fn (fn [x] (println :OUT x))
                 :consume/async? true}}]})

  (s/valid? ::model model)
  (s/valid? ::connected model)
  (def system (compile-model model))

  (a/close! (:in (:chans system))))

