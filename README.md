# Comparing Streaming Frameworks

I recently decided that I wanted to learn more about the world of stream processing. I felt quite comfortable in the world of CRUD but knew that if I wanted to elevate as a software engineer, I couldn't ignore the tremendous progress and change that is happening when it comes to dealing with unbounded and plentiful data.

At my company, for the infrastructure engineer position, we have a challenge that seemed suitable for me to start from. The problem is as following:

> Suppose you have a continous stream of heart rate events `{"user_id":12345, "heart_rate":200}` and blood pressure events `{"user_id":12345,"systolic":120,"diastolic":80}`. If for any user, the heart rate is above 100 and the systolic blood pressure is below 100, trigger an alert.

The prompt seemed simple enough, but as we will see, the world of streaming requires us to think very diffirently and there are gotchas and edge cases to be had. I also noticed that a lot of content out there mostly focus on getting a basic word count out there, so hopefully this will be help to dive into a more complex (yet still totally unrealistic) problem with joining and windowing.

One of the very first things I ran into as a streaming-novice is the need to window your streams. I.e. when and for how long are biometrics events valid for? When are two events valid togehter? How do I express that in my stream processing framework? The challenge actually refers to this problem:

> What would change about your implementation to support time-windowing operations, e.g. low blood pressure and high heart rate within a 60 minute interval?

You might be tempted to think that you could just create fixed 60 minute intervals, consider events in that interval and trigger an alert but what then happens when two matching events happen right around the window boundary? Thus, you'll need a different approach, and as we'll see, it depends on the features of the streaming framework.


# Biometric Alert Streaming with Spark

The first framework I looked at was Apache Spark. While Spark gained Structured Streaming in 2.0, it is still evolving, so I decided to use the traditional Spark Streaming feature. Spark has been one of the greater successes in the big data landscape in recent years due to its great performance while handling large amounts of data, high-level API in Scala, Java and Python, similar programming model for streaming and batch and widespread industry support. That means that there is a good amount of documentation out there.

To get started, we have the most basic spark setup

```scala
val conf = new SparkConf().setMaster("local[2]").setAppName("BiometricAlertStreamProcessor")

val sc = new SparkContext((conf))
val ssc = new StreamingContext(sc, Seconds(1))
val heartRateLines = mutable.Queue[RDD[String]]()
val hearRateInput = ssc.queueStream(heartRateLines)

val bloodPressureLines = mutable.Queue[RDD[String]]()
val bloodPressureInput = ssc.queueStream(bloodPressureLines)
```

In reality, you'd use a real input and a real cluster. For the purpose of this excercise, I just create a manual DStream. The next thing we'll want to do is to parse the data into case classes:

```scala
case class HeartRateEvent(userId: Integer, heartRate: Integer)
case class BloodPressureEvent(userId: Integer, systolic: Integer, diastolic: Integer)

def toEvent[T: Manifest] = (string: String) => {
  val objectMapper = new ObjectMapper with ScalaObjectMapper
  objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE)
  objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  objectMapper.registerModule(DefaultScalaModule)
  objectMapper.readValue[T](string)
}

val bloodPressureStream = bloodPressureInput.map(toEvent[BloodPressureEvent])
val heartRateStream = hearRateInput.map(toEvent[HeartRateEvent])

```

Now to the interesting piece: the join. Spark streaming supports quite nice ergononimcs when it comes to joining (and consequently windowing the stream). Let's take a look

```scala
def reducer(
  a: (List[BloodPressureEvent], List[HeartRateEvent]),
  b: (List[BloodPressureEvent], List[HeartRateEvent])
  ) = (a, b) match {
  case ((bp1, hr1), (bp2, h2)) => (bp1 ++ bp2, hr1 ++ h2)
}

val combinedStream =
  bloodPressureStream
    .map((event) => (event.userId, List(event)))
    .fullOuterJoin(heartRateStream.map((event) => (event.userId, List(event))))
    .map({
      case (id, (Some(x), Some(y))) => (id, (x, y))
      case (id, (_, Some(y))) => (id, (List(), y))
      case (id, (Some(x), _)) => (id, (x, List()))
      case (id, _) => (id, (List(), List()))
    })
    .reduceByKeyAndWindow(reducer(_, _), Minutes(60), Minutes(1))
```

First, we transform both stream of events into streams of lists of events. That will be helpful for the `fullOuterJoin`, as it combines two streams of elements into one stream of Option pairs. In the following map I "un-option" the individual pairs. Finally, in the `.reduceByKeyAndWindow` I window the stream so that in each window of 60 minutes, I have all events that apply for a given user.

From here, we can simply filter that combined stream for the conditions of our alert:

```scala
val alertStream = combinedStream
  .filter {
    case (_, (bloodPressureEvents, heartRateEvents)) =>
      bloodPressureEvents.exists(_.systolic < 100) && heartRateEvents.exists(_.heartRate > 100)
    case _ => false
  }.map {
    case (id, (_, _)) => (id, s"User $id has a problem")
  }
```

If you print out this stream, you will note that it will print the "same" alert every minute. That is because the `combinedStream` window is sliding every minute, thus for the following minutes after an alert was detected, the window will still contain event combinations that should trigger. If we didn't have sliding windows, but hopping ones, we'd potentially miss out of valid event combinations across the window boundary. Thus the only way to ensure proper alerting is to have minimally sliding windows. Take the following "visualization":

```
bloodPressure  - - - - - x - - - - - - - - - -
heartRate      - - - - - - - x - - - - - - - -

alert          - - - - - - - x x x x x x - - -
```

If you print `alertStream`, you'll get an output like

```
-------------------------------------------
Time: 1485622596000 ms
-------------------------------------------

-------------------------------------------
Time: 1485622597000 ms
-------------------------------------------

-------------------------------------------
Time: 1485622598000 ms
-------------------------------------------
(12346,User 12346 has a problem)

-------------------------------------------
Time: 1485622599000 ms
-------------------------------------------
(12346,User 12346 has a problem)

-------------------------------------------
Time: 1485622600000 ms
-------------------------------------------
(12346,User 12346 has a problem)

-------------------------------------------
Time: 1485622601000 ms
-------------------------------------------
```

If we only want to "trigger" once, we'll need to maintain state, track alerts and have a cooldown on them. We can achieve such by using `updateStateByKey`:

```scala
def updateFunction(alertStrings: Seq[String], stateOption: Option[(Boolean, Date, String)]): Option[(Boolean, Date, String)] = {
  (alertStrings, stateOption) match {
    case (_, Some((_, triggerTime, string))) => {
      val cal = Calendar.getInstance()
      cal.add(Calendar.Minute, -60)
      if (triggerTime.before(cal.getTime)) None else Some((false, triggerTime, string))
    }
    case (Nil, None) => None
    case (Seq(string, _*), None) => Some((true, Calendar.getInstance().getTime(), string))
  }
}
val alertState =
  alertStream
    .updateStateByKey(updateFunction)
    .filter {
      case (_, (bool, _, _)) => bool
    }.map {
      case (_, (_, _, string)) => string
    }

alertState.print
```

This way, we'll only ever get one alert that "lasts" for 60 minutes.



# Biometric Alert Streaming with Akka Streams

The next framework we'll be looking at is Akka Streams. Akka enjoys high regard in the JVM ecosystem due to its solid foundations of providing concurrency, especially with the actor model it borrowed from Erlang. A lot of other projects have been using Akka under the hood. In fact, [Spark in the past has been built upon Akka](https://www.quora.com/How-is-Spark-built-on-top-of-the-Akka-toolkit/answer/Ethan-Petuchowski-1). Akka expanded beyond providing simple concurrency primitives with higher level abstractions such as Akka Streams. I have also been meaning to check out their [event sourcing library "Persistence Query"](http://doc.akka.io/docs/akka/2.4/scala/persistence-query.html), but that shall be the topic of another post.

You should soon notice that Akka Streams is very different from Spark Streaming. It does not support clustering and has not native support for windowing and joins. So well have to do it ourselves.

To get started:

```scala
implicit val system = ActorSystem("QuickStart")
implicit val materializer = ActorMaterializer()

def peekMatValue[T, M](src: Source[T, M]): (Source[T, M], Future[M]) = {
  val p = Promise[M]
  val s = src.mapMaterializedValue { m =>
    p.trySuccess(m)
    m
  }
  (s, p.future)
}

val (bloodPressureSource, bloodPressureQueueFuture) = peekMatValue(Source.queue[String](100, OverflowStrategy.dropHead))
val (heartRateSource, heartRateQueueFuture) = peekMatValue(Source.queue[String](100, OverflowStrategy.dropHead))
```

We have to use peekMatValue per [akka/akka#17769](https://github.com/akka/akka/issues/17769) to be able to add elements dynamically to the sources via the futures. Next, we'll want to to join the two sources. To do that, we'll be creating list of events again, and then merge them.

```scala
 val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit b: GraphDSL.Builder[NotUsed] =>
  import GraphDSL.Implicits._

  val merger = b.add(Merge[(List[BloodPressureEvent], List[HeartRateEvent])](2))

  bloodPressureSource.map(toEvent[BloodPressureEvent]).map(event => (List(event), List())) ~> merger.in(0)
  heartRateSource.map(toEvent[HeartRateEvent]).map(event => (List(), List(event))) ~> merger.in(1)

  merger ~> Sink.foreach(println)

  ClosedShape
})

```

Similarly like joining, windowing isn't natively supported. I'll be mostly following the excellent guide from [SoftwareMill](https://softwaremill.com/windowing-data-in-akka-streams/):

```scala
val eventsByWindowAndUser = Flow[Event]
//  windowing the stream by creating WindowCommands
  .statefulMapConcat { () =>
    val generator = new CommandGenerator()
    ev => generator.forEvent(ev, 10.seconds, 1.second)
  }
//   Group by the windows
  .groupBy(164, _.w)
//   "Finish" the each window group when we detect a close window
  .takeWhile(!_.isInstanceOf[CloseWindow])
//   Only collect the events with the windows
  .collect { case AddToWindow(e, w) => (w, e) }
//    Further group the group of streams by the user id and aggregate the events
    .groupBy(64, _._2.userId)
    .fold(List[Event]()) { case (agg, ev) => ev._2 :: agg }
//    Merge back the sub streams
    .mergeSubstreams
    .async
  .mergeSubstreams
  .async

val complexAlerts =
  Flow[List[Event]]
    .filter((list) =>
      list.exists((e) => e.isInstanceOf[BloodPressureEvent] && e.asInstanceOf[BloodPressureEvent].systolic < 100) &&
        list.exists((e) => e.isInstanceOf[HeartRateEvent] && e.asInstanceOf[HeartRateEvent].heartRate > 100)
    ).map { case (event :: _) =>  (event.userId, s"User ${event.userId} has a complex alert") }

...

merger ~> eventsByWindowAndUser ~> complexAlerts ~> Sink.foreach(println)
```

Compared to the Spark example, this obviously is a lot more code to window the stream and group by user id. But it does give a nice insight as to how one might accomplish windowing and at the end, we get alerts for the right windows and users. The last thing we'll need to do is to "rate-limit" the stream to only trigger once per user for a certain time.

```scala
val rateLimiter =
  Flow[(Integer, String)]
    .statefulMapConcat { () =>
      val hashMap = new mutable.HashMap[Integer, (Long, String)]()
      (el) => el match {
        case (userId, alert) => {
          val current = System.currentTimeMillis()
          hashMap.get(userId) match {
            case Some((oldTime, oldAlert)) => {
              if (oldTime + 15.seconds.toMillis < current) {
                hashMap.put(userId, (current, alert))
                List[(Integer, String)]((userId, alert))
              } else
                List[(Integer, String)]()
            }
            case None => {
              hashMap.put(userId, (current, alert))
              List[(Integer, String)]((userId, alert))
            }
          }
        }
      }
    }
```

Once again, very similar to Spark's `updateStateByKey`, but a little more verbose. To the point that I'm wondering what I'm missing because this seems very easily generlizable functionality.

Overall, akka has very nice documentation and a lot of other functionality which make it a great choice to use it as the backbone of your system. However, if your focus on streaming, it certainly seems that other competitors have a little more functionality, although I really like their Graph DSL and stages abstraction.

# Biometric Alert Streaming with Kafka Streams

Kafka Streams has been getting a lot of attention since its announcement since May 2016. It has the promise of simplification by avoiding the need for having a streaming-specific cluster a la Spark or Flink. It is built right into your Kafka cluster by using Kafka topics as its distribution model, making Kafka Streams a "mere library". Let's take a look at how we would implement the biometric alerting example. I omitted some boilerplate/setup code in the following snippets. Also note that there is some difficulties around using Kafka Streams as it does not have a native Scala API yet.

```scala
val bloodPressureStream: KStream[String, String] = builder.stream("rawBloodPressureEvents")
val heartRateStream: KStream[String, String] = builder.stream("rawHeartRateEvents")

val bloodPressureEvents: KStream[Integer, Option[BloodPressureEvent]] =
  bloodPressureStream
    .mapValues(toEvent[BloodPressureEvent])
    .map((_, v: BloodPressureEvent) => (v.userId, Some(v)))

val heartRateEvents: KStream[Integer, Option[HeartRateEvent]] =
  heartRateStream
    .mapValues(toEvent[HeartRateEvent])
    .map((_, v: HeartRateEvent) => (v.userId, Some(v)))
```

Here we just parse the raw strings into case classes and make them Options. That will be important for the following join, since Kafka's outer join will possible leave `null` as the values. ([Kafka Join Semantics for more info](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Join+Semantics)). Other than the `null`, the ergonimics of the joins in Kafka Streams are quite nice.

```scala
val combinedEvents: KStream[Integer, (Option[HeartRateEvent], Option[BloodPressureEvent])] =
  heartRateEvents
    .outerJoin(
      bloodPressureEvents,
      (hrEvent, bpEvent: Option[BloodPressureEvent]) => (hrEvent, bpEvent),
      JoinWindows.of(60.minutes.toMillis),
      integerSerde,
      new JsonSerde[Option[HeartRateEvent]],
      new JsonSerde[Option[BloodPressureEvent]]
    )
```

Another complexity addition is having to think about [Serdes](http://docs.confluent.io/3.0.0/streams/developer-guide.html#data-types-and-serialization). Since Kafka Streams uses Kafka as the distribution model, you haev to think about how to de-/serialize state to Kafka. I haven't much experience with serialization in the jvm/apache ecosystem so I just created a generic JsonSerde, but Avro seems preferential.

After the join, we can operate on the streams like any other stream processing framework:

```scala
val filteredEvents: KStream[Integer, (HeartRateEvent, BloodPressureEvent)] =
  combinedEvents
    .filter((userId: Integer, tuple: (Option[HeartRateEvent], Option[BloodPressureEvent])) => {
      (userId, tuple) match {
        case (_, (Some(hrEvent: HeartRateEvent), Some(bpEvent: BloodPressureEvent))) =>
          hrEvent.heartRate > 100 && bpEvent.systolic < 100
        case _ => false
      }
    }).mapValues {
      case (Some(hrEvent: HeartRateEvent), Some(bpEvent: BloodPressureEvent)) => (hrEvent, bpEvent)
    }
```

While Spark has `updateStateByKey` and Akka has `statefulMapConcat`, Akka's stateful aggregation is somewhat more limited, so if we want to have the "rate-limiting" functionality, we'll have to drop down to the Processor API:

```scala
case class StateRecord(hrEvent: HeartRateEvent, bpEvent: BloodPressureEvent, time: Long)

val countStore = Stores.create("Limiter")
  .withKeys(integerSerde)
  .withValues(new JsonSerde[StateRecord])
  .persistent()
  .build();
builder.addStateStore(countStore)

class Limiter extends Transformer[Integer, (HeartRateEvent, BloodPressureEvent), KeyValue[Integer, (HeartRateEvent, BloodPressureEvent)]] {
  var context: ProcessorContext = null;
  var store: KeyValueStore[Integer, StateRecord] = null;

  override def init(context: ProcessorContext) = {
    this.context = context
    this.store = context.getStateStore("Limiter").asInstanceOf[KeyValueStore[Integer, StateRecord]]
  }

  override def transform(key: Integer, value: (HeartRateEvent, BloodPressureEvent)) = {
    val current = System.currentTimeMillis()
    val newRecord = StateRecord(value._1, value._2, current)
    store.get(key) match {
      case StateRecord(_, _, time) if time + 15.seconds.toMillis < current => {
        store.put(key, newRecord)
        (key, value)
      }
      case StateRecord(_, _, _) => null
      case null => {
        store.put(key, newRecord)
        (key, value)
      }
    }
  }

  override def punctuate(timestamp: Long) = null
  override def close() = {}
}

filteredEvents
  .transform(() => new Limiter(), "Limiter")
  .map((userId: Integer, pair: (HeartRateEvent, BloodPressureEvent)) =>
    new KeyValue(userId, s"User $userId has a problem")
  ).print
```

While it is clear that Kafka Streams is quite young, I think it has great a great foundation, and with recent additions like queryable state stores and Kafka's general versatility, I am quite excited to see where Kafka Streams will go. Articles like [this](http://why-not-learn-something.blogspot.ch/2016/12/why-and-when-distributed-stream.html?spref=tw) especially resonate with me when it comes to streaming being a potential game changer to general application development, and Kafka seems well-poised to be a major player.
