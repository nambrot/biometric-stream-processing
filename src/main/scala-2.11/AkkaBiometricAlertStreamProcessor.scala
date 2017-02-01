import BiometricAlertStreamProcessor.{BloodPressureEvent, HeartRateEvent, toEvent}
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.impl.SubFlowImpl
import akka.stream.{ActorMaterializer, ClosedShape, OverflowStrategy}
import akka.stream.scaladsl.{Flow, GraphDSL, Merge, RunnableGraph, Sink, Source, SubFlow}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, PropertyNamingStrategy}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._

/**
  * Created by nambrot on 1/28/17.
  */
object AkkaBiometricAlertStreamProcessor extends App {
  implicit val system = ActorSystem("QuickStart")
  implicit val materializer = ActorMaterializer()

  sealed trait Event {
    def timestamp: Long
    def userId: Integer
  }
  case class HeartRateEvent(userId: Integer, heartRate: Integer, timestamp: Long) extends Event
  case class BloodPressureEvent(userId: Integer, systolic: Integer, diastolic: Integer, timestamp: Long) extends Event

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

  case class Window(start: Long, end: Long, length: Long, step: Long)

  object Window {
    def windowsFor(ts: Long, length: Long, step: Long): Set[Window] = {
      val firstWindowStart = ts - ts % step - length + step
      val windowsPerEvent = (length / step).toInt
      (for (i <- 0 until windowsPerEvent) yield
        Window(
          firstWindowStart + i * step,
          firstWindowStart + i * step + length,
          step,
          length)
        ).toSet
    }
  }

  sealed trait WindowCommand {
    def w: Window
  }

  case class OpenWindow(w: Window) extends WindowCommand
  case class CloseWindow(w: Window) extends WindowCommand
  case class AddToWindow(ev: Event, w: Window) extends WindowCommand

  class CommandGenerator {
    private val MaxDelay = 0.seconds.toMillis
    private var watermark = 0L
    private val openWindows = mutable.Set[Window]()

    def forEvent(ev: Event, length: FiniteDuration, step: FiniteDuration): List[WindowCommand] = {
      watermark = math.max(watermark, ev.timestamp - MaxDelay)
      if (ev.timestamp < watermark) {
        println(s"Dropping event with timestamp: ${ev.timestamp}")
        Nil
      } else {
        val eventWindows = Window.windowsFor(ev.timestamp, length.toMillis, step.toMillis)

        val closeCommands = openWindows.flatMap { ow =>
          if (!eventWindows.contains(ow) && ow.end < watermark) {
            openWindows.remove(ow)
            Some(CloseWindow(ow))
          } else None
        }

        val openCommands = eventWindows.flatMap { w =>
          if (!openWindows.contains(w)) {
            openWindows.add(w)
            Some(OpenWindow(w))
          } else None
        }

        val addCommands = eventWindows.map(w => AddToWindow(ev, w))

        openCommands.toList ++ closeCommands.toList ++ addCommands.toList
      }
    }
  }


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

  val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit b: GraphDSL.Builder[NotUsed] =>
    import GraphDSL.Implicits._

    val merger = b.add(Merge[Event](2))

    bloodPressureSource
      .map(toEvent[BloodPressureEvent]) ~> merger.in(0)
    heartRateSource
      .map(toEvent[HeartRateEvent])  ~> merger.in(1)

    merger ~> eventsByWindowAndUser ~> complexAlerts ~> rateLimiter ~> Sink.foreach(println)

    ClosedShape
  })

  val a = g.run()

  bloodPressureQueueFuture.map { q =>
    q.offer(s"""{\"user_id\":12345,\"systolic\":120,\"diastolic\":80,\"timestamp\":${System.currentTimeMillis()}}""")
    q.offer(s"""{\"user_id\":12346,\"systolic\":80,\"diastolic\":80,\"timestamp\":${System.currentTimeMillis()}}""")
  }

  heartRateQueueFuture.map { q =>
    Thread.sleep(2000)
    q.offer(s"""{\"user_id\":12345,\"heart_rate\":200,\"timestamp\":${System.currentTimeMillis()}}""")
    q.offer(s"""{\"user_id\":12345,\"heart_rate\":200,\"timestamp\":${System.currentTimeMillis()}}""")
    q.offer(s"""{\"user_id\":12346,\"heart_rate\":101,\"timestamp\":${System.currentTimeMillis()}}""")
    Thread.sleep(2000)
    q.offer(s"""{\"user_id\":12345,\"heart_rate\":200,\"timestamp\":${System.currentTimeMillis()}}""")
    Thread.sleep(2000)
    q.offer(s"""{\"user_id\":12345,\"heart_rate\":200,\"timestamp\":${System.currentTimeMillis()}}""")
  }



}
