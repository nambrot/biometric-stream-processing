import java.util.{Calendar, Date}

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, PropertyNamingStrategy}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * Created by nambrot on 1/27/17.
  */

object BiometricAlertStreamProcessor extends App {
  def toEvent[T: Manifest](string: String) = {
    val objectMapper = new ObjectMapper with ScalaObjectMapper
    objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE)
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.readValue[T](string)
  }

  def reducer(a: (List[BloodPressureEvent], List[HeartRateEvent]), b: (List[BloodPressureEvent], List[HeartRateEvent])) = (a, b) match {
    case ((bp1, hr1), (bp2, h2)) => (bp1 ++ bp2, hr1 ++ h2)
  }

  case class HeartRateEvent(userId: Integer, heartRate: Integer)
  case class BloodPressureEvent(userId: Integer, systolic: Integer, diastolic: Integer)

  val conf = new SparkConf().setMaster("local[2]").setAppName("BiometricAlertStreamProcessor")

  val sc = new SparkContext((conf))
  val ssc = new StreamingContext(sc, Seconds(1))
  ssc.checkpoint("/Users/nambrot/devstuff/biometric-stream-processing/tmp")
  val heartRateLines = mutable.Queue[RDD[String]]()
  val hearRateInput = ssc.queueStream(heartRateLines)

  val bloodPressureLines = mutable.Queue[RDD[String]]()
  val bloodPressureInput = ssc.queueStream(bloodPressureLines)

  val bloodPressureStream = bloodPressureInput.map(toEvent[BloodPressureEvent])
  val heartRateStream = hearRateInput.map(toEvent[HeartRateEvent])

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
      .reduceByKeyAndWindow(reducer(_, _), Seconds(5), Seconds(1))

  val alertStream = combinedStream
    .filter {
      case (_, (bloodPressureEvents, heartRateEvents)) =>
        bloodPressureEvents.exists(_.systolic < 100) && heartRateEvents.exists(_.heartRate > 100)
      case _ => false
    }.map {
      case (id, (_, _)) => (id, s"User $id has a problem")
    }

  def updateFunction(alertStrings: Seq[String], stateOption: Option[(Boolean, Date, String)]): Option[(Boolean, Date, String)] = {
    (alertStrings, stateOption) match {
      case (_, Some((_, triggerTime, string))) => {
        val cal = Calendar.getInstance()
        cal.add(Calendar.SECOND, -5)
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

  ssc.start()

  bloodPressureLines += sc.makeRDD(Seq(
    "{\"user_id\":12345,\"systolic\":120,\"diastolic\":80}",
    "{\"user_id\":12346,\"systolic\":80,\"diastolic\":80}"
  ))

  Thread.sleep(2000)
  heartRateLines += sc.makeRDD(Seq(
    "{\"user_id\":12345,\"heart_rate\":200}",
    "{\"user_id\":12345,\"heart_rate\":200}",
    "{\"user_id\":12346,\"heart_rate\":101}"
  ))
  ssc.awaitTerminationOrTimeout(8000)
}
