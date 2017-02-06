import java.util.Properties

import BiometricAlertStreamProcessor.{BloodPressureEvent, HeartRateEvent, toEvent}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.apache.kafka.streams.kstream.{JoinWindows, KStream, KStreamBuilder, Transformer}
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.{KeyValueStore, Stores}

import scala.concurrent.duration._
/**
  * Created by nambrot on 2/4/17.
  */
object KafkaBiometricAlertStreamProcessor extends App {
  import KeyValueImplicits._
  val streamsConfiguration = new Properties()
  streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-biometric-alert-stream-processing")
  streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181")
  streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
  streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
  streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10")
  val stringSerde: Serde[String] = Serdes.String()
  val integerSerde: Serde[Integer] = Serdes.Integer()

  val builder = new KStreamBuilder()

  val bloodPressureStream: KStream[String, String] = builder.stream("rawBloodPressureEvents")
  val heartRateStream: KStream[String, String] = builder.stream("rawHeartRateEvents")

  val bloodPressureEvents: KStream[Integer, Option[BloodPressureEvent]] =
    bloodPressureStream
      .mapValues(toEvent[BloodPressureEvent])
      .map((_, v: BloodPressureEvent) => new KeyValue(v.userId, Some(v)))

  val heartRateEvents: KStream[Integer, Option[HeartRateEvent]] =
    heartRateStream
      .mapValues(toEvent[HeartRateEvent])
      .map((_, v: HeartRateEvent) => new KeyValue(v.userId, Some(v)))

  val combinedEvents: KStream[Integer, (Option[HeartRateEvent], Option[BloodPressureEvent])] =
    heartRateEvents
      .outerJoin(
        bloodPressureEvents,
        (hrEvent, bpEvent: Option[BloodPressureEvent]) => (hrEvent, bpEvent),
        JoinWindows.of(15.seconds.toMillis),
        integerSerde,
        new JsonSerde[Option[HeartRateEvent]],
        new JsonSerde[Option[BloodPressureEvent]]
      )

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

  val streams = new KafkaStreams(builder, streamsConfiguration)
  streams.start()

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("client.id", "KafkaProducer")
  props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[Integer, String](props)

  producer.send(new ProducerRecord[Integer, String]("rawBloodPressureEvents", 1, "{\"user_id\":12345,\"systolic\":92,\"diastolic\":80}"))
  producer.send(new ProducerRecord[Integer, String]("rawHeartRateEvents", 1, "{\"user_id\":12345,\"heart_rate\":200}"))
  producer.send(new ProducerRecord[Integer, String]("rawHeartRateEvents", 1, "{\"user_id\":12345,\"heart_rate\":200}"))
}
