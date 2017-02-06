import java.util

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, PropertyNamingStrategy}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

class JsonDeserializer[T: Manifest] extends Deserializer[T] {
  val mapper = new ObjectMapper with ScalaObjectMapper
  mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  mapper.registerModule(DefaultScalaModule)

  override def configure(configs: util.Map[String, _], isKey: Boolean) = {

  }

  override def close() = {}

  override def deserialize(topic: String, data: Array[Byte]) = {
    try {
      mapper.readValue[T](new String(data, "UTF-8"))
    }
    catch {
      case e: Exception => throw new Exception(e);
    }
  }
}

class JsonSerializer[T: Manifest] extends Serializer[T] {
  val mapper = new ObjectMapper with ScalaObjectMapper
  mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  mapper.registerModule(DefaultScalaModule)

  override def configure(configs: util.Map[String, _], isKey: Boolean) = {}

  override def close() = {}

  override def serialize(topic: String, data: T) = {
    try {
      mapper.writeValueAsString(data).getBytes("UTF-8")
    }
    catch {
      case e: Exception => throw new Exception(e);
    }
  }
}

class JsonSerde[T: Manifest] extends Serde[T] {
  override def configure(configs: util.Map[String, _], isKey: Boolean) = {}
  override def close() = {}
  override def serializer() = { new JsonSerializer[T] }

  override def deserializer() = { new JsonDeserializer[T] }
}
