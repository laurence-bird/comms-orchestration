package servicetest.helpers

import cakesolutions.kafka.KafkaProducer
import com.ovoenergy.comms.serialisation.Serialisation.avroSerializer
import com.sksamuel.avro4s.{FromRecord, SchemaFor, ToRecord}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.Await
import scala.concurrent.duration.Duration
// Implicits
import com.ovoenergy.comms.serialisation.Codecs._

trait KafkaTesting {
  // TODO: Remove me when everyone has migrated to Aiven
  def legacyPublishOnce[E: SchemaFor: ToRecord](topic: String, hosts: String, event: E, timeout: Duration) = {

    val producer = KafkaProducer(
      KafkaProducer.Conf(
        new StringSerializer,
        avroSerializer[E],
        hosts
      )
    )
    try {
      val future = producer.send(new ProducerRecord[String, E](topic, event))
      // Enforce blocking behaviour
      Await.result(future, timeout)
    } catch {
      case e: Throwable => {
        throw new Exception(s"Failed to publish message to topic $topic with error $e")
      }
    } finally {
      producer.close()
    }
  }
}
