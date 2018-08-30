package servicetest.helpers

import com.ovoenergy.comms.helpers.Topic
import com.ovoenergy.comms.testhelpers.KafkaTestHelpers.withThrowawayConsumerFor
import com.sksamuel.avro4s.{FromRecord, SchemaFor}
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.reflect.ClassTag

trait KafkaConsumerExtensions {

  def withMultipleThrowawayConsumerFor[E1: SchemaFor: FromRecord: ClassTag,
                                       E2: SchemaFor: FromRecord: ClassTag,
                                       E3: SchemaFor: FromRecord: ClassTag,
                                       R](t1: Topic[E1],
                                          t2: Topic[E2],
                                          t3: Topic[E3])(f: (KafkaConsumer[String, Option[E1]],
                                                             KafkaConsumer[String, Option[E2]],
                                                             KafkaConsumer[String, Option[E3]]) => R): R = {
    withThrowawayConsumerFor(t1) { c1 =>
      withThrowawayConsumerFor(t2) { c2 =>
        withThrowawayConsumerFor(t3) { c3 =>
          f(c1, c2, c3)
        }
      }
    }
  }
}
