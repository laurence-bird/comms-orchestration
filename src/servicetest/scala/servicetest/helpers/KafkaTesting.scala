package servicetest.helpers

import com.ovoenergy.comms.helpers.Topic
import com.ovoenergy.comms.model.{Feedback, FeedbackStatus}
import com.ovoenergy.comms.testhelpers.KafkaTestHelpers.withThrowawayConsumerFor
import com.sksamuel.avro4s.{FromRecord, SchemaFor}
import org.apache.kafka.clients.consumer.KafkaConsumer
import servicetest.BaseSpec
import com.ovoenergy.comms.testhelpers.KafkaTestHelpers._

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

trait KafkaTesting { _: BaseSpec =>

  def withMultipleThrowawayConsumersFor[E1: SchemaFor: FromRecord: ClassTag,
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

  def withMultipleThrowawayConsumersFor[E1: SchemaFor: FromRecord: ClassTag,
  E2: SchemaFor: FromRecord: ClassTag,
  E3: SchemaFor: FromRecord: ClassTag,
  E4: SchemaFor: FromRecord: ClassTag,
  R](t1: Topic[E1],
     t2: Topic[E2],
     t3: Topic[E3], t4: Topic[E4])(f: (KafkaConsumer[String, Option[E1]],
    KafkaConsumer[String, Option[E2]],
    KafkaConsumer[String, Option[E3]],
    KafkaConsumer[String, Option[E4]],
    ) => R): R = {
    withThrowawayConsumerFor(t1) { c1 =>
      withThrowawayConsumerFor(t2) { c2 =>
        withThrowawayConsumerFor(t3) { c3 =>
          withThrowawayConsumerFor(t4) { c4 =>
            f(c1, c2, c3, c4)
          }
        }
      }
    }
  }

  def expectFeedbackEvents(pollTime: FiniteDuration = 25000.millisecond,
                           noOfEventsExpected: Int,
                           consumer: KafkaConsumer[String, Option[Feedback]],
                           expectedStatuses: Set[FeedbackStatus]) = {
    if (noOfEventsExpected < expectedStatuses.size)
      fail(
        s"List of expected status: ${expectedStatuses.size} needs to be less than the total number of events expected ${noOfEventsExpected}")

    val feedbackEvents = consumer.pollFor(pollTime, noOfEventsExpected)
    feedbackEvents.map(_.status) should contain theSameElementsAs expectedStatuses
  }
}
