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

  def withMultipleThrowawayConsumersFor[E1: SchemaFor: FromRecord: ClassTag, E2: SchemaFor: FromRecord: ClassTag, R](
      t1: Topic[E1],
      t2: Topic[E2])(f: (KafkaConsumer[String, E1], KafkaConsumer[String, E2]) => R): R = {
    withThrowawayConsumerFor(t1) { c1 =>
      withThrowawayConsumerFor(t2) { c2 =>
        f(c1, c2)
      }
    }
  }

  def expectFeedbackEvents(pollTime: FiniteDuration = 25000.millisecond,
                           noOfEventsExpected: Int,
                           consumer: KafkaConsumer[String, Feedback],
                           expectedStatuses: Set[FeedbackStatus]) = {
    if (noOfEventsExpected < expectedStatuses.size)
      fail(
        s"List of expected status: ${expectedStatuses.size} needs to be less than the total number of events expected ${noOfEventsExpected}")

    note("Waiting for Feedback events")
    val feedbackEvents = consumer.pollFor(pollTime, noOfEventsExpected)
    feedbackEvents.map(_.status) should contain theSameElementsAs expectedStatuses
  }
}
