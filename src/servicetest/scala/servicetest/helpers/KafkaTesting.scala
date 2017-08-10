package servicetest.helpers

import com.ovoenergy.comms.helpers.Kafka
import com.ovoenergy.comms.model.{CancelledV2, OrchestrationStartedV2}
import com.ovoenergy.comms.model.email.OrchestratedEmailV3
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.{KafkaConsumer => ApacheKafkaConsumer}
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration.{Deadline, FiniteDuration, _}

// Implicits
import com.ovoenergy.comms.serialisation.Codecs._
trait KafkaTesting extends BeforeAndAfterAll { self: Suite =>

  implicit val config: Config = ConfigFactory.load("servicetest.conf")

  /*
  Note: The consumers and producers are lazy so we don't create and assign them
  until after we've tested the topics can be consumed from.

  BUT! You have to assign the consumer to the topic before you send any events to it,
  hence the initKafkaConsumers() method below.
   */

  /*lazy val orchestrationStartedConsumer: ApacheKafkaConsumer[String, Option[OrchestrationStartedV2]] =
    Kafka.aiven.orchestrationStarted.v2.consumer(fromBeginning = true, useMagicByte = false)
  lazy val orchestratedEmailConsumer: ApacheKafkaConsumer[String, Option[OrchestratedEmailV3]] =
    Kafka.aiven.orchestratedEmail.v3.consumer(fromBeginning = true, useMagicByte = false)
  lazy val cancelledConsumer: ApacheKafkaConsumer[String, Option[CancelledV2]] =
    Kafka.aiven.cancelled.v2.consumer(fromBeginning = true, useMagicByte = false)
   */
  //lazy val allKafkaConsumers = Seq(orchestratedEmailConsumer, orchestrationStartedConsumer, cancelledConsumer)

  /*def initKafkaConsumers(): Unit = {
    allKafkaConsumers.foreach { consumer =>
      println("Polling consumer")
      consumer.poll(100)
      println("Poll complete")
    }
  }*/

  def pollForEvents[E](pollTime: FiniteDuration = 20000.millisecond,
                       noOfEventsExpected: Int,
                       consumer: ApacheKafkaConsumer[String, Option[E]],
                       topic: String): Seq[E] = {
    @tailrec
    def poll(deadline: Deadline, events: Seq[E]): Seq[E] = {
      if (deadline.hasTimeLeft) {
        val polledEvents: Seq[E] = consumer
          .poll(100)
          .records(topic)
          .asScala
          .toList
          .flatMap(_.value())
        val eventsSoFar: Seq[E] = events ++ polledEvents
        eventsSoFar.length match {
          case n if n == noOfEventsExpected => eventsSoFar
          case exceeded if exceeded > noOfEventsExpected =>
            throw new Exception(s"Consumed more than $noOfEventsExpected events from $topic")
          case _ => poll(deadline, eventsSoFar)
        }
      } else throw new Exception(s"Events didn't appear within the timelimit (got ${events.size} events)")
    }
    poll(pollTime.fromNow, Nil)
  }

  def pollForEventsYo[E](pollTime: FiniteDuration = 20000.millisecond,
                         consumer: ApacheKafkaConsumer[String, Option[E]],
                         topic: String): Seq[E] = {
    @tailrec
    def poll(deadline: Deadline, events: Seq[E]): Seq[E] = {
      if (deadline.hasTimeLeft) {
        val polledEvents: Seq[E] = consumer
          .poll(100)
          .records(topic)
          .asScala
          .toList
          .flatMap(_.value())
        val eventsSoFar: Seq[E] = events ++ polledEvents
        poll(deadline, eventsSoFar)
      } else events
    }
    poll(pollTime.fromNow, Nil)
  }

  //def shutdownAllKafkaConsumers(): Unit = allKafkaConsumers.foreach(_.close())

}
