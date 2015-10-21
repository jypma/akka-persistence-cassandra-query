package akka.persistence.cassandra.query

import akka.persistence.cassandra.CassandraLifecycle
import org.scalatest.Matchers
import org.scalatest.WordSpec
import akka.persistence.cassandra.test.SharedActorSystem
import com.typesafe.config.ConfigFactory
import akka.persistence.PersistentActor
import akka.actor.ActorRef
import akka.testkit.TestProbe
import akka.actor.Props
import akka.stream.scaladsl.Sink
import akka.persistence.query.EventEnvelope
import CassandraReadJournalIntegrationSpec._
import akka.testkit.TestKit
import akka.actor.ActorSystem
import org.scalatest.WordSpecLike
import akka.stream.ActorMaterializer
import akka.actor.ExtendedActorSystem
import akka.persistence.cassandra.journal.CassandraJournalConfig
import akka.persistence.cassandra.Cassandra
import akka.persistence.cassandra.streams.rt.RealTime
import akka.stream.scaladsl.Source
import akka.persistence.cassandra.query.CassandraOps.IndexEntry
import akka.persistence.cassandra.streams.rt.Chronology
import java.time.Instant
import scala.concurrent.duration.DurationInt
import akka.actor.Status.Failure
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream
import org.scalatest.concurrent.ScalaFutures
import akka.persistence.cassandra.streams.Reaper


object CassandraReadJournalIntegrationSpec {
  val config = ConfigFactory.parseString(
    """
      |akka.loglevel = "DEBUG"
      |akka.loggers = ["akka.event.slf4j.Slf4jLogger"]
      |akka.logging-filter = "akka.event.slf4j.Slf4jLoggingFilter"
      |akka.persistence.snapshot-store.plugin = "cassandra-snapshot-store"
      |akka.persistence.journal.plugin = "cassandra-journal"
      |akka.persistence.journal.max-deletion-batch-size = 3
      |akka.persistence.publish-confirmations = on
      |akka.persistence.publish-plugin-commands = on
      |akka.test.single-expect-default = 10s
      |cassandra-journal.max-partition-size = 5
      |cassandra-journal.max-result-size = 3
      |cassandra-journal.port = 9142
      |cassandra-journal.timeWindowLength = 10s
      |cassandra-snapshot-store.port = 9142
    """.stripMargin)

  case class Event(content: String, timestamp: Instant = Instant.now) extends Timestamped
    
  class DocumentActor(probe: ActorRef) extends PersistentActor {
    override def persistenceId = context.self.path.name

    override def receiveRecover: Receive = handle

    override def receiveCommand: Receive = {
      case payload: String =>
        persist(Event(payload))(handle)
    }

    def handle: Receive = {
      case Event(payload, _) =>
        probe ! payload
    }
  }
}

class CassandraReadJournalIntegrationSpec extends TestKit(ActorSystem("test", config)) with WordSpecLike with Matchers with ScalaFutures with CassandraLifecycle {
  implicit val m = ActorMaterializer()

  "the CassandraReadJournal" when {
    "listening on an empty database" should {

      "pick up a single event and send it to a single running query" in {
        val testStart = System.currentTimeMillis()

        val probe = TestProbe()
        val doc = system.actorOf(Props(classOf[DocumentActor], probe.ref), "document-1")
        doc ! "change-1"
        probe.within(1.minute) {
          probe.expectMsg("change-1") // we know it's persisted once it hits the probe
        }

        val received = TestProbe()

        val journal = CassandraReadJournal.instance
        journal.eventsByTag("_all", 0).runWith(Sink.actorRef(received.ref, "complete"))

        received.within(1.minute) {
        	val event = received.expectMsgType[EventEnvelope]
        	// we don't validate event.offset, since setting that requires us to deserialize all events.
        	event.persistenceId should be ("document-1")
        	event.sequenceNr should be (1)
        	event.event shouldBe a[akka.util.ByteString]

        	// by default, akka uses Java serialization, which has serialized our Event.
        	val content = new ObjectInputStream(event.event.asInstanceOf[akka.util.ByteString].iterator.asInputStream).readObject().asInstanceOf[Event]
        	content.content should be ("change-1")
        }

        system.stop(received.ref)
        system.stop(doc)
        Reaper(received.ref,doc).futureValue
        journal.shutdown().futureValue
      }

      "eventually send all generated events to all clients" in {
        val probe = TestProbe()
        val doc = system.actorOf(Props(classOf[DocumentActor], probe.ref), "document-1")
        val journal = CassandraReadJournal.instance

        val messageCount = 100
        val queries = for (i <- 0 to messageCount) yield {
          Thread.sleep(10)
          doc ! s"change-{i}"
          probe.expectMsgType[String] // we know it's persisted once it hits the probe
          val received = TestProbe()
          journal.eventsByTag("_all", 0).runWith(Sink.actorRef(received.ref, "complete"))
          received
        }

        for (i <- 0 to messageCount) {
          val q = queries(i)
          for (j <- 0 to messageCount) {
            q.within(1.minute) {
            	q.expectMsgType[EventEnvelope]
            }
          }
        }

        for (i <- 0 to messageCount) system.stop(queries(i).ref)
        system.stop(doc)
        Reaper(queries.map(_.ref) :+ doc).futureValue
        journal.shutdown().futureValue
      }

      /*
      "discover real-time events as they are added, across partition boundaries" in {
        pending
      }
      * /
      */
    }
  }

}
