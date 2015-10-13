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
      |cassandra-journal.indexed-persistence-id-prefixes = ["document"]
      |cassandra-snapshot-store.port = 9142
    """.stripMargin)

  class DocumentActor(probe: ActorRef) extends PersistentActor {
    override def persistenceId = context.self.path.name

    override def receiveRecover: Receive = handle

    override def receiveCommand: Receive = {
      case payload: String =>
        persist(payload)(handle)
    }

    def handle: Receive = {
      case payload: String =>
        probe ! payload
        probe ! lastSequenceNr
        probe ! recoveryRunning
    }
  }
}

class CassandraReadJournalIntegrationSpec extends TestKit(ActorSystem("test", config)) with WordSpecLike with Matchers with ScalaFutures with CassandraLifecycle {
  implicit val m = ActorMaterializer()

  "the CassandraReadJournal" when {
    "listening on an empty database" should {
      "discover real-time events as they are added, across partition boundaries" in {
        val testStart = System.currentTimeMillis()

        val probe = TestProbe()
        val doc = system.actorOf(Props(classOf[DocumentActor], probe.ref), "document-1")
        doc ! "change-1"

        val received = TestProbe()

        val journal = CassandraReadJournal.instance
        journal.eventsByTag("_all", 0).runWith(Sink.actorRef(received.ref, "complete"))

        received.within(1.minute) {
        	val event = received.expectMsgType[EventEnvelope]
        	event.offset should be (testStart +- 30000)
        	event.persistenceId should be ("document-1")
        	event.sequenceNr should be (1)
        	event.event shouldBe a[akka.util.ByteString]

        	// by default, akka uses Java serialization, which has serialized our String.
        	val content = new ObjectInputStream(new ByteArrayInputStream(event.event.asInstanceOf[akka.util.ByteString].toArray)).readObject()
        	content should be ("change-1")
        }

        system.stop(received.ref)
        journal.shutdown().futureValue
      }

      "eventually send all generated events to all clients" in {

      }
    }
  }

}
