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
      |akka.persistence.query.journal.cassandra.allowedClockDrift = 1s
      |cassandra-journal.max-partition-size = 5
      |cassandra-journal.max-result-size = 3
      |cassandra-journal.port = 9142
      |cassandra-journal.time-window-length = 1s
      |cassandra-snapshot-store.port = 9142
    """.stripMargin)

  case class Event(content: String, timestamp: Instant = Instant.now) extends Timestamped {
    println(s"*** Created Event at ${timestamp.toEpochMilli()}") 
    def getTimestamp: Long = timestamp.toEpochMilli
  }
    
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
    
    class Fixture {
      val probe = TestProbe()
      val doc = system.actorOf(Props(classOf[DocumentActor], probe.ref))
      val persistenceId = doc.path.name
      val journal = CassandraReadJournal.instance
      def shutdown() {
        system.stop(doc)
        Reaper(doc).futureValue
        journal.shutdown().futureValue
      }
    }
    
    def fixture(code: Fixture => Unit) = {
      val f = new Fixture()
      try {
        code(f)
      } finally {
        f.shutdown()
      }
    }
    
    "listening on an empty database" should {

      "pick up a single event and send it to a single running query" in fixture { f =>
        import f._
        
        val testStart = System.currentTimeMillis()

        doc ! "change-1"
        probe.within(1.minute) {
          probe.expectMsg("change-1") // we know it's persisted once it hits the probe
        }

        val received = TestProbe()

        journal.eventsByTag("_all", 0).runWith(Sink.actorRef(received.ref, "complete"))

        received.within(1.minute) {
        	val envelope = received.expectMsgType[EventEnvelope]
        	// we don't validate event.offset, since setting that requires us to deserialize all events.
        	envelope.persistenceId should be (persistenceId)
        	envelope.sequenceNr should be (1)
        	envelope.event shouldBe an[EventPayload[_]]

        	val content = envelope.event.asInstanceOf[EventPayload[Event]]
        	content.deserialized.content should be ("change-1")
        	
        	val testedDeserialized = new ObjectInputStream(content.serialized.iterator.asInputStream).readObject().asInstanceOf[Event]
        	testedDeserialized.content should be ("change-1")
        }
      }

      "eventually send all generated events to all clients" in fixture { f =>
        import f._ 
  
        val messageCount = 40
        val queries = for (i <- 0 to messageCount) yield {
          Thread.sleep(200)
          doc ! s"change-{i}"
          probe.expectMsgType[String] // we know it's persisted once it hits the probe
          val received = TestProbe()
          journal.eventsByTag("_all", 0).runWith(Sink.actorRef(received.ref, "complete"))
          received
        }

        for (i <- 0 to messageCount) {
          val q = queries(i)
          q.within(1.minute) {
            for (j <- 0 to messageCount) {
              //println(s"*** Waiting for query $i, message $j") 
            	q.expectMsgType[EventEnvelope]
            }
          }
        }

        for (i <- 0 to messageCount) system.stop(queries(i).ref)
        Reaper(queries.map(_.ref)).futureValue
      }
      
      "keep sending events to a query as time windows progress" in fixture { f =>
        import f._ 
  
        val received = TestProbe()
        journal.eventsByTag("_all", 0).runWith(Sink.actorRef(received.ref, "complete"))
        
        val messageCount = 30
        for (i <- 0 to messageCount) {
          Thread.sleep(100)
          doc ! s"change-{i}"
          probe.expectMsgType[String] // we know it's persisted once it hits the probe
        }

        received.within(1.minute) {
          for (i <- 0 to messageCount) {
            println(s"*** Waiting for message $i") 
          	received.expectMsgType[EventEnvelope]
          }
        }

        system.stop(received.ref)
        Reaper(received.ref).futureValue
      }
      
      "pick up on changes when multiple changes are made during one time window" in fixture { f =>
        import f._
        
        // start the event stream
        val received = TestProbe()
        journal.eventsByTag("_all", 0).runWith(Sink.actorRef(received.ref, "complete"))
        
        def send(i:Int) = { 
          doc ! s"change-$i"
          probe.expectMsgType[String] // we know it's persisted once it hits the probe          
        }
        
        send(1)
        send(2)
        received.expectMsgType[EventEnvelope]
        received.expectMsgType[EventEnvelope]
        Thread.sleep(6000) // poll interval + 1
        
        send(3)
        send(4)
        received.expectMsgType[EventEnvelope]
        received.expectMsgType[EventEnvelope]
        Thread.sleep(6000) // poll interval + 1
        
        send(5)
        send(6)
        received.expectMsgType[EventEnvelope]
        received.expectMsgType[EventEnvelope]
        Thread.sleep(6000) // poll interval + 1
        
        send(7)
        send(8)
        received.expectMsgType[EventEnvelope]
        received.expectMsgType[EventEnvelope]
        Thread.sleep(6000) // poll interval + 1
      }
    }

    "answering a query with offset" should {
      "return only events from time windows at or after the offset" in fixture { f =>
        import f._
        
        def send(i:Int) = { 
          doc ! s"change-$i"
          probe.expectMsgType[String] // we know it's persisted once it hits the probe          
        }
        
        send(1)
        Thread.sleep(1000) // time window is 1 second
        send(2)
        Thread.sleep(1000) // time window is 1 second
        send(3)
        Thread.sleep(1000) // time window is 1 second
        send(4)
        val offset = System.currentTimeMillis()
        
        Thread.sleep(1000) // time window is 1 second
        send(5)
        Thread.sleep(1000) // time window is 1 second
        send(6)
        Thread.sleep(1000) // time window is 1 second
        
        val received = TestProbe()
        journal.eventsByTag("_all", offset).runWith(Sink.actorRef(received.ref, "complete"))
        val first = received.expectMsgType[EventEnvelope]
        first.sequenceNr should be (4)
        val second = received.expectMsgType[EventEnvelope]
        second.sequenceNr should be (first.sequenceNr + 1)
        val third = received.expectMsgType[EventEnvelope]
        third.sequenceNr should be (second.sequenceNr + 1)
      }
    }
  }
}
