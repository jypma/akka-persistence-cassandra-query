package akka.persistence.cassandra.query

import java.time.Instant
import java.time.Duration
import scala.concurrent.duration.DurationInt
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.{ Finders, Matchers, WordSpec }
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.time.{ Seconds, Span }
import akka.actor.Props
import akka.persistence.cassandra.test.SharedActorSystem
import akka.persistence.query.EventEnvelope
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.testkit.TestProbe
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Publish
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.persistence.cassandra.MockitoOps

class PersistenceIdEventsPollerSpec extends WordSpec with Matchers with ScalaFutures with Eventually with MockitoOps 
  with SharedActorSystem {
  implicit val patience = PatienceConfig(timeout = Span(10, Seconds))
  val pubsub = DistributedPubSub(system).mediator

  "PersistenceIdEventsPoller" when {
    val startTime: Instant = Instant.ofEpochSecond(1444053395)

    class Fixture(initialEvents: Seq[EventEnvelope] = Seq.empty) {
      @volatile var events = initialEvents

      var now: Instant = startTime
      val cassandraOps = mock[CassandraOps]
      when(cassandraOps.findHighestSequenceNr("doc1")) thenAnswer new Answer[Int] {
        override def answer(invocation: InvocationOnMock) = events.size
      }
      
      
      when(cassandraOps.readEvents("doc1":=)(?, Long.MaxValue:=)) thenAnswer new Answer[Source[EventEnvelope,Any]] {
        override def answer(invocation: InvocationOnMock) = {
          val from = invocation.getArgumentAt(1, classOf[Long]).toInt
          Source(events.drop(from - 1).toList)
        }
      }

      val pubsub = TestProbe()
      val emitted = TestProbe()
      val poller = system.actorOf(
        PersistenceIdEventsPoller.props(cassandraOps, "doc1", Duration.ofSeconds(120), 
          pollDelay = 200.milliseconds, nowFunc = now, pubsub = Some(pubsub.ref)))
      emitted.send(poller, PersistenceIdEventsPoller.Subscribe)

      // Allow the publisher to pick up the initialEvents (which it'll do shortly after having queried for initialTime)
      eventually {
        verify(cassandraOps, atLeastOnce).readEvents("doc1")(initialEvents.size + 1, Long.MaxValue)
      }

      def cleanup () {
        system.stop(poller)
      }
    }

    def fixture(initialEvents: Seq[EventEnvelope] = Seq.empty)(testcode: Fixture => Any) = {
      val f = new Fixture(initialEvents)
      try testcode(f) finally f.cleanup()
    }

    "starting with no stored events for its persistence id" should {
      "not emit anything when starting up" in fixture() { f =>
        f.emitted.expectNoMsg(50.milliseconds)
      }

      "emit any new events that appear in the db, and then wait and poll" in fixture() { f =>
        val event = EventEnvelope(startTime.toEpochMilli, "doc1", 1, "hello")
        f.events :+= event
        f.emitted.expectMsg(event)
        f.emitted.expectNoMsg(50.milliseconds)
      }
      
      "emit any new events that appear in the db immediately if pubsub publishes an event" in fixture() { f =>
        f.pubsub.expectMsg(DistributedPubSubMediator.Subscribe("persistenceId:doc1", f.poller))
        
        val event = EventEnvelope(startTime.toEpochMilli, "doc1", 1, "hello")
        f.events :+= event
        f.poller ! "added:1"
        f.emitted.within(100.milliseconds) {
          f.emitted.expectMsg(event)
        }
      }      
    }

    "starting with some stored events for its persistence id" should {
      val initialEvents = EventEnvelope(startTime.toEpochMilli, "doc1", 1, "hello") :: Nil

      "not emit anything when starting up" in fixture(initialEvents) { f =>
        f.emitted.expectNoMsg(50.milliseconds)
      }

      "emit any new events that appear in the db" in fixture(initialEvents) { f =>
        val event = EventEnvelope(startTime.toEpochMilli, "doc1", 2, "hello")
        f.events :+= event
        pubsub ! Publish("persistenceId:doc1", "added:2")
        f.emitted.expectMsg(event)
      }
    }

    "noticing that the time window for the last emitted real-time event has closed" should {
      "complete itself" in fixture() { f =>
        f.emitted.watch(f.poller)

        val event = EventEnvelope(startTime.toEpochMilli, "doc1", 1, "hello")
        f.events :+= event
        pubsub ! Publish("persistenceId:doc1", "added:1")
        f.emitted.expectMsg(event)
        f.now = f.now plusMillis 300000
        f.emitted.expectTerminated(f.poller, 2.seconds)
      }
    }
    
    "encountering errors during polling" should {
      "sleep and poll again" in fixture() { f =>
        val x = new RuntimeException("Simulated failure")
        when(f.cassandraOps.readEvents("doc1":=)(?, Long.MaxValue:=)).thenReturn(Source.failed(x))
        Thread.sleep(400) // poll delay * 2

        when(f.cassandraOps.readEvents("doc1":=)(?, Long.MaxValue:=)) thenAnswer new Answer[Source[EventEnvelope,Any]] {
          override def answer(invocation: InvocationOnMock) = {
            val from = invocation.getArgumentAt(1, classOf[Long]).toInt
            Source(f.events.drop(from - 1).toList)
          }
        }
        val event = EventEnvelope(startTime.toEpochMilli, "doc1", 1, "hello")
        f.events :+= event
        f.emitted.expectMsg(event)        
      }
    }
  }
}
