package akka.persistence.cassandra.query

import java.time.Instant

import scala.concurrent.duration.DurationInt

import org.mockito.Matchers.{ anyLong, eq => is }
import org.mockito.Mockito.{ atLeastOnce, mock, verify, when }
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

class PersistenceIdEventsPollerSpec extends WordSpec with Matchers with ScalaFutures with Eventually with SharedActorSystem {
  implicit val patience = PatienceConfig(timeout = Span(10, Seconds))

  "PersistenceIdEventsPoller" when {
    val startTime: Instant = Instant.ofEpochSecond(1444053395)

    class Fixture(initialEvents: Seq[EventEnvelope] = Seq.empty) {
      @volatile var events = initialEvents

      var now: Instant = startTime
      val cassandraOps = mock(classOf[CassandraOps])
      when(cassandraOps.findHighestSequenceNr("doc1")).thenAnswer(new Answer[Int] {
        override def answer(invocation: InvocationOnMock) = events.size
      })
      when(cassandraOps.readEvents(is("doc1"))(anyLong, is(Long.MaxValue))).thenAnswer(new Answer[Source[EventEnvelope,Any]] {
        override def answer(invocation: InvocationOnMock) = {
          val from = invocation.getArgumentAt(1, classOf[Long]).toInt
          Source(events.drop(from - 1).toList)
        }
      })

      val emitted = TestProbe()
      val poller = system.actorOf(Props(
        new PersistenceIdEventsPoller(cassandraOps, "doc1", pollDelay = 1.milliseconds, nowFunc = now)))
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
    }

    "starting with some stored events for its persistence id" should {
      val initialEvents = EventEnvelope(startTime.toEpochMilli, "doc1", 1, "hello") :: Nil

      "not emit anything when starting up" in fixture(initialEvents) { f =>
        f.emitted.expectNoMsg(50.milliseconds)
      }

      "emit any new events that appear in the db" in fixture(initialEvents) { f =>
        val event = EventEnvelope(startTime.toEpochMilli, "doc1", 2, "hello")
        f.events :+= event
        f.emitted.expectMsg(event)
      }
    }

    "noticing that the time window for the last emitted real-time event has closed" should {
      "complete itself" in fixture() { f =>
        f.emitted.watch(f.poller)

        val event = EventEnvelope(startTime.toEpochMilli, "doc1", 1, "hello")
        f.events :+= event
        f.emitted.expectMsg(event)
        f.now = f.now plusMillis 300000
        f.emitted.expectTerminated(f.poller, 1.second)
      }
    }
  }
}
