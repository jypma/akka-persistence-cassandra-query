package akka.persistence.cassandra.query

import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.WordSpec
import akka.persistence.cassandra.test.SharedActorSystem
import org.scalatest.Matchers
import org.mockito.Mockito.{ mock, verify, when, atLeastOnce }
import org.mockito.Matchers.{ anyLong, eq => is }
import org.mockito.stubbing.Answer
import scala.concurrent.duration._
import akka.actor.Props
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.Sink
import akka.testkit.TestProbe
import akka.persistence.query.EventEnvelope
import org.mockito.invocation.InvocationOnMock

class CassandraRealTimeEventsSpec extends WordSpec with Matchers with ScalaFutures with Eventually with SharedActorSystem {
  "CassandraRealTimeEvents" when {
    class Fixture(initialEvents: Seq[EventEnvelope] = Seq.empty) {
      @volatile var events = initialEvents

    	val cassandraOps = mock(classOf[CassandraOps])
    	when(cassandraOps.findHighestSequenceNr("doc1")).thenAnswer(new Answer[Int] {
        override def answer(invocation: InvocationOnMock) = events.size
      })
      when(cassandraOps.readEvents(is("doc1"), anyLong, is(Long.MaxValue))).thenAnswer(new Answer[Source[EventEnvelope,Any]] {
        override def answer(invocation: InvocationOnMock) = {
          val from = invocation.getArgumentAt(1, classOf[Long]).toInt
          Source(events.drop(from - 1).toList)
        }
      })

    	val emitted = TestProbe()
			val publisher = Source.actorPublisher(Props(
			  new CassandraRealTimeEvents(cassandraOps, "doc1", 1.milliseconds)
	    )).runWith(Sink.actorRef(emitted.ref, "done"))

	    // Allow the publisher to pick up the initialEvents (which it'll do shortly after having queried for initialTime)
      eventually {
        verify(cassandraOps, atLeastOnce).readEvents("doc1", initialEvents.size + 1, Long.MaxValue)
      }
    }

    "starting with no stored events for its persistence id" should {
      "not emit anything when starting up" in new Fixture {
        emitted.expectNoMsg(50.milliseconds)
      }

      "emit any new events that appear in the db" in new Fixture {
        val event = EventEnvelope(0, "doc1", 1, "hello")
        events :+= event
      	emitted.expectMsg(event)
      }
    }

    "starting with some stored events for its persistence id" should {
      val initialEvents = EventEnvelope(0, "doc1", 1, "hello") :: Nil

      "not emit anything when starting up" in new Fixture(initialEvents) {
        emitted.expectNoMsg(50.milliseconds)
      }

      "emit any new events that appear in the db" in new Fixture(initialEvents) {
        val event = EventEnvelope(0, "doc1", 2, "hello")
        events :+= event
      	emitted.expectMsg(event)
      }
    }
  }
}
