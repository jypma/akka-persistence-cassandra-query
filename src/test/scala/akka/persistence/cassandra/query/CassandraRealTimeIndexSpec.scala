package akka.persistence.cassandra.query

import java.time.{ Duration, Instant }

import scala.concurrent.duration.DurationInt

import org.mockito.Mockito.{ mock, verify, when, atLeastOnce }
import org.scalatest.{ Matchers, WordSpec }
import org.scalatest.concurrent.{ Eventually, ScalaFutures }

import akka.actor.Props
import akka.persistence.cassandra.query.CassandraOps.IndexEntry
import akka.persistence.cassandra.test.SharedActorSystem
import akka.stream.scaladsl.{ Sink, Source }
import akka.testkit.TestProbe

class CassandraRealTimeIndexSpec extends WordSpec with Matchers with ScalaFutures with Eventually with SharedActorSystem {
  "CassandraRealTimeIndex" when {

    val                 noon = Instant.ofEpochSecond(1420113600) // Thu, 01 Jan 2015 12:00:00 GMT
    val secondBeforeMidnight = Instant.ofEpochSecond(1420156799) // Thu, 01 Jan 2015 23:59:59 GMT
    val      startOfTomorrow = Instant.ofEpochSecond(1420156800) // Thu, 01 Jan 2015 00:00:00 GMT

    def mkIndexEntry(windowStart: Instant, persistenceId: String) =
      IndexEntry(CassandraRealTimeIndex.toYearMonthDay(windowStart), windowStart, persistenceId, 0, 0)

    class Fixture(
      val initialContent: Set[IndexEntry] = Set.empty,
      val initialTime: Instant = noon
    ) {
    	val extTimeWindow = Duration.ofSeconds(120)

    	val emitted = TestProbe()

    	@volatile var now = initialTime

    	val cassandraOps = mock(classOf[CassandraOps])
    	when(cassandraOps.readIndexEntriesSince(initialTime minus extTimeWindow)).thenReturn(Source(initialContent.toList))

			val publisher = Source.actorPublisher(Props(
			  new CassandraRealTimeIndex(cassandraOps, pollDelay = 1.milliseconds, nowFunc = now, extendedTimeWindowLength = extTimeWindow)
	    )).runWith(Sink.actorRef(emitted.ref, "done"))

	    // Allow the publisher to pick up the initialContent (which it'll do shortly after having queried for initialTime)
      eventually {
        verify(cassandraOps, atLeastOnce).readIndexEntriesSince(initialTime minus extTimeWindow)
      }
    }

    "polling an empty database" should {
      "never emit any entries" in new Fixture {
        emitted.expectNoMsg(50.milliseconds)
      }
    }

    "polling an non-empty, but not changing database with recent items" should {
      "never emit any entries" in new Fixture (
        initialContent = Set(mkIndexEntry(noon minusMillis 10, "foo"))
      ){
        val soon = now plusMillis 50
        when(cassandraOps.readIndexEntriesSince(soon minus extTimeWindow)).thenReturn(Source(initialContent.toList))
        now = soon
        emitted.expectNoMsg(50.milliseconds)
      }
    }

    "discovering new entries between poll runs" should {
      "emit the newly found entries" in new Fixture {
        val soon = now plusMillis 50
        val entry = mkIndexEntry(soon, "foo")
        when(cassandraOps.readIndexEntriesSince(soon minus extTimeWindow)).thenReturn(Source.single(entry))
        now = soon
        emitted.expectMsg(entry)
      }
    }

    "discovering new entries having started with a non-empty database" should {
      "emit the newly found entries" in new Fixture (
        initialContent = Set(mkIndexEntry(noon minusMillis 10, "foo"))
      ){
        val soon = now plusMillis 50
        val entry = mkIndexEntry(soon, "bar")
        when(cassandraOps.readIndexEntriesSince(soon minus extTimeWindow)).thenReturn(Source(initialContent.toList :+ entry))

        now = soon
        emitted.expectMsg(entry)
      }
    }

    "crossing a date boundary where time windows both in the previous and next date continue to emit entries" should {
      "emit those entries until it considers the time windows closed" in new Fixture(
        initialTime = secondBeforeMidnight
      ) {
        val soon = now plusMillis 10 // still before midnight
        val todaysEntry = mkIndexEntry(soon, "today")
        when(cassandraOps.readIndexEntriesSince(soon minus extTimeWindow)).thenReturn(Source.single(todaysEntry))
        now = soon
        emitted.expectMsg(todaysEntry)

        val tomorrow = now plusSeconds 10 // now crossed the date boundary
        val tomorrowsEntry = mkIndexEntry(tomorrow, "tomorrow")
        when(cassandraOps.readIndexEntriesSince(tomorrow minus extTimeWindow)).thenReturn(Source.single(todaysEntry))
        when(cassandraOps.readIndexEntriesSince(startOfTomorrow)).thenReturn(Source.single(tomorrowsEntry))
        now = tomorrow
        emitted.expectMsg(tomorrowsEntry)
      }
    }
  }
}
