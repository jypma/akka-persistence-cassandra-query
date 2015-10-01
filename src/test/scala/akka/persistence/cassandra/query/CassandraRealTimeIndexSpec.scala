package akka.persistence.cassandra.query

import org.scalatest.concurrent.ScalaFutures
import java.time.Duration
import org.scalatest.Matchers
import org.scalatest.WordSpec
import akka.persistence.cassandra.test.SharedActorSystem
import akka.stream.scaladsl.Source
import akka.actor.Props
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.mockito.Matchers.{eq => is}
import akka.persistence.cassandra.journal.CassandraJournalConfig
import akka.persistence.cassandra.test.MockCassandra
import akka.persistence.cassandra.Cassandra
import akka.persistence.cassandra.Cassandra.RowMapper
import akka.persistence.cassandra.Cassandra.PreparedSelectStatement
import java.time.Instant
import akka.stream.scaladsl.Sink
import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.duration._
import org.scalatest.concurrent.Eventually
import akka.persistence.cassandra.query.CassandraOps.IndexEntry

class CassandraRealTimeIndexSpec extends WordSpec with Matchers with ScalaFutures with Eventually with SharedActorSystem with MockCassandra {
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

    	val emitted = new ConcurrentLinkedQueue[IndexEntry]

    	@volatile var now = initialTime

    	val cassandraOps = mock(classOf[CassandraOps])
    	when(cassandraOps.readIndexEntriesSince(initialTime minus extTimeWindow)).thenReturn(Source(initialContent.toList))

			val publisher = Source.actorPublisher(Props(
			  new CassandraRealTimeIndex(cassandraOps, pollDelay = 1.milliseconds, nowFunc = now, extendedTimeWindowLength = extTimeWindow)
	    )).runWith(Sink.foreach(emitted.add))
    }

    "polling an empty database" should {
      "never emit any entries" in new Fixture {
        Thread.sleep(50) // allow the actor to poll a few times
        emitted should have size(0)
      }
    }

    "polling an non-empty, but not changing database with recent items" should {
      "never emit any entries" in new Fixture (
        initialContent = Set(mkIndexEntry(noon minusMillis 10, "foo"))
      ){
        Thread.sleep(50)
        val soon = now plusMillis 50
        when(cassandraOps.readIndexEntriesSince(soon minus extTimeWindow)).thenReturn(Source(initialContent.toList))
        now = soon
        Thread.sleep(50)
        emitted should have size(0)
      }
    }

    "discovering new entries between poll runs" should {
      "emit the newly found entries" in new Fixture {
        Thread.sleep(50) // allow for the actor to pick up the initial, empty database
        val soon = now plusMillis 50
        val entry = mkIndexEntry(soon, "foo")
        when(cassandraOps.readIndexEntriesSince(soon minus extTimeWindow)).thenReturn(Source.single(entry))
        now = soon
        eventually {
          emitted should contain(entry)
        }
      }
    }

    "discovering new entries having started with a non-empty database" should {
      "emit the newly found entries" in new Fixture (
        initialContent = Set(mkIndexEntry(noon minusMillis 10, "foo"))
      ){
        Thread.sleep(50) // allow for the actor to pick up the initial, empty database
        val soon = now plusMillis 50
        val entry = mkIndexEntry(soon, "bar")
        when(cassandraOps.readIndexEntriesSince(soon minus extTimeWindow)).thenReturn(Source(initialContent.toList :+ entry))
        now = soon
        eventually {
          emitted should have size (1)
          emitted should contain(entry)
        }
      }
    }

    "crossing a date boundary where time windows both in the previous and next date continue to emit entries" should {
      "emit those entries until it considers the time windows closed" in new Fixture(
        initialTime = secondBeforeMidnight
      ) {
        Thread.sleep(50) // allow for the actor to pick up the initial, empty database
        val soon = now plusMillis 10 // still before midnight
        val todaysEntry = mkIndexEntry(soon, "today")
        when(cassandraOps.readIndexEntriesSince(soon minus extTimeWindow)).thenReturn(Source.single(todaysEntry))
        now = soon
        eventually {
          emitted should have size(1)
        }

        val tomorrow = now plusSeconds 10 // now crossed the date boundary
        when(cassandraOps.readIndexEntriesSince(tomorrow minus extTimeWindow)).thenReturn(Source.single(todaysEntry))
        when(cassandraOps.readIndexEntriesSince(startOfTomorrow)).thenReturn(Source.single(mkIndexEntry(tomorrow, "tomorrow")))
        now = tomorrow
        eventually {
          emitted should have size(2)
        }
      }
    }
  }
}
