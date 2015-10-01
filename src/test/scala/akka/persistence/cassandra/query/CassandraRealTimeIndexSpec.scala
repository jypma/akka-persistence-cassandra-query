package akka.persistence.cassandra.query

import org.scalatest.concurrent.ScalaFutures
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
import akka.persistence.cassandra.query.CassandraReadJournal.IndexEntry
import akka.stream.scaladsl.Sink
import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.duration._
import org.scalatest.concurrent.Eventually

class CassandraRealTimeIndexSpec extends WordSpec with Matchers with ScalaFutures with Eventually with SharedActorSystem with MockCassandra {
  "CassandraRealTimeIndex" when {

    val                 noon = Instant.ofEpochSecond(1420113600) // Thu, 01 Jan 2015 12:00:00 GMT
    val secondBeforeMidnight = Instant.ofEpochSecond(1420156799) // Thu, 01 Jan 2015 23:59:59 GMT

    def mkIndexEntry(windowStart: Instant, persistenceId: String) =
      IndexEntry(CassandraRealTimeIndex.toYearMonthDay(windowStart), windowStart, persistenceId, 0, 0)

    class Fixture(
      initialContent: Set[IndexEntry] = Set.empty,
      initialTime: Instant = noon
    ) {
    	val config = mock(classOf[CassandraJournalConfig])
    	when(config.keyspace) thenReturn "keyspace"
    	when(config.timeIndexTable) thenReturn "timeIndexTable"

    	@volatile var databaseContent =
    	  collection.immutable.TreeSet.empty[IndexEntry](Ordering.by { i => (i.window_start, i.persistenceId) }) ++ initialContent

    	val emitted = new ConcurrentLinkedQueue[IndexEntry]

		  val cassandra = new Cassandra {
        override def prepareSelect[T: RowMapper](cql: String, fetchSize: Int = 0): PreparedSelectStatement[T] = cql match {
          case "SELECT * FROM table WHERE year_month_day = ? AND window_start >= ?" =>
            mockStatement {
              case (ymd: Int) :: (start: Instant) :: Nil =>
                // return the mocked content of the database, at day [ymd], starting from [start]
                val startEntry = IndexEntry(window_start = start, persistenceId = "", yearMonthDay = 0, firstSequenceNrInWindow = 0, partitionNr = 0)
                databaseContent.filter(_.yearMonthDay == ymd).from(startEntry).toSeq
            }
        }
    	}

    	@volatile var now = initialTime

			val publisher = Source.actorPublisher(Props(
			  new CassandraRealTimeIndex(cassandra, "table", pollDelay = 1.milliseconds, nowFunc = now)
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
        now = now plusMillis 50
        Thread.sleep(50)
        emitted should have size(0)
      }
    }

    "discovering new entries between poll runs" should {
      "emit the newly found entries" in new Fixture {
        Thread.sleep(50) // allow for the actor to pick up the initial, empty database
        now = now plusMillis 50
        val entry = IndexEntry(CassandraRealTimeIndex.toYearMonthDay(now), now, "foo", 0, 0)
        databaseContent += entry
        eventually {
          emitted should contain(entry)
        }
      }

      "not emit entries that are erroneously added to time windows that are considered too old" in new Fixture {
        Thread.sleep(50) // allow for the actor to poll
        val old = now minusSeconds 600 // more than extendedTimeWindowLength
        val entry = IndexEntry(CassandraRealTimeIndex.toYearMonthDay(old), old, "foo", 0, 0)
        databaseContent += entry
        Thread.sleep(50) // allow for the actor to poll
        emitted should have size (0)
      }
    }

    "discovering new entries having started with a non-empty database" should {
      "emit the newly found entries" in new Fixture (
        initialContent = Set(mkIndexEntry(noon minusMillis 10, "foo"))
      ){
        Thread.sleep(50) // allow for the actor to pick up the initial, empty database
        now = now plusMillis 50
        val entry = mkIndexEntry(now, "bar")
        databaseContent += entry
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
        now = now plusMillis 10 // still before midnight
        databaseContent += mkIndexEntry(now, "today")
        eventually {
          emitted should have size(1)
        }
        now = now plusSeconds 10 // now crossed the date boundary
        databaseContent += mkIndexEntry(now, "nextDay")
        println(databaseContent)
        eventually {
          emitted should have size(2)
        }
      }
    }
  }
}
