package akka.persistence.cassandra.query

import java.time.{ Duration, Instant }
import scala.concurrent.duration.DurationInt
import org.mockito.Mockito.{ atLeastOnce, mock, verify, when }
import org.scalatest.{ Finders, Matchers, WordSpec }
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.time.{ Seconds, Span }
import akka.actor.Props
import akka.persistence.cassandra.query.CassandraOps.IndexEntry
import akka.persistence.cassandra.test.SharedActorSystem
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.testkit.TestProbe
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Publish

class IndexEntryPollerWorkerSpec extends WordSpec with Matchers with ScalaFutures with Eventually with SharedActorSystem {
  implicit val patience = PatienceConfig(timeout = Span(10, Seconds))
  val pubsub = DistributedPubSub(system).mediator

  "IndexEntryPollerWorker" when {

    val                 noon = Instant.ofEpochSecond(1420113600) // Thu, 01 Jan 2015 12:00:00 GMT
    val secondBeforeMidnight = Instant.ofEpochSecond(1420156799) // Thu, 01 Jan 2015 23:59:59 GMT
    val      startOfTomorrow = Instant.ofEpochSecond(1420156800) // Thu, 01 Jan 2015 00:00:00 GMT

    def mkIndexEntry(windowStart: Instant, persistenceId: String) =
      IndexEntry(IndexEntryPollerWorker.toYearMonthDay(windowStart), windowStart, 60000, persistenceId, 0, 0)

    class Fixture(
      val initialContent: Set[IndexEntry] = Set.empty,
      val initialTime: Instant = noon
    ) {
    	val extTimeWindow = Duration.ofSeconds(120)

    	val emitted = TestProbe()

    	@volatile var now = initialTime

    	val cassandraOps = mock(classOf[CassandraOps])
    	when(cassandraOps.readIndexEntriesOnSameDaySince(initialTime minus extTimeWindow)).thenReturn(Source(initialContent.toList))

    	val poller = system.actorOf(Props(
			  new IndexEntryPollerWorker(cassandraOps, 
			    pollDelay = 200.milliseconds, 
			    nowFunc = now, 
			    extendedTimeWindowLength = extTimeWindow, 
			    maximumQueueSize = 10000)))
			emitted.send(poller, IndexEntryPoller.Subscribe)

	    // Allow the publisher to pick up the initialContent (which it'll do shortly after having queried for initialTime)
      eventually {
        verify(cassandraOps, atLeastOnce).readIndexEntriesOnSameDaySince(initialTime minus extTimeWindow)
      }

    	def cleanup () {
    	  system.stop(poller)
    	}
    }

    def fixture(initialContent: Set[IndexEntry] = Set.empty, initialTime: Instant = noon)(testcode: Fixture => Unit) = {
      val f = new Fixture(initialContent, initialTime)
      try testcode(f) finally f.cleanup()
    }

    "polling an empty database" should {
      "never emit any entries" in fixture() { f =>
        f.emitted.expectNoMsg(50.milliseconds)
      }
    }

    "polling an non-empty, but not changing database with recent items" should {
      "never emit any entries" in fixture (
        initialContent = Set(mkIndexEntry(noon minusMillis 10, "foo"))
      ){ f =>
        val soon = f.now plusMillis 50
        when(f.cassandraOps.readIndexEntriesOnSameDaySince(soon minus f.extTimeWindow)).thenReturn(Source(f.initialContent.toList))
        f.now = soon
        f.emitted.expectNoMsg(50.milliseconds)
      }
    }

    "discovering new entries between poll runs" should {
      "emit the newly found entries" in fixture() { f =>
        val soon = f.now plusMillis 50
        val entry = mkIndexEntry(soon, "foo")
        when(f.cassandraOps.readIndexEntriesOnSameDaySince(soon minus f.extTimeWindow)).thenReturn(Source.single(entry))
        f.now = soon
        f.emitted.expectMsg(entry)
      }
      
      "emit the newly found entries immediately after seeing a pubsub event" in fixture() { f =>
        val soon = f.now plusMillis 50
        val entry = mkIndexEntry(soon, "foo")
        when(f.cassandraOps.readIndexEntriesOnSameDaySince(soon minus f.extTimeWindow)).thenReturn(Source.single(entry))
        f.now = soon
        pubsub ! Publish("persistenceIndex", "added:XXX")
        f.emitted.within(100.milliseconds) {
          f.emitted.expectMsg(entry)          
        }
      }
    }

    "discovering new entries having started with a non-empty database" should {
      "emit the newly found entries" in fixture (
        initialContent = Set(mkIndexEntry(noon minusMillis 10, "foo"))
      ){ f =>
        val soon = f.now plusMillis 50
        val entry = mkIndexEntry(soon, "bar")
        when(f.cassandraOps.readIndexEntriesOnSameDaySince(soon minus f.extTimeWindow)).thenReturn(Source(f.initialContent.toList :+ entry))

        f.now = soon
        pubsub ! Publish("persistenceIndex", "added:XXX")
        f.emitted.expectMsg(entry)
      }
    }

    "crossing a date boundary where time windows both in the previous and next date continue to emit entries" should {
      "emit those entries until it considers the time windows closed" in fixture (
        initialTime = secondBeforeMidnight
      ) { f =>
        val soon = f.now plusMillis 10 // still before midnight
        val todaysEntry = mkIndexEntry(soon, "today")
        when(f.cassandraOps.readIndexEntriesOnSameDaySince(soon minus f.extTimeWindow)).thenReturn(Source.single(todaysEntry))
        f.now = soon
        pubsub ! Publish("persistenceIndex", "added:XXX")
        f.emitted.within(10.seconds) {
          f.emitted.expectMsg(todaysEntry)          
        }

        val tomorrow = f.now plusSeconds 10 // now crossed the date boundary
        val tomorrowsEntry = mkIndexEntry(tomorrow, "tomorrow")
        when(f.cassandraOps.readIndexEntriesOnSameDaySince(tomorrow minus f.extTimeWindow)).thenReturn(Source.single(todaysEntry))
        when(f.cassandraOps.readIndexEntriesOnSameDaySince(startOfTomorrow)).thenReturn(Source.single(tomorrowsEntry))
        f.now = tomorrow
        pubsub ! Publish("persistenceIndex", "added:YYY")
        f.emitted.within(10.seconds) {
          f.emitted.expectMsg(tomorrowsEntry)
        }
      }
    }
    
    "encountering a failure during polling" should {
      "re-try after the standard poll interval" in fixture() { f =>
        val x = new RuntimeException("Simulated failure")
        when(f.cassandraOps.readIndexEntriesOnSameDaySince(f.initialTime minus f.extTimeWindow)).thenReturn(Source.failed(x))
        Thread.sleep(400) // poll delay * 2
        
        val soon = f.now plusMillis 50
        val entry = mkIndexEntry(soon, "foo")
        when(f.cassandraOps.readIndexEntriesOnSameDaySince(soon minus f.extTimeWindow)).thenReturn(Source.single(entry))
        f.now = soon
        f.emitted.expectMsg(entry)
      }
    }
  }
}
