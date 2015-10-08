package akka.persistence.cassandra.query

import java.time.{ Duration, Instant }
import java.util.{ Calendar, TimeZone }

import scala.collection.immutable.TreeSet
import scala.concurrent.duration.{ DurationInt, FiniteDuration }

import akka.persistence.cassandra.query.CassandraOps.IndexEntry
import akka.stream.Materializer
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{ Cancel, Request }
import akka.stream.scaladsl.{ Keep, Sink, Source }

import IndexEntryPoller._

/**
 * ActorPublisher which publishes index entries as they become visible in cassandra.
 */
class IndexEntryPoller(
    cassandraOps: CassandraOps,

    // longest time window ever stored + allowed clock drift
    extendedTimeWindowLength:Duration = Duration.ofSeconds(120),

    pollDelay:FiniteDuration = 5.seconds,

    nowFunc: => Instant = Instant.now,

    // maximum queue size should be about 10 * (expected number of index entries during [extendedTimeWindowLength])
    maximumQueueSize: Int = 100000
  )(implicit m:Materializer) extends ActorPublisher[IndexEntry] {
  import context.dispatcher

  val queue = collection.mutable.Queue.empty[IndexEntry]

  def allTimeWindowsClosed = nowFunc minus extendedTimeWindowLength

  def receive = { case _ => }
  context.become(polling(start = allTimeWindowsClosed, previousEvents = None))

  def polling(start: Instant, previousEvents: Option[Set[IndexEntry]]): Receive = {
    var entries = TreeSet.empty[IndexEntry](Ordering.by { i => (i.window_start, i.persistenceId) })
    concatOpt(
      cassandraOps.readIndexEntriesSince(start),
      nextDayWithinExtendedTimeWindow(start).map { nextDay =>
        cassandraOps.readIndexEntriesSince(nextDay)
      }
    ).runWith(Sink.actorRef(self, CassandraDone))

    {
      case entry:IndexEntry =>
        entries += entry

      case CassandraDone =>
        for (previous <- previousEvents) {
          val newItems = entries.diff(previous)
          if (queue.size + newItems.size > maximumQueueSize) {
            onErrorThenStop(new IllegalStateException(
                s"Attempting to add ${newItems.size} items to current queue of size" +
                s"${queue.size}, which would exceed the maximum of ${maximumQueueSize}"))
          } else {
        	  queue ++= newItems
        	  deliverQueue()
          }
        }
        context.system.scheduler.scheduleOnce(pollDelay, self, Repoll)

      case Repoll =>
        val threshold = allTimeWindowsClosed
        context become polling(threshold, Some(entriesToRemember(threshold, entries)))

      case Request(_) =>
        deliverQueue()

      case Cancel =>
        onCompleteThenStop()
    }
  }

  /**
   * Returns the entries from [candidates] that are after [threshold]
   */
  def entriesToRemember(threshold: Instant, candidates: TreeSet[IndexEntry]): Set[IndexEntry] = {
    candidates.from(IndexEntry(window_start = threshold, persistenceId = "",
        // the subsequent fields are not used for sorting, but are required to instantiate an IndexEntry
        yearMonthDay = 0, firstSequenceNrInWindow = 0, partitionNr = 0))
  }

  /**
   * Returns an Some(Instant) with 0:00 the next day, IF [time] is [extendedTimeWindowLength] before
   * the end of its day (all measured in UTC). Returns None otherwise.
   */
  def nextDayWithinExtendedTimeWindow(time: Instant): Option[Instant] = {
	  import Calendar._

    val cal = Calendar.getInstance
    cal.setTimeZone(TimeZone.getTimeZone("UTC"))
    cal.setTimeInMillis(time.toEpochMilli())
    cal.add(DAY_OF_MONTH, 1)
    cal.set(HOUR, 0)
    cal.set(AM_PM, 0)
    cal.set(MINUTE, 0)
    cal.set(SECOND, 0)
    cal.set(MILLISECOND, 0)
    val nextDay = Instant.ofEpochMilli(cal.getTimeInMillis)

    if (time plus extendedTimeWindowLength isAfter nextDay) Some(nextDay) else None
  }

  def deliverQueue() {
    while (isActive && totalDemand > 0 && !queue.isEmpty) {
      onNext(queue.dequeue())
    }
  }
}

object IndexEntryPoller {
  private case object CassandraDone
  private case object Repoll

  def toYearMonthDay(instant: Instant): Int = {
	  import Calendar._

    val cal = Calendar.getInstance
    cal.setTimeZone(TimeZone.getTimeZone("UTC"))
    cal.setTimeInMillis(instant.toEpochMilli())
    cal.get(YEAR) * 10000 + cal.get(MONTH) * 100 + cal.get(DAY_OF_MONTH)
  }

  def concatOpt[T,M1,M2](s1: Source[T,M1], s2:Option[Source[T,M2]]): Source[T,M1] = {
    if (s2.isEmpty) s1 else s1.concatMat(s2.get)(Keep.left)
  }
}
