package akka.persistence.cassandra.query

import java.time.{ Duration, Instant }
import java.util.{ Calendar, TimeZone }
import scala.collection.immutable.TreeSet
import scala.concurrent.duration.{ DurationInt, FiniteDuration }
import akka.persistence.cassandra.query.CassandraOps.IndexEntry
import akka.stream.Materializer
import akka.stream.scaladsl.{ Keep, Sink, Source }
import IndexEntryPollerWorker._
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Actor
import akka.actor.Terminated
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.ConfigurationException
import akka.actor.Cancellable
import akka.actor.Status.Failure

/**
 * Actual worker implementation for IndexEntryPoller.
 */
class IndexEntryPollerWorker(
    cassandraOps: CassandraOps,

    // longest time window ever stored + allowed clock drift
    extendedTimeWindowLength:Duration,

    pollDelay:FiniteDuration = 5.seconds,

    // maximum queue size should be about 10 * (expected number of index entries during [extendedTimeWindowLength])
    maximumQueueSize: Int = 100000,
    
    nowFunc: => Instant = Instant.now
  )(implicit m:Materializer) extends Actor with ActorLogging {
  import context.dispatcher

  val targets = collection.mutable.Set.empty[ActorRef]
  val queue = collection.mutable.Queue.empty[IndexEntry]

  def allTimeWindowsClosed = nowFunc minus extendedTimeWindowLength

  def receive = { case _ => }
  
  try {
    DistributedPubSub(context.system).mediator ! DistributedPubSubMediator.Subscribe(s"persistenceIndex", self)
  } catch {
    case x:ConfigurationException => 
      // ignore. If the Cluster extension isn't available, we simply don't listen to topic messages.
  }
  
  become(querying(start = allTimeWindowsClosed, previousEvents = None))

  def becomeQuerying(entries:TreeSet[IndexEntry]): Unit = {
    val threshold = allTimeWindowsClosed
    become(querying(threshold, Some(entriesToRemember(threshold, entries))))    
  }
  
  def querying(start: Instant, previousEvents: Option[Set[IndexEntry]]): Receive = {
    log.debug("Polling from {}", start);
    var entries = TreeSet.empty[IndexEntry](Ordering.by { i => (i.window_start, i.persistenceId) })
    var immediateRepoll = false
    concatOpt(
      cassandraOps.readIndexEntriesOnSameDaySince(start),
      nextDayWithinExtendedTimeWindow(start).map { nextDay =>
        cassandraOps.readIndexEntriesOnSameDaySince(nextDay)
      }
    ).runWith(Sink.actorRef(self, CassandraDone))

    {
      case Failure(cause) =>
        log.warning("Poll query failed: {}. Re-polling in {}.", cause, pollDelay)
        val scheduledRepoll = context.system.scheduler.scheduleOnce(pollDelay, self, Repoll)
        become(sleeping(entries, scheduledRepoll))
      
      case entry:IndexEntry =>
        log.debug("Seen {}", entry);
        entries += entry

      case CassandraDone =>
        for (previous <- previousEvents) {
          val newItems = entries.diff(previous)
          if (queue.size + newItems.size > maximumQueueSize) {
            throw new IllegalStateException(
                s"Attempting to add ${newItems.size} items to current queue of size" +
                s"${queue.size}, which would exceed the maximum of ${maximumQueueSize}")
          } else {
        	  queue ++= newItems
        	  deliverQueue()
          }
        }
        if (immediateRepoll) {
          becomeQuerying(entries)
        } else {
          val scheduledRepoll = context.system.scheduler.scheduleOnce(pollDelay, self, Repoll)
          become(sleeping(entries, scheduledRepoll))          
        }

      // This is the message posted to the DistributedPubSub topic
      case s:String if s.startsWith("added") =>
        log.debug("Seen added during query!")
        immediateRepoll = true
    }
  }
  
  def sleeping(entries:TreeSet[IndexEntry], scheduledRepoll: Cancellable): Receive = {
    case Repoll =>
      becomeQuerying(entries)

    // This is the message posted to the DistributedPubSub topic
    case s:String if s.startsWith("added") =>
      log.debug("Seen added during sleep!")
      scheduledRepoll.cancel()
      becomeQuerying(entries)
  }

  def handleSubscriptions: Receive = {
    case IndexEntryPoller.Subscribe =>
      log.debug("Subscribed {}", sender)
      context.watch(sender)
      targets += sender

    case Terminated(actor) =>
      log.debug("Lost {}", sender)
      targets -= actor    
  }
  
  def become(handler: Receive): Unit = context.become(handler.orElse(handleSubscriptions))
  
  /**
   * Returns the entries from [candidates] that are after [threshold]
   */
  def entriesToRemember(threshold: Instant, candidates: TreeSet[IndexEntry]): Set[IndexEntry] = {
    candidates.from(IndexEntry(window_start = threshold, persistenceId = "",
        // the subsequent fields are not used for sorting, but are required to instantiate an IndexEntry
        window_length = 0, yearMonthDay = 0, firstSequenceNrInWindow = 0, partitionNr = 0))
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
    log.debug("deliverQueue of size {} to {} subscribers", queue.size, targets.size)
    while (!queue.isEmpty) {
      val item = queue.dequeue()
      log.debug("Delivering {}", item)
      for (t <- targets) t ! item
    }
  }
}

object IndexEntryPollerWorker {
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
