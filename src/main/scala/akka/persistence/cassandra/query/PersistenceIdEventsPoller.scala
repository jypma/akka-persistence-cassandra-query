package akka.persistence.cassandra.query

import java.time.{ Duration, Instant }

import scala.concurrent.duration.{ DurationInt, FiniteDuration }

import akka.persistence.query.EventEnvelope
import akka.stream.Materializer
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{ Cancel, Request }
import akka.stream.scaladsl.Sink

import PersistenceIdEventsPoller._

/**
 * ActorPublisher which publishes messages as they become visible in cassandra, for a specific persistenceId.
 */
class PersistenceIdEventsPoller(
    cassandraOps: CassandraOps,
    persistenceId: String,
    // longest time window ever stored + allowed clock drift
    extendedTimeWindowLength:Duration = Duration.ofSeconds(120),
    pollDelay: FiniteDuration = 5.seconds,
    nowFunc: => Instant = Instant.now,
    // Maximum queue size should be the amount of memory we want to spend on slow real-time consumers.
    // Remember this is PER concurrently accessed persistenceId.
    maximumQueueSize: Int = 100
)(implicit m:Materializer) extends ActorPublisher[EventEnvelope] {
  import context.dispatcher

  assert(pollDelay.toMillis < extendedTimeWindowLength.toMillis, "poll interval should be (much) less than extended window length")

  val queue = collection.mutable.Queue.empty[EventEnvelope]

  def receive = { case _ => }
  context become polling(cassandraOps.findHighestSequenceNr(persistenceId), None)

  def polling(lastSeenSeqNr: Long, lastSeenOffset: Option[Long]): Receive = {
    if (lastSeenOffset.map(Instant.ofEpochMilli).exists(_ isBefore nowFunc.minus(extendedTimeWindowLength))) {
      // last seen offset is before (now - time window length), so we stop ourselves
      context.stop(self)
      idle
    } else {
      poll (lastSeenSeqNr, lastSeenOffset)
    }
  }

  val idle: Receive = { case _ => }

  def poll(lastSeenSeqNr: Long, lastSeenOffset: Option[Long]): Receive = {
    cassandraOps.readEvents(persistenceId)(lastSeenSeqNr + 1, Long.MaxValue).runWith(Sink.actorRef(self, CassandraDone))
    var highestSeenSeqNr = lastSeenSeqNr
    var highestSeenOffset = lastSeenOffset

    {
      case event:EventEnvelope =>
        if (event.sequenceNr < highestSeenSeqNr) {
          onErrorThenStop(new IllegalStateException(
              s"${persistenceId}: Cassandra delivered decreasing sequence nr ${event.sequenceNr} after already having seen $highestSeenSeqNr"))
        }
        for (ofs <- highestSeenOffset) if (event.offset < ofs) {
          onErrorThenStop(new IllegalStateException(
              s"${persistenceId}: Cassandra delivered decreasing time window offset ${event.offset} for sequence nr ${event.sequenceNr}," +
              s"after already having seen offset ${ofs} for sequence nr $highestSeenSeqNr"))
        }
        if (queue.size >= maximumQueueSize) {
          onErrorThenStop(new IllegalStateException(
              s"${persistenceId}: Exceeding maximum queue size of ${maximumQueueSize}"))
        } else {
          queue += event
          highestSeenSeqNr = event.sequenceNr
          highestSeenOffset = Some(event.offset)
          deliverQueue()
        }

      case CassandraDone =>
        context.system.scheduler.scheduleOnce(pollDelay, self, Repoll)

      case Repoll =>
        context become polling(highestSeenSeqNr, highestSeenOffset)

      case Request(_) =>
        deliverQueue()

      case Cancel =>
        onCompleteThenStop()
    }
  }

  def deliverQueue() {
    while (isActive && totalDemand > 0 && !queue.isEmpty) {
      onNext(queue.dequeue())
    }
  }
}

object PersistenceIdEventsPoller {
  private case object CassandraDone
  private case object Repoll
}
