package akka.persistence.cassandra.query

import scala.concurrent.duration.{ DurationInt, FiniteDuration }

import CassandraRealTimeEvents.{ CassandraDone, Repoll }
import akka.persistence.query.EventEnvelope
import akka.stream.Materializer
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{ Cancel, Request }
import akka.stream.scaladsl.Sink

class CassandraRealTimeEvents(
    cassandraOps: CassandraOps,
    persistenceId: String,
    pollDelay: FiniteDuration = 5.seconds
)(implicit m:Materializer) extends ActorPublisher[EventEnvelope] {
  import context.dispatcher

  // TODO limit queue size
  val queue = collection.mutable.Queue.empty[EventEnvelope]

  def receive = { case _ => }
  context become polling(cassandraOps.findHighestSequenceNr(persistenceId))

  def polling(lastSeenSeqNr: Long): Receive = {
    cassandraOps.readEvents(persistenceId, lastSeenSeqNr + 1, Long.MaxValue).runWith(Sink.actorRef(self, CassandraDone))
    var highestSeenSeqNr = lastSeenSeqNr

    {
      case event:EventEnvelope =>
        if (event.sequenceNr < highestSeenSeqNr) {
          onErrorThenStop(new IllegalStateException(
              s"Cassandra delivered decreasing sequence nr ${event.sequenceNr} after already having seen $highestSeenSeqNr"))
        }
        queue += event
        highestSeenSeqNr = event.sequenceNr
        deliverQueue()

      case CassandraDone =>
        context.system.scheduler.scheduleOnce(pollDelay, self, Repoll)

      case Repoll =>
        context become polling(highestSeenSeqNr)

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

object CassandraRealTimeEvents {
  private case object CassandraDone
  private case object Repoll
}
