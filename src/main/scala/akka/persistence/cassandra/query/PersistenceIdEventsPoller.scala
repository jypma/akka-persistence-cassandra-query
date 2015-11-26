package akka.persistence.cassandra.query

import java.time.{ Duration, Instant }
import scala.concurrent.duration.{ DurationInt, FiniteDuration }
import akka.persistence.query.EventEnvelope
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import PersistenceIdEventsPoller._
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Terminated
import akka.actor.ActorLogging
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.actor.Cancellable
import akka.ConfigurationException

/**
 * Actor which publishes messages as they become visible in cassandra, for a specific persistenceId.
 */
class PersistenceIdEventsPoller(
    cassandraOps: CassandraOps,
    persistenceId: String,
    // longest time window ever stored + allowed clock drift
    extendedTimeWindowLength:Duration,
    pollDelay: FiniteDuration = 5.seconds,
    nowFunc: => Instant = Instant.now,
    // Maximum queue size should be the amount of memory we want to spend on slow real-time consumers.
    // Remember this is PER concurrently accessed persistenceId.
    maximumQueueSize: Int = 100
)(implicit m:Materializer) extends Actor with ActorLogging {
  import context.dispatcher

  log.debug("Starting poller for {}", persistenceId)
  
  assert(pollDelay.toMillis < extendedTimeWindowLength.toMillis, "poll interval should be (much) less than extended window length")

  val queue = collection.mutable.Queue.empty[EventEnvelope]
  var subscriptions = Set.empty[ActorRef]

  def receive = { case _ => }
  
  try {
    DistributedPubSub(context.system).mediator ! DistributedPubSubMediator.Subscribe(s"persistenceId:$persistenceId", self)
  } catch {
    case x:ConfigurationException => 
      // ignore. If the Cluster extension isn't available, we simply don't listen to topic messages.
  }
  
  become(polling(cassandraOps.findHighestSequenceNr(persistenceId), None))

  def polling(lastSeenSeqNr: Long, lastSeenOffset: Option[Long]): Receive = {
    if (lastSeenOffset.map(Instant.ofEpochMilli).exists(_ isBefore nowFunc.minus(extendedTimeWindowLength))) {
      log.info("Stopping, since last seen offset {} is too long ago.", lastSeenOffset)
      // last seen offset is before (now - time window length), so we stop ourselves
      context.stop(self)
      idle
    } else {
      query(lastSeenSeqNr, lastSeenOffset)
    }
  }

  val idle: Receive = { case _ => }

  def query(lastSeenSeqNr: Long, lastSeenOffset: Option[Long]): Receive = {
    log.debug("Querying from seqnr {}, last seen offset {}", lastSeenSeqNr, lastSeenOffset)
    cassandraOps.readEvents(persistenceId)(lastSeenSeqNr + 1, Long.MaxValue).runWith(Sink.actorRef(self, CassandraDone))
    var highestSeenSeqNr = lastSeenSeqNr
    var highestSeenOffset = lastSeenOffset
    var immediateRepoll = false

    {
      case event:EventEnvelope =>
        if (event.sequenceNr < highestSeenSeqNr) {
          throw new IllegalStateException(
              s"${persistenceId}: Cassandra delivered decreasing sequence nr ${event.sequenceNr} after already having seen $highestSeenSeqNr")
        }
        for (ofs <- highestSeenOffset) if (event.offset < ofs) {
          throw new IllegalStateException(
              s"${persistenceId}: Cassandra delivered decreasing time window offset ${event.offset} for sequence nr ${event.sequenceNr}," +
              s"after already having seen offset ${ofs} for sequence nr $highestSeenSeqNr")
        }
        if (queue.size >= maximumQueueSize) {
          throw new IllegalStateException(
              s"${persistenceId}: Exceeding maximum queue size of ${maximumQueueSize}")
        } else {
          queue += event
          highestSeenSeqNr = event.sequenceNr
          highestSeenOffset = Some(event.offset)
          log.debug("Seen event #{} at {} ({} subscribers)", event.sequenceNr, event.offset, subscriptions.size) 
          deliverQueue()
        }

      case CassandraDone =>
        log.debug("Cassandra query complete.")
        if (immediateRepoll) {
          become(polling(highestSeenSeqNr, highestSeenOffset))
        } else {
          val scheduledRepoll = context.system.scheduler.scheduleOnce(pollDelay, self, Repoll)
          become(queryComplete(highestSeenSeqNr, highestSeenOffset, scheduledRepoll))          
        }

      // This is the string posted to the persistenceId:XXX topic on DistributedPubSub
      case s:String if s.startsWith("added") =>
        immediateRepoll = true
    }
  }
  
  def queryComplete(lastSeenSeqNr: Long, lastSeenOffset: Option[Long], scheduledRepoll: Cancellable): Receive = {
    // This is the string posted to the persistenceId:XXX topic on DistributedPubSub
    case s:String if s.startsWith("added") =>
      scheduledRepoll.cancel()
      become(polling(lastSeenSeqNr, lastSeenOffset))
        
    case Repoll =>
      become(polling(lastSeenSeqNr, lastSeenOffset))
  } 
  
  def handleSubscriptions: Receive = {
    case Subscribe =>
      context.watch(sender)
      subscriptions += sender
      log.debug("Added subscription from {}, now at {}", sender, subscriptions.size)

    case Terminated(actor) =>
      subscriptions -= actor    
      log.debug("Removed subscription from {}, now at {}", actor, subscriptions.size)
  }
  
  def become(handler: Receive): Unit = {
    context.become(handler.orElse(handleSubscriptions))
  }

  def deliverQueue() {
    while (!queue.isEmpty) {
      val item = queue.dequeue()
      for (s <- subscriptions) {
        s ! item
      }
    }
  }
}

object PersistenceIdEventsPoller {
  case object Subscribe

  private case object CassandraDone
  private case object Repoll
}
