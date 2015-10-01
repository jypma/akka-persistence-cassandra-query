package akka.persistence.cassandra.streams

import akka.stream.scaladsl.Source
import akka.stream.scaladsl.Sink
import akka.stream.actor.ActorPublisher
import akka.actor.Props
import akka.stream.scaladsl.Flow
import akka.stream.stage.PushStage
import akka.stream.stage.Context
import akka.actor.ActorRef
import akka.stream.actor.ActorSubscriber
import akka.stream.Materializer
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor.ActorSubscriberMessage.OnComplete
import akka.stream.actor.MaxInFlightRequestStrategy
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.actor.ActorPublisherMessage.Cancel
import akka.actor.ActorLogging
import akka.actor.Terminated
import akka.actor.ActorSystem
import akka.actor.Stash
import akka.actor.Actor

object FanoutAndMerge {
  def apply[In,Key,Out](getKey: In => Key, getSource: In => Source[Out,Any])(implicit system:ActorSystem, m:Materializer): (Sink[In,Unit], Source[Out,Unit]) = {
    val mergeOutProxy = system.actorOf(Props(classOf[MergeOutProxy]))

    val mergeOut = Source.actorPublisher(Props[MergeOutActor[Out]]).mapMaterializedValue {
      actor => mergeOutProxy ! actor
    }

    (Sink.actorSubscriber(Props(new FanoutActor(getKey,getSource,mergeOutProxy))).mapMaterializedValue { x => }, mergeOut)
  }

  /**
   * Sent from MergeOutActor to MergeInActor whenever [count] messages have been passed along downstream.
   */
  case class Landed(count: Int)

  class MergeOutProxy extends Actor with Stash {
    def receive = {
      case delegate:ActorRef =>
        context become active(delegate)
        unstashAll()

      case other =>
        stash()
    }

    def active(delegate:ActorRef): Receive = {
      case msg =>
        delegate forward msg
    }
  }

  class MergeOutActor[T] extends ActorPublisher[T] {
    val buffer = collection.mutable.Queue.empty[(T,ActorRef)]
    val knownSenders = collection.mutable.Set.empty[ActorRef]

    def receive = {
      case requestedMore:Request =>
        sendBuffer()

      case Cancel =>
        onCompleteThenStop()

      case Terminated(actor) =>
        knownSenders -= actor
        stopIfDone()

      case elem =>
        if (!knownSenders(sender)) {
          knownSenders += sender
          context.watch(sender)
        }
        buffer.enqueue((elem.asInstanceOf[T], sender))
        sendBuffer()
    }

    private def sendBuffer() {
      val landed = collection.mutable.Map.empty[ActorRef,Int].withDefaultValue(0)

      while (isActive && totalDemand > 0 && !buffer.isEmpty) {
        val (elem, actor) = buffer.dequeue()
        onNext(elem)
        landed(actor) += 1
      }

      for ((actor, count) <- landed) {
        if (knownSenders.contains(actor)) {
        	actor ! Landed(count)
        }
      }

      if (landed.size > 0) {
        stopIfDone()
      }
    }

    private def stopIfDone() {
      if (buffer.isEmpty && knownSenders.isEmpty) {
        onCompleteThenStop()
      }
    }
  }

  case object RequestDeath

  class MergeInActor(owner: ActorRef, outActor: ActorRef) extends ActorSubscriber with ActorLogging {
    var inFlight:Int = 0

    //TODO externalize this number. For now, it's the same as akka's internal stage's maximum buffer size.
    override def requestStrategy = new MaxInFlightRequestStrategy(16) {
      override def inFlightInternally = inFlight
    }

    def receive = {
      case OnNext(elem) =>
        inFlight += 1
        outActor ! elem

      case Landed(count) =>
        if (count > inFlight) {
      	  log.warning("Landed {} messages while only {} in flight. Assuming all messages have landed.", count, inFlight)
      	  inFlight = 0
        } else {
      	  inFlight -= count
        }

      case OnComplete =>
        owner ! RequestDeath
    }
  }

  class FanoutActor[In,Key,Out](
      getKey: In => Key,
      getSource: In => Source[Out,Any],
      out: ActorRef)
      (implicit m:Materializer) extends ActorSubscriber {

	  val mergeIn = Sink.actorSubscriber(Props(classOf[MergeInActor], out))

    val inProgress = collection.mutable.Set.empty[Key]
	  val keyForActor = collection.mutable.Map.empty[ActorRef,Key]

    // TODO We keep this maximum number of open sub-streams, before blocking upstream. Make this configurable.
    override def requestStrategy = new MaxInFlightRequestStrategy(1000) {
      override def inFlightInternally = inProgress.size
    }

    def receive = {
      case OnNext(elem) =>
        val in = elem.asInstanceOf[In]
        val key = getKey(in)
        if (inProgress.add(key)) {
          val actor = getSource(in).runWith(mergeIn)
          context.watch(actor)
          keyForActor(actor) = key
        }

      case RequestDeath =>
        keyForActor.get(sender).foreach(inProgress.remove)
        context.stop(sender)

      case OnComplete =>
        context.stop(self)
    }
  }
}
