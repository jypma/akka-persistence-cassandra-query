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
    val mergeIn = Sink.actorSubscriber(Props(classOf[MergeInActor], mergeOutProxy))

    (Flow[In].transform(() => new PushStage[In,Nothing] {
      val inProgress = collection.mutable.Map.empty[Key, ActorRef]

      override def onPush(in: In, ctx:Context[Nothing]) = {
        val result = inProgress.getOrElseUpdate(getKey(in), {
          try {
          getSource(in).runWith(mergeIn)
          } catch {
            case x:Throwable =>
              x.printStackTrace()
              throw x
          }
        })
        ctx.pull()
      }
    }).to(Sink.foreach[Nothing](println)), mergeOut)
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
        actor ! Landed(count)
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

  class MergeInActor(outActor: ActorRef) extends ActorSubscriber with ActorLogging {
    var inFlight:Int = 0

    //TODO consider ZeroRequestStrategy instead, having the MergeOutActor broadcasting requests evenly?
    //although that might not work with many many input actors.
    override def requestStrategy = new MaxInFlightRequestStrategy(50) {
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
        context.stop(self)
    }
  }


}
