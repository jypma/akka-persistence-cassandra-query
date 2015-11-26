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
import scala.concurrent.duration.DurationInt
import akka.stream.actor.ActorSubscriberMessage.OnError

/**
 * Fans out input elements to nested streams using a provided function. In addition, it preserves
 * some ordering for chunks of input elements that have the same "key".
 * 
 * During the same key, it will allow all nested sources to arbitrarily contribute
 * elements to downstream. As soon as an input element arrives that yields a new key, that input element
 * will be blocked until all current nested sources complete, after which the stream resumes
 * with the new key.
 * 
 * Consider replacing this with https://github.com/akka/akka/pull/19006 once merged.
 */
object FanoutAndMerge {
  def apply[In,Key,Out](getKey: In => Key, getSource: In => Source[Out,Any])(implicit system:ActorSystem, m:Materializer): (Sink[In,Unit], Source[Out,Unit]) = {
    val mergeOutProxy = system.actorOf(Props(classOf[MergeOutProxy]))

    val mergeOut = Source.actorPublisher(Props[MergeOutActor[Out]]).mapMaterializedValue {
      actor => mergeOutProxy ! actor
    }

    (Sink.actorSubscriber(Props(new FanoutActor(getKey,getSource,mergeOutProxy))).named("fanout").mapMaterializedValue { x => }, mergeOut.named("merge"))
  }
  /* Internals:
                             ~~~>   getSource(T)      ------>
                             ~~~>        |            ------>
                             ~~~>        v            ------>
  [sink] -----> FanoutActor  ~~~>    MergeInActor     ------>    MergeOutProxy  ---->   MergeOutActor  --> [source]   
                   |         ~~~>  (ActorSubscriber)  ------>         ^               (ActorPublisher)
                   |                                                  |
                   `--------------------------Register----------------'  
   */ 

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

  /**
   * Sent from FanoutActor to MergeOutActor, so the latter can conclude the stream
   * has completed when FanoutActor has stopped.
   */
  case class Register(owner: ActorRef)

  class MergeOutActor[T] extends ActorPublisher[T] {
    val buffer = collection.mutable.Queue.empty[(T,ActorRef)]
    val knownSenders = collection.mutable.Set.empty[ActorRef]
    var owner: ActorRef = null
    var completed: Boolean = false

    def receive = {
      case Register(someOwner) =>
        owner = someOwner
        context.watch(owner)

      case requestedMore:Request =>
        sendBuffer()

      case Cancel =>
        onCompleteThenStop()

      case Terminated(actor) if actor == owner =>
        completed = true
        stopIfDone()

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
      if (buffer.isEmpty && knownSenders.isEmpty && completed) {
        onCompleteThenStop()
      }
    }
  }

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
        context.stop(self)
        
      case OnError(cause: Throwable) =>
        // FIXME: We need to signal upstream that this was an error, rather than end-of-stream
        log.error("Oh, no, our stream has errored. Stopping.", cause)
        context.stop(self)
    }
  }

  class FanoutActor[In,Key,Out](
      getKey: In => Key,
      getSource: In => Source[Out,Any],
      out: ActorRef)
      (implicit m:Materializer) extends ActorSubscriber with ActorLogging {

	  val mergeIn = Sink.actorSubscriber(Props(classOf[MergeInActor], self, out)).named("mergeIn")

    var currentKey: Option[Key] = None
	  val inProgress = collection.mutable.Set.empty[ActorRef]
	  val queue = collection.mutable.Queue.empty[In]
	  var completed: Boolean = false

	  out ! Register(self)

    // TODO We keep this maximum number of open sub-streams or queued items, before blocking upstream. Make this configurable.
    override def requestStrategy = new MaxInFlightRequestStrategy(100) {
      override def inFlightInternally = inProgress.size + queue.size
    }

    def receive = {
      case OnNext(elem) =>
        val in = elem.asInstanceOf[In]
        val key = getKey(in)
        if (currentKey.isEmpty || (currentKey.get == key)) {
          // OK to start this substream
          startStream(in)
        } else {
          // queue the element and wait for current substreams to complete
          queue.enqueue(in)
        }

      case Terminated(actor) =>
        inProgress -= actor
        stopOrSendQueue()

      case OnComplete =>
        completed = true
        stopOrSendQueue()
        
      case OnError(cause: Throwable) =>
        // FIXME: We need to signal upstream that this was an error, rather than end-of-stream
        log.error("Oh, no, our stream has errored. Stopping.", cause)
        context.stop(self)
    }

    def startStream(in: In) {
      currentKey = Some(getKey(in))
      val actor = getSource(in).runWith(mergeIn)
      context.watch(actor)
      inProgress += actor      
    }
    
    def stopOrSendQueue() {
      if (inProgress.isEmpty) {
        currentKey = None
        
        if (!queue.isEmpty) {
          // Nothing in progress, so start reading from queue for next key.
          // The queue will never have more than MaxInflight items, because of our requestStrategy.
          // Hence, we can just start all sub-streams as long as they have the same key.
          startStream(queue.dequeue())
          while (queue.headOption.filter(in => getKey(in) == currentKey.get).isDefined) {
            startStream(queue.dequeue())
          }
        } else if (completed) {
          context.stop(self)          
        }
      }
    }
  }
}
