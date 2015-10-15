package akka.persistence.cassandra.query

import akka.actor.Actor
import akka.stream.scaladsl.Source
import scala.concurrent.Future
import org.reactivestreams.Publisher
import akka.actor.ActorSystem
import akka.actor.Props
import akka.pattern.ask
import SourcePool._
import akka.util.Timeout
import scala.concurrent.duration._
import akka.stream.scaladsl.FlattenStrategy
import akka.persistence.cassandra.streams.WhenComplete
import akka.persistence.cassandra.streams.Reaper
import akka.actor.ActorRef
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Keep
import akka.stream.Materializer
import akka.actor.Terminated
import akka.stream.OverflowStrategy

/**
 * Manages a pool of Source[T] instances, managed by key, backed by actors. The actor
 * implementation must react to the [subscribeMessage] by starting to send its events
 * to the sender of [subscribeMessage].
 *
 * Internally, each materialization is a Source.actorRef with the given [bufferSize] and OverflowStrategy.fail.
 *
 * @param factory Should return a Props for an Actor that emits messages of type T.
 */
class SourcePool[T,K](factory: K => Props, subscribeMessage: Any, bufferSize: Int)(implicit system: ActorSystem, m:Materializer) {
  private val manager = system.actorOf(Props(new ManagerActor()))

  private implicit val timeout = Timeout(1.second)

  def apply(key: K): Source[T,Unit] = {
    Source.actorRef(bufferSize, OverflowStrategy.fail).mapMaterializedValue { actor =>
      manager.tell(AddSubscription(key), actor)
    }
  }

  def shutdown(): Future[Unit] = {
    system.stop(manager)
    Reaper(manager)
  }

  private class ManagerActor extends Actor {
    val running = collection.mutable.Map.empty[K,ActorRef]
    val keyForActor = collection.mutable.Map.empty[ActorRef,K]

    def receive = {
      case AddSubscription(key) =>
        val worker = running.getOrElseUpdate(key, {
          val actor = context.actorOf(factory(key), key.toString)
          context.watch(actor)
          keyForActor(actor) = key
          actor
        })
        worker.tell(subscribeMessage, sender)

      case Terminated(actor) =>
        running.remove(keyForActor(actor))
    }
  }

  private case class AddSubscription(key: K)
}

object SourcePool {
  def apply[T,K](subscribeMessage: Any, bufferSize: Int)(factory: K => Props)
                (implicit system: ActorSystem, m:Materializer): SourcePool[T,K] =
    new SourcePool(factory, subscribeMessage, bufferSize)
}
