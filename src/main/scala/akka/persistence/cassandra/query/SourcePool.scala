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

/**
 * Manages a pool of Source[T] instances, managed by key. It does not preserve the materialized
 * value of the sources created by the [factory] argument.
 */
class SourcePool[T,K](factory: K => Source[T,Any])(implicit system: ActorSystem) {
  private val actor = system.actorOf(Props(new ManagerActor()))

  private implicit val timeout = Timeout(1.second)

  def apply(key: K): Source[T,Unit] = {
    Source((actor ask GetSource(key)).mapTo[Source[T,Any]]).flatten(FlattenStrategy.concat)
  }

  private class ManagerActor extends Actor {
    val sources = collection.mutable.Map.empty[K,Source[T,Any]]

    def receive = {
      case GetSource(key) =>
        sender ! sources.getOrElseUpdate(key, {
          factory(key).transform { () => WhenComplete(self ! Completed(key)) }
        })

      case Completed(key) =>
        sources.remove(key)
    }
  }

  private case class GetSource(key: K)
  private case class Completed(key: K)
}

object SourcePool {
  def apply[T,K](factory: K => Source[T,Any])(implicit system: ActorSystem) = new SourcePool(factory)
}
