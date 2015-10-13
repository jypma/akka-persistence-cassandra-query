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

/**
 * Manages a pool of Source[T] instances, managed by key, backed by actor publishers. The created
 * actor publishers are expected to keep running and be re-used for multiple sources.
 *
 * Internally, this is done by using Sink.fanoutPublisher with the given arguments.
 *
 * @param factory Should return a Props for an ActorPublisher that emits messages of type T.
 */
class SourcePool[T,K](factory: K => Props, initialBufferSize: Int, maximumBufferSize: Int)(implicit system: ActorSystem, m:Materializer) {
  private val actor = system.actorOf(Props(new ManagerActor()))

  private implicit val timeout = Timeout(1.second)

  def apply(key: K): Source[T,Unit] = {
    Source((actor ask GetSource(key)).mapTo[Source[T,Any]]).flatten(FlattenStrategy.concat)
  }

  def shutdown(): Future[Unit] = {
    actor ! Shutdown
    Reaper(actor)
  }

  private class ManagerActor extends Actor {
    val running = collection.mutable.Map.empty[K,(ActorRef, Publisher[T])]

    def receive = {
      case GetSource(key) =>
        sender ! Source(running.getOrElseUpdate(key, {
          Source.actorPublisher[T](factory(key))
                .transform { () => WhenComplete(self ! Completed(key)) }
                .toMat(Sink.fanoutPublisher(initialBufferSize, maximumBufferSize))(Keep.both)
                .run()
        })._2)

      case Completed(key) =>
        running.remove(key)

      case Shutdown =>
        val awaitingActors = running.values.map(_._1).toSet
        awaitingActors.foreach(context.watch)
        awaitingActors.foreach(context.stop)
        context become shuttingDown(awaitingActors)
    }

    def shuttingDown(awaiting: Set[ActorRef]): Receive = {
      if (awaiting.isEmpty) {
        context.stop(self)
      }

      {
        case Terminated(actor) if awaiting.contains(actor) =>
          context become shuttingDown(awaiting - actor)
      }
    }
  }

  private case object Shutdown
  private case class GetSource(key: K)
  private case class Completed(key: K)
}

object SourcePool {
  def apply[T,K](initialBufferSize: Int, maximumBufferSize: Int)(factory: K => Props)
                (implicit system: ActorSystem, m:Materializer): SourcePool[T,K] =
    new SourcePool(factory, initialBufferSize, maximumBufferSize)

}
