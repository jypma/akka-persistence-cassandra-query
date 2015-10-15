package akka.persistence.cassandra.streams

import akka.actor.ActorSystem
import akka.actor.ActorRef
import akka.actor.Actor
import akka.actor.Terminated
import scala.concurrent.Promise
import scala.concurrent.Future
import akka.actor.Props

object Reaper {
  /**
   * Returns a future that completes when all given actor refs have terminated.
   */
  def apply(refs: Iterable[ActorRef])(implicit system:ActorSystem): Future[Unit] = {
    val p = Promise[Unit]
    system.actorOf(Props(new Reaper(refs.toSeq, p)))
    p.future
  }

  /**
   * Returns a future that completes when all given actor refs have terminated.
   */
  def apply(refs: ActorRef*)(implicit system:ActorSystem): Future[Unit] = {
    apply(refs.toIterable)
  }
}

class Reaper(refs: Seq[ActorRef], p:Promise[Unit]) extends Actor {
  refs.foreach(context.watch)

  var awaiting = refs.toSet

  def receive = {
    case Terminated(who) =>
      awaiting -= who
      if (awaiting.isEmpty) {
        p.success(Unit)
        context.stop(self)
      }
  }
}
