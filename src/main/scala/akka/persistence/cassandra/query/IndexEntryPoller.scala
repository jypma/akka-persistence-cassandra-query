package akka.persistence.cassandra.query

import java.time.Instant
import scala.concurrent.duration.FiniteDuration
import akka.actor.ActorLogging
import akka.actor.Actor
import akka.stream.Materializer
import java.time.Duration
import scala.concurrent.duration._
import akka.actor.ActorRef
import akka.actor.Props
import akka.pattern.BackoffSupervisor
import akka.actor.SupervisorStrategy
import akka.actor.Terminated

object IndexEntryPoller {
  case object Subscribe  
  
  private case object RestartWorker
}

/**
 * Actor which publishes index entries as they become visible in cassandra.
 */
class IndexEntryPoller (
  workerProps: Props,
     
  restartDelay:FiniteDuration = 3.seconds
) extends Actor with ActorLogging {
  
  import IndexEntryPoller._
  import context.dispatcher
  
  val subscribers = collection.mutable.Set.empty[ActorRef]
  override val supervisorStrategy = SupervisorStrategy.stoppingStrategy
  
  var worker:Option[ActorRef] = None
  self ! RestartWorker
  
  def receive = {
    case msg @ Subscribe =>
      log.debug("Adding subscriber {} while worker is {}", sender, worker)
      
      subscribers += sender
      worker foreach (_ forward msg) 
      
    case Terminated(actor) if actor == worker.get =>
      log.debug("Worker {} has terminated, stopping subscribers {}", worker, subscribers)
      
      context.system.scheduler.scheduleOnce(restartDelay, self, RestartWorker)
      subscribers foreach (context.system.stop)
      subscribers.clear()
      worker = None
      
    case RestartWorker =>
      worker = Some(context.actorOf(workerProps))
      log.debug("Worker {} has started, adding subscribers {}", worker, subscribers)
      
      context.watch(worker.get)
      subscribers foreach (subscriber => worker.get.tell(Subscribe, subscriber))
  }
}
