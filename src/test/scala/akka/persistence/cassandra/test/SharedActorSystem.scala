package akka.persistence.cassandra.test

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.actor.Actor
import akka.actor.Props
import com.typesafe.config.ConfigFactory

/**
 * Marks a spec that is to OK to run in a shared actor system, so it can run concurrently with other specs.
 */
trait SharedActorSystem {
  implicit val system = SharedActorSystem.system
  implicit val materializer = SharedActorSystem.materializer
}

object SharedActorSystem {
  private val config = ConfigFactory.load();
  private implicit val system = ActorSystem(config.getString("clustering.name"), config)
  private implicit val materializer = ActorMaterializer()

  private class DummyActor extends Actor {
    def receive = { case _ => }
  }

  private val dummyActorToStartSystem = system.actorOf(Props[DummyActor])
}
