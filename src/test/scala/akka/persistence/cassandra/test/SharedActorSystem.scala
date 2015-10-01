package akka.persistence.cassandra.test

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

/**
 * Marks a spec that is to OK to run in a shared actor system, so it can run concurrently with other specs.
 */
trait SharedActorSystem {
  implicit val system = SharedActorSystem.system
  implicit val materializer = SharedActorSystem.materializer
}

object SharedActorSystem {
  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

}
