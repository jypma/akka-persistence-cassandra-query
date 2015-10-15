package akka.persistence.cassandra.query

import org.scalatest.Matchers
import org.scalatest.WordSpec
import akka.persistence.cassandra.test.SharedActorSystem
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.Sink
import org.scalatest.concurrent.Eventually
import org.scalatest.time.Span
import org.scalatest.time.Seconds
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request
import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef

class SourcePoolSpec extends WordSpec with Matchers with Eventually with SharedActorSystem  {
  implicit val patience = PatienceConfig(timeout = Span(2, Seconds)) // actual run-time on 4-core machine: 0.2 seconds

  def toSequence[T] = Sink.fold[Seq[T],T](Seq.empty)((seq, elem) => seq :+ elem)

  class SingleRequestActor[T](value: T) extends Actor {

    def receive = {
      case "subscribe" =>
        sender ! value
        println("stopping myself")
        context.stop(self)
    }
  }

  "SourcePool" when {
    "handling a source with one element" should {
      "call the factory again once its actor has ended" in {
        var invocations = 0
        val pool = SourcePool("subscribe",1) { key: Int => invocations += 1; Props(new SingleRequestActor(key)) }
        val src = pool(42)
        src.runWith(Sink.ignore)
        eventually {
           invocations should be (1)
        }
        Thread.sleep(10) // wait for all streams to actually complete

        eventually {
        	pool(42).runWith(Sink.ignore)
          invocations should not be (1)
        }
      }
    }
  }
}
