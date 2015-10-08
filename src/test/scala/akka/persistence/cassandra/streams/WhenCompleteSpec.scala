package akka.persistence.cassandra.streams

import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.{ Matchers, WordSpec }
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import akka.persistence.cassandra.test.SharedActorSystem
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{ Keep, Sink, Source }
import org.scalatest.time.Span
import org.scalatest.time.Seconds

class WhenCompleteSpec extends WordSpec with Matchers with Eventually with ScalaFutures with SharedActorSystem {
  implicit val patience = PatienceConfig(timeout = Span(2, Seconds)) // actual run-time on 4-core machine: 0.1 seconds

  "WhenComplete" when {
    "passed onto a stream" should {
      "invoke its callback when the stream completes" in {
        val invoked = new AtomicInteger(0)

        val srcActor = Source
          .actorRef[Int](1, OverflowStrategy.fail)
          .transform(() => WhenComplete(invoked.incrementAndGet()))
          .toMat(Sink.ignore)(Keep.left)
          .run

        invoked.get should be (0)
        srcActor ! 1
        Thread.sleep(10)
        invoked.get should be (0)
        system.stop(srcActor)
        eventually {
          invoked.get should be (1)
        }
      }
    }
  }
}
