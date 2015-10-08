package akka.persistence.cassandra.query

import org.scalatest.Matchers
import org.scalatest.WordSpec
import akka.persistence.cassandra.test.SharedActorSystem
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.Sink
import org.scalatest.concurrent.Eventually
import org.scalatest.time.Span
import org.scalatest.time.Seconds

class SourcePoolSpec extends WordSpec with Matchers with Eventually with SharedActorSystem  {
  implicit val patience = PatienceConfig(timeout = Span(2, Seconds)) // actual run-time on 4-core machine: 0.2 seconds

  def toSequence[T] = Sink.fold[Seq[T],T](Seq.empty)((seq, elem) => seq :+ elem)

  "SourcePool" when {
    "handling a source with one element" should {
      "call the factory again once all elements have been read from all materializations of the source" in {
        var invocations = 0
        val pool = SourcePool { key: Int => invocations += 1; Source.single(key) }
        val src1 = pool(42)
        val src2 = pool(42)
        src1.runWith(Sink.ignore)
        eventually {
           invocations should be (1)
        }
        Thread.sleep(10)
        src2.runWith(Sink.ignore)
        Thread.sleep(10) // allow all sources to complete
        invocations should be (1)

        eventually {
        	pool(42).runWith(Sink.ignore)
          invocations should not be (1)
        }
      }
    }
  }
}
