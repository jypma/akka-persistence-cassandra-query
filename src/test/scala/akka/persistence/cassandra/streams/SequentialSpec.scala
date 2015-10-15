package akka.persistence.cassandra.streams

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.Matchers
import org.scalatest.WordSpec
import akka.persistence.cassandra.test.SharedActorSystem
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.Sink
import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.time.Seconds
import org.scalatest.time.Span

class SequentialSpec extends WordSpec with Matchers with ScalaFutures with SharedActorSystem {
  implicit val patience = PatienceConfig(timeout = Span(2, Seconds))
  def toSequence[T] = Sink.fold[Seq[T],T](Seq.empty)((seq, elem) => seq :+ elem)

  "Sequential.foreachUntilEmpty" when {
    "given a factory of empty sources" should {
      "complete immediately after reading only from 1 source" in {
        val invocations = new AtomicInteger(0)
        val src = Sequential.forEachUntilEmpty(0)(_ + 1) { i =>
          invocations.incrementAndGet()
          Source.empty[String]
        }
        val result = src.runWith(toSequence).futureValue

        result should have size (0)
        invocations.get should be (1)
      }
    }

    "given a factory of multiple sources that eventually is empty" should {
      "concatenate all sources" in {
        val src = Sequential.forEachUntilEmpty(0)(_ + 1) { i =>
          if (i < 3) {
            Source.single("item " + i)
          } else {
        	  Source.empty[String]
          }
        }
        val result = src.runWith(toSequence).futureValue

        result should be (Seq("item 0", "item 1", "item 2"))
      }
    }
  }
}
