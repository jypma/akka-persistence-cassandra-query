package akka.persistence.cassandra.streams

import org.scalatest.{ Matchers, WordSpec }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{ Seconds, Span }

import akka.persistence.cassandra.test.SharedActorSystem
import akka.stream.scaladsl.{ Sink, Source }

class FanoutAndMergeSpec extends WordSpec with Matchers with ScalaFutures with SharedActorSystem {
  implicit val patience = PatienceConfig(timeout = Span(10, Seconds)) // actual run-time on 4-core machine: 1 second

  "The FanoutAndMerge operation" should {
    case class InElem(i:Int)
    case class OutElem(s:String)

    def getKey(elem: InElem) = elem.i
    def getSource(elem: InElem) = Source(elem.i until (elem.i + 200))

    "eventually forward and receive all elements in all generated nested sources" in {
      val (sink, source) = FanoutAndMerge(getKey, getSource)

      Source(1 to 200).map(i => InElem(i * 200)).runWith(sink)
      val count = source.runWith(Sink.fold(0)((i, elem) => i + 1)).futureValue

      count should be (40000)
    }

    "only process input elements once when they yield the same key" in {
      val (sink, source) = FanoutAndMerge(getKey, getSource)

      Source(List(1,1,1)).map(i => InElem(i * 200)).runWith(sink)
      val count = source.runWith(Sink.fold(0)((i, elem) => i + 1)).futureValue

      count should be (200)
    }
  }
}
