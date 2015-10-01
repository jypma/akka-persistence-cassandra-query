package akka.persistence.cassandra.streams

import org.scalatest.{ Matchers, WordSpec }
import org.scalatest.concurrent.ScalaFutures

import akka.persistence.cassandra.test.SharedActorSystem
import akka.stream.scaladsl.{ Sink, Source }

class SortedFilterDuplicateSpec extends WordSpec with Matchers with ScalaFutures with SharedActorSystem {
  def toSequence[T] = Sink.fold[Seq[T],T](Seq.empty)((seq, elem) => seq :+ elem)

  "A SortedFilterDuplicate operation" when {
    "grouping case classes by a field" should {
      case class Message(day: Int, message: String)

      def sortedFilterDuplicate = () => new SortedFilterDuplicate[Message,Int,String](_.day)(_.message)

      "remove duplicates for the same key" in {
        val source = Source(List(Message(0, "A"), Message(0, "B"), Message(0, "A")))
        val result = source.transform(sortedFilterDuplicate).runWith(toSequence).futureValue
        result should be (Seq(Message(0, "A"), Message(0, "B")))
      }

      "allow duplicates across different keys" in {
        val source = Source(List(Message(0, "A"), Message(1, "A")))
        val result = source.transform(sortedFilterDuplicate).runWith(toSequence).futureValue
        result should be (Seq(Message(0, "A"), Message(1, "A")))
      }
    }
  }
}
