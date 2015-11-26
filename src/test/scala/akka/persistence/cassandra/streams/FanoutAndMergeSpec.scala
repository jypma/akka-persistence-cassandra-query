package akka.persistence.cassandra.streams

import org.scalatest.{ Matchers, WordSpec }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{ Seconds, Span }
import akka.persistence.cassandra.test.SharedActorSystem
import akka.stream.scaladsl.{ Sink, Source }
import akka.stream.OverflowStrategy
import akka.testkit.TestProbe
import akka.stream.scaladsl.Keep
import scala.util.Random

class FanoutAndMergeSpec extends WordSpec with Matchers with ScalaFutures with SharedActorSystem {
  implicit val patience = PatienceConfig(timeout = Span(10, Seconds)) // actual run-time on 4-core machine: 1 second
  def toSequence[T] = Sink.fold[Seq[T],T](Seq.empty)((seq, elem) => seq :+ elem)

  "The FanoutAndMerge operation" should {
    case class InElem(key: Int, value: String) 

    def getKey(elem: InElem) = elem.key

    "eventually forward and receive all elements in all generated nested sources" in {
      def getSource(elem: InElem) = Source(elem.key until (elem.key + 200))
      
      val (sink, source) = FanoutAndMerge(getKey, getSource)

      Source(1 to 200).map(i => InElem(i * 200, i.toString)).runWith(sink)
      val count = source.runWith(Sink.fold(0)((i, elem) => i + 1)).futureValue

      count should be (40000)
    }

    "process all nested sources for one key before continuing with the next key" in {
      def getSource(elem: InElem) = Source.repeat(elem.key).map { i =>
        Thread.sleep(Random.nextInt(5))
        i
      }.take(100)
      val (sink, source) = FanoutAndMerge(getKey, getSource)
      Source(List(1,1,1,1,1,1,1,1,1,1,2)).map(i => InElem(i, i.toString)).runWith(sink)
      
      val result = source.runWith(toSequence).futureValue
      result should have size(11 * 100)
      result should contain inOrderOnly(1,2)
      result(999) should be (1)
      result(1000) should be (2)
    }
    
    "process nested sources with the same key out of order" in {
      def getSource(elem: InElem) = Source.repeat(elem.value).map { i =>
        Thread.sleep(Random.nextInt(5))
        i
      }.take(100)
      val (sink, source) = FanoutAndMerge(getKey, getSource)
      Source(List(InElem(1,"one"), InElem(1,"two"), InElem(1,"three"), InElem(1,"four"))).runWith(sink)
      
      val result = source.runWith(toSequence).futureValue
      result should have size(4 * 100)
      result should contain only ("one", "two", "three", "four")
      result should not contain inOrderOnly ("one", "two", "three", "four")
      result.filter(_ == "one") should have size(100)
      result.filter(_ == "two") should have size(100)
      result.filter(_ == "three") should have size(100)
      result.filter(_ == "four") should have size(100)
    }
    
    "resume processing after reaching a state where all substreams have completed and nothing was queued" in {
      def getSource(elem: InElem) = Source.single(elem)
      val (sink, source) = FanoutAndMerge(getKey, getSource)
      val trigger = Source.actorRef[InElem](1, OverflowStrategy.fail).toMat(sink)(Keep.left).run()
      val receiver = TestProbe()
      source.runWith(Sink.actorRef(receiver.ref, "done"))
      
      trigger ! InElem(1,"1.1")
      trigger ! InElem(1,"1.2")
      trigger ! InElem(1,"1.3")
      receiver.expectMsgType[InElem].key should be (1)
      receiver.expectMsgType[InElem].key should be (1)
      receiver.expectMsgType[InElem].key should be (1)
      
      Thread.sleep(100) // Allow the intermediate FanoutActor to discover all sub-streams have completed
      
      trigger ! InElem(2,"2.1")
      receiver.expectMsgType[InElem].key should be (2)
    }
  }
}
