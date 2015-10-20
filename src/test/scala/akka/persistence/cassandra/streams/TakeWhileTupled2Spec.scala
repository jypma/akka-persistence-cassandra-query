package akka.persistence.cassandra.streams

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.WordSpec
import akka.persistence.cassandra.test.SharedActorSystem
import akka.stream.scaladsl.Sink
import org.scalatest.Matchers
import akka.stream.scaladsl.Source
import org.scalatest.time.Seconds
import org.scalatest.time.Span

class TakeWhileTupled2Spec extends WordSpec with Matchers with ScalaFutures with SharedActorSystem {
  implicit val patience = PatienceConfig(timeout = Span(2, Seconds)) 
  
  def toSequence[T] = Sink.fold[Seq[T],T](Seq.empty)((seq, elem) => seq :+ elem)
  
  "A TakeWhileTupled2 operation" when {
    def takeWhileIncreasing = () => TakeWhileTupled2[Int](_ < _)
    def run(source: Source[Int,_]) = source.transform(takeWhileIncreasing).runWith(toSequence).futureValue
    
    "matching an empty stream" should {
      "yield an empty result" in {
        val source = Source.empty[Int]
        run(source) should be (Seq.empty)
      }
    }
    
    "matching a stream with only 1 element" should {
      "yield that element" in {
        val source = Source.single(1)
        run(source) should be (Seq(1))
      }
    }
    
    "matching a stream with 2 elements" should {
      "yield both elements if they match the predicate" in {
        val source = Source(1 :: 2 :: Nil)
        run(source) should be (Seq(1, 2))        
      }
      
      "yield only the first element if the elements don't match the predicate" in {
        val source = Source(2 :: 1 :: Nil)
        run(source) should be (Seq(2))                
      }
    }
    
    "matching a stream with 3 elements" should {
      "yield the first two elements if the second and third don't match the predicate" in {
        val source = Source(1 :: 2 :: 0 :: Nil)
        run(source) should be (Seq(1, 2))                
      }
      
      "yield all elements if all pairs match the predicate" in {
        val source = Source(1 :: 2 :: 3 :: Nil)
        run(source) should be (Seq(1, 2, 3))                        
      }
    }
  }
}
