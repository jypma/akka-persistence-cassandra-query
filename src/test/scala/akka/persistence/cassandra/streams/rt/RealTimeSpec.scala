package akka.persistence.cassandra.streams.rt

import scala.collection.SortedMap
import org.scalatest.{ Matchers, WordSpec }
import org.scalatest.concurrent.ScalaFutures
import akka.persistence.cassandra.test.SharedActorSystem
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.stream.stage.{ Context, PushStage }
import akka.testkit.TestProbe
import scala.concurrent.duration.DurationInt
import scala.collection.immutable.SortedSet
import scala.concurrent.Future
import akka.actor.Status.Failure

class RealTimeSpec extends WordSpec with Matchers with ScalaFutures with SharedActorSystem {

  case class Event(time: Long, contents: String) extends Ordered[Event] {
    def compare(that: Event) = time.compare(that.time)
  }

  def deduplicate[T] = new PushStage[T,T] {
    var last:Option[T] = None

    override def onPush(elem:T, ctx:Context[T]) = {
      if (last.isEmpty || last.get != elem) {
        last = Some(elem)
        ctx.push(elem)
      } else {
        last = Some(elem)
        ctx.pull()
      }
    }
  }

  "RealTime.source" when {
    class Fixture(initialEvents: SortedSet[Event] = SortedSet.empty, initialTime:Long = 0, offset:Long = 0) {
      self =>
      var now: Long = initialTime
      var pastEvents: SortedSet[Event] = initialEvents

      def emit(contents: String): Event = {
        val event = Event(now, contents)
        pastEvents += event
        realtimeActor ! event
        event
      }

      implicit val chronology = new Chronology[Event,Long] {
        override def getTime(elem: Event) = elem.time
        override def beginningOfTime = Long.MinValue
        override def endOfTime = self.now
        override def isBefore(a: Long, b: Long) = a < b
      }

      // "realtimeActor" is the actor to which we can send simulated actions that occur in real-time
      val (realtimeActor, realtimePublisher) = Source.actorRef[Event](10, OverflowStrategy.fail).toMat(Sink.publisher)(Keep.both).run()
      def getPast(from: Long, to: Long) = Source(pastEvents.from(Event(from,"")).to(Event(to,"")).toList)

      val receiver = TestProbe("receiver")
      // "realtimeSourceActor" is the actor representing the RealTime instance under test, which is merging
      // the "realtimeActor" above which invocations the getPast() method.
      val realtimeSourceActor = RealTime(getPast, Source(realtimePublisher), offset, 10.milliseconds).toMat(Sink.actorRef(receiver.ref, "complete"))(Keep.left).run()

      def cleanup() {
        system.stop(realtimeActor)
        system.stop(realtimeSourceActor)
      }
    }

    def fixture(initialEvents: SortedSet[Event] = SortedSet.empty, initialTime:Long = 0, offset:Long = 0)(testcode: Fixture => Any) = {
      val f = new Fixture(initialEvents, initialTime, offset)
      try testcode(f) finally f.cleanup()
    }
    
    def withFixture[F <: Fixture](f: F)(testcode: F => Any): Unit = {
      try testcode(f) finally f.cleanup()      
    }

    "reading from an empty history" should {
      "transition to forwarding real-time events immediately" in fixture() { f =>
        f.now = 1
        f.receiver.expectMsg(f.emit("hello"))

        f.now = 2
        f.receiver.expectMsg(f.emit("world"))
      }
    }

    "reading from history while real-time events come in with later timestamps" should {
      "ignore the real-time events until it has seen a historic event and a real-time event with the same timestamp" in fixture (
        initialEvents = SortedSet(Event(1, "hello"), Event(2, "world")),
        initialTime = 3
      ){ f =>
        f.realtimeActor ! Event(3, "should be ignored since no past event with timestamp 3 was seen yet")

        f.receiver.expectMsg(Event(1, "hello"))
        f.receiver.expectMsg(Event(2, "world"))
        // we emit several realtime+past events, so that the RealTime source is very unlikely to miss them all.
        // however, there's still a race condition that would allow this test to fail.
        f.receiver.expectMsg(f.emit("real-time and past event for timestamp 3"))
        Thread.sleep(20)
        f.now = 4
        f.receiver.expectMsg(f.emit("real-time and past event for timestamp 4"))
        Thread.sleep(20)
        f.now = 5
        f.receiver.expectMsg(f.emit("real-time and past event for timestamp 5"))

        Thread.sleep(20)
        val rtEvent = Event(6, "only sent to real-time, should be forwarded directly")
        f.realtimeActor ! rtEvent
        f.receiver.expectMsg(rtEvent)
      }
    }

    "noticing that the given real-time stream completes" should {
      "complete itself" in fixture() { f =>
        f.now = 1
        f.receiver.expectMsg(f.emit("hello"))
        system.stop(f.realtimeActor)
        f.receiver.expectMsg("complete")
      }
    }
    
    "reading from history from a given offset" should {
      "return events from and including the offset" in fixture (
        initialEvents = SortedSet(
          Event(1, "hello"), 
          Event(2, "world")
        ),
        initialTime = 3,
        offset = 2
      ){ f =>
        f.receiver.expectMsg(Event(2, "world"))
      }
    }
    
    "exposed to an error in a past source" should {
      "forward the error and close with error itself" in withFixture (new Fixture {
        import system.dispatcher
        override def getPast(from: Long, to: Long) = Source(Future { throw new RuntimeException("oh-no") })
      }) { f =>
        val failure = f.receiver.expectMsgType[Failure]
        failure.cause shouldBe a[RuntimeException]
        failure.cause.getMessage should be("oh-no")
      }
    }
    
    "exposed to an error in the realtime source, while still reading from a past source" should {
      "forward the error and close with error itself" in withFixture (new Fixture {
        import system.dispatcher
        override def getPast(from: Long, to: Long) = Source(Future { Thread.sleep(1000); Event(1,"1") })
      }) { f =>
        f.realtimeActor ! Failure(new RuntimeException("oh-no"))
        
        val failure = f.receiver.expectMsgType[Failure]
        failure.cause shouldBe a[RuntimeException]
        failure.cause.getMessage should be("oh-no")
      } 
    }
    
    "exposed to an error in the realtime source, while caught up with real-time" should {
      "forward the error and close with error itself" in fixture() { f =>
        f.now = 1
        f.receiver.expectMsg(f.emit("hello"))
        
        // caught up with real-time now.
        f.realtimeActor ! Failure(new RuntimeException("oh-no"))
        
        val failure = f.receiver.expectMsgType[Failure]
        failure.cause shouldBe a[RuntimeException]
        failure.cause.getMessage should be("oh-no")        
      }
    }
  }
}
