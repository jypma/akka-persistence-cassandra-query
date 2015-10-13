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

class RealTimeSpec extends WordSpec with Matchers with ScalaFutures with SharedActorSystem {

  case class Event(time: Long, contents: String)

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
    class Fixture(initialEvents: SortedMap[Long,Event] = SortedMap.empty, initialTime:Long = 0) {
      self =>
      var now: Long = initialTime
      var pastEvents: SortedMap[Long,Event] = initialEvents

      def emit(contents: String): Event = {
        val event = Event(now, contents)
        pastEvents = pastEvents + (event.time -> event)
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
      def getPast(from: Long, to: Long) = Source(pastEvents.from(from).to(to).values.toList)

      val receiver = TestProbe("receiver")
      // "realtimeSourceActor" is the actor representing the RealTime instance under test, which is merging
      // the "realtimeActor" above which invocations the getPast() method.
      val realtimeSourceActor = RealTime.source(getPast, Source(realtimePublisher), 10.milliseconds).toMat(Sink.actorRef(receiver.ref, "complete"))(Keep.left).run()

      def cleanup() {
        system.stop(realtimeActor)
        system.stop(realtimeSourceActor)
      }
    }

    def fixture(initialEvents: SortedMap[Long,Event] = SortedMap.empty, initialTime:Long = 0)(testcode: Fixture => Any) = {
      val f = new Fixture(initialEvents, initialTime)
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
        initialEvents = SortedMap((1l -> Event(1, "hello")), (2l -> Event(2, "world"))),
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
  }
}
