package akka.persistence.cassandra.streams.rt

import scala.collection.SortedMap

import org.scalatest.{ Matchers, WordSpec }
import org.scalatest.concurrent.ScalaFutures

import akka.persistence.cassandra.test.SharedActorSystem
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.stream.stage.{ Context, PushStage }
import akka.testkit.TestProbe

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

      val (realtimeActor, realtimePublisher) = Source.actorRef[Event](10, OverflowStrategy.fail).toMat(Sink.publisher)(Keep.both).run()
      def getPast(from: Long, to: Long) = Source(pastEvents.from(from).to(to).values.toList)

      val receiver = TestProbe("receiver")
      RealTime.source(getPast, realtimePublisher).runWith(Sink.actorRef(receiver.ref, "complete"))
    }

    "reading from an empty history" should {
      "transition to forwarding real-time events immediately" in new Fixture {
        now = 1
        receiver.expectMsg(emit("hello"))

        now = 2
        receiver.expectMsg(emit("world"))
      }
    }

    "reading from history while real-time events come in with later timestamps" should {
      "ignore the real-time events until it has seen a historic event and a real-time event with the same timestamp" in new Fixture(
        initialEvents = SortedMap((1l -> Event(1, "hello")), (2l -> Event(2, "world"))),
        initialTime = 3
      ){
        realtimeActor ! Event(3, "should be ignored since no past event with timestamp 3 was seen yet")

        receiver.expectMsg(Event(1, "hello"))
        receiver.expectMsg(Event(2, "world"))
        receiver.expectMsg(emit("real-time and past event for timestamp 3"))

        now = 4
        realtimeActor ! Event(4, "only sent to real-time, should be forwarded directly")
        receiver.expectMsg(Event(4, "only sent to real-time, should be forwarded directly"))
      }
    }

    "noticing that the given real-time stream completes" should {
      "complete itself" in new Fixture {
        system.stop(realtimeActor)
        receiver.expectMsg("complete")
      }
    }
  }
}
