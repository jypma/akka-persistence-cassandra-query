package akka.persistence.cassandra.streams.rt

import akka.actor.ActorRef
import akka.stream.scaladsl.Source
import org.reactivestreams.Publisher
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.FlattenStrategy
import akka.stream.stage.StatefulStage
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.FlowGraph
import akka.stream.scaladsl.Merge
import akka.stream.scaladsl.MergePreferred
import akka.stream.stage.Context
import akka.actor.ActorSystem
import scala.concurrent.duration._

trait RealTime[Elem,Time] {
  implicit def chronology:Chronology[Elem,Time]
  def system: ActorSystem
  def pollInterval = 10.milliseconds

  /**
   * Returns a source that repeatedly creates [past] Sources (e.g. reading known, persisted elements from disk),
   * which are expected to catch up with a realtime-broadcasting Publisher [rt] that will inform of elements
   * coming in in real-time. Eventually, the returned source will only return the broadcasted events
   * from [rt].
   *
   * The stream completes after having transitioned to real-time, and the [realtime] completes.
   */
  def source(getPast: (Time, Time) => Source[Elem,Any], realtime: Publisher[Elem]): Source[Elem, ActorRef] = {

    val past = Source.actorRef[Time](2, OverflowStrategy.dropHead).map { startTime =>
      val endTime = chronology.now
      getPast(startTime, endTime) concat Source.single(SourceCompleted)
    }.flatten(FlattenStrategy.concat)

    val current = Source(realtime).map(RealTimeElem).transform { () => OnComplete(RealtimeCompleted) }

    val merged = Source(past, current)(Keep.left) { implicit b =>
      (in1, in2) =>
        import FlowGraph.Implicits._

        val merge = b.add(MergePreferred[Any](2))
        b.materializedValue ~> merge.preferred
        in1 ~> merge.in(0)
        in2 ~> merge.in(1)

        merge.out
    }

    merged.transform(() => new ControlStage).transform(() => Deduplicate())
  }

  private[rt] case object SourceCompleted
  private[rt] case object RealtimeCompleted
  private[rt] case class RealTimeElem(elem:Elem)

  private[rt] class ControlStage(implicit chronology:Chronology[Elem,Time]) extends StatefulStage[Any,Elem] {
    /**
     * The first element on the stream must always be the ActorRef of the time actor
     * sending out start times. This is achieved by the MergePreferred node above.
     */
    def initial = new State {
      def onPush(msg: Any, ctx: Context[Elem]) = msg match {
        case ref:ActorRef =>
          println("Received actor")
          ref ! chronology.beginningOfTime
          become(catchingUp(ref, chronology.beginningOfTime))
          ctx.pull()
        case other =>
          println("Unexpected message: " + other)
          ctx.fail(new RuntimeException("Unexpected message: " + other))
      }
    }

    /**
     * We are still reading events from the persistent storage, trying to catch up
     * with real-time.
     */
    def catchingUp(timeActor: ActorRef, from: Time): State = new State {
      var lastRealtime: Option[Time] = None
      var lastPast: Option[Time] = None
      var realtimeCompleted:Boolean = false

      def onPush(msg: Any, ctx: Context[Elem]) = msg match {
        case RealtimeCompleted =>
          realtimeCompleted = true
          ctx.pull()

        case RealTimeElem(elem) =>
          println("Got realtime elem " + elem)
          lastRealtime = Some(chronology.getTime(elem))
          ctx.pull()

        case SourceCompleted =>
          println("completed a source")
          if (realtimeCompleted) {
            // our current run from past is complete, but the real-time source has gone away. Let's just end.
            ctx.finish()
          } else if (lastPast.isDefined) {
            if (lastRealtime.isDefined) {
              if (lastPast.equals(lastRealtime)) {
                // we're in sync, precisely.
                become(realtime)
              } else if (chronology.isBefore(lastPast.get, lastRealtime.get)) {
                // the "past" source hasn't seen that realtime element yet.
                // simply continue reading from past.
                readMoreFromPast()
              } else if (chronology.isBefore(lastRealtime.get, lastPast.get)) {
                // the "past" source has seen the realtime element, but our actor hasn't gotten it
                // directly from the broadcaster yet. Shouldn't really happen, but we'll continue
                // from past, since that's the best we can do. Let's hope realtime catches up.
                //logger.warn(""
                readMoreFromPast()
              }
            } else {
             // what to do with elems that have same Time? i.e. only IndexEntry
             // - pass the outgoing Source past a deduplicate filter
             // - take a param "maxElemsWithSameTime", which is the max buffer
            	readMoreFromPast()
            }
            ctx.pull()
          } else {
            // retry from the same startTime, we need a run where we get both elements from our source AND a realtime element.
            // but put in a delay before re-running the query, where we CAN receive the realtime elems.
            readMoreFromPast()
            ctx.pull()
          }

        case mustBeElem => // everything else is forwarded from the incoming "past" stream
          val elem = mustBeElem.asInstanceOf[Elem]
          lastPast = Some(chronology.getTime(elem))
          ctx.push(elem)
      }

      def readMoreFromPast() {
        val nextFrom = lastPast.getOrElse(from)
    		lastRealtime = None
    		lastPast = None
    		println("Reading some more from " + nextFrom + " after having read from " + from)
        if (nextFrom == from) {
          // We're about to poll for the same query.
          system.scheduler.scheduleOnce(pollInterval, timeActor, nextFrom)(system.dispatcher, system.deadLetters)
        } else {
          // Tell the time actor to start reading the next source
        	timeActor ! nextFrom
        }
        become(catchingUp(timeActor, nextFrom))
      }
    }

    def realtime = new State {
      def onPush(msg: Any, ctx: Context[Elem]) = msg match {
        case RealTimeElem(elem) =>
          ctx.push(elem)
        case RealtimeCompleted =>
          ctx.finish()
        case SourceCompleted =>
          // ignore
          ctx.pull()
        case elemFromPast =>
          // log.error
          // this shouldn't happen
          ctx.pull()
      }
    }

  }
}

object RealTime {
  def source[Elem,Time](getPast: (Time, Time) => Source[Elem,Any], realtime: Publisher[Elem])
                       (implicit sys:ActorSystem, ch:Chronology[Elem,Time]) = {
    new RealTime[Elem,Time] {
      override def chronology = ch
      override def system = sys
    }.source(getPast, realtime)
  }
}
