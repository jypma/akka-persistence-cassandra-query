package akka.persistence.cassandra.streams.rt

import akka.actor.ActorRef
import akka.stream.scaladsl.Source
import org.reactivestreams.Publisher
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.FlattenStrategy
import akka.stream.stage.StatefulStage
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.FlowGraph
import akka.stream.scaladsl.MergePreferred
import akka.stream.stage.Context
import akka.actor.ActorSystem
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

trait RealTime[Elem,Time] {
  implicit def chronology:Chronology[Elem,Time]
  def system: ActorSystem
  def pollInterval: FiniteDuration

  /**
   * Returns a source that repeatedly creates [past] Sources (e.g. reading known, persisted elements from disk),
   * which are expected to catch up with a realtime-broadcasting source [rt] that will inform of elements
   * coming in in real-time. Eventually, the returned source will only return the broadcasted events
   * from [rt].
   *
   * The stream completes after having transitioned to real-time, and the [realtime] completes.
   */
  def source(getPast: (Time, Time) => Source[Elem,Any], realtime: Source[Elem,Any]): Source[Elem, ActorRef] = {

    val past = Source.actorRef[Time](2, OverflowStrategy.dropHead).map { startTime =>
      val endTime = chronology.endOfTime
      println("  reading past from " + startTime + " to " + endTime)
      getPast(startTime, endTime).log("past with start " + startTime) concat Source.single(SourceCompleted)
    }.flatten(FlattenStrategy.concat)

    val current = realtime.map(RealTimeElem).concat(Source.single(RealtimeCompleted))

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
          ref ! chronology.beginningOfTime
          become(catchingUp(ref, chronology.beginningOfTime))
          println("  initial pull()")
          ctx.pull()
        case other =>
          ctx.fail(new RuntimeException("Unexpected message: " + other))
      }
    }

    /**
     * We are still reading events from the persistent storage, trying to catch up
     * with real-time.
     */
    def catchingUp(timeActor: ActorRef, from: Time): State = new State {
      println("RealTime: catching up, from" + from)
      var lastRealtime: Option[Time] = None
      var lastPast: Option[Time] = None
      var realtimeCompleted:Boolean = false

      def onPush(msg: Any, ctx: Context[Elem]) = msg match {
        case RealtimeCompleted =>
          println("RealTime: RealtimeCompleted")
          realtimeCompleted = true
          ctx.pull()

        case RealTimeElem(elem) =>
          println("RealTime: " + elem)
          lastRealtime = Some(chronology.getTime(elem))
          ctx.pull()

        case SourceCompleted =>
          println("RealTime: SourceCompleted")
          if (realtimeCompleted) {
            println("RealTime: source and realtimeCompleted")
            // our current run from past is complete, but the real-time source has gone away. Let's just end.
            ctx.finish()
          } else if (lastPast.isDefined) {
            if (lastRealtime.isDefined) {
              if (lastPast.equals(lastRealtime)) {
                println("RealTime: in sync")
                // we're in sync, precisely.
                become(realtime)
              } else if (chronology.isBefore(lastPast.get, lastRealtime.get)) {
                println("RealTime: lastPast " + lastPast.get + " is before " + lastRealtime.get)
                // the "past" source hasn't seen that realtime element yet.
                // simply continue reading from past.
                readMoreFromPast()
              } else if (chronology.isBefore(lastRealtime.get, lastPast.get)) {
                println("RealTime: lastPast " + lastPast.get + " is after " + lastRealtime.get)
                // the "past" source has seen the realtime element, but our actor hasn't gotten it
                // directly from the broadcaster yet. Shouldn't really happen, but we'll continue
                // from past, since that's the best we can do. Let's hope realtime catches up.
                //logger.warn(""
                readMoreFromPast()
              }
            } else {
             println("RealTime: not seen lastRealtime")
             // what to do with elems that have same Time? i.e. only IndexEntry
             // - pass the outgoing Source past a deduplicate filter
             // - take a param "maxElemsWithSameTime", which is the max buffer
            	readMoreFromPast()
            }
            ctx.pull()
          } else {
            println("RealTime: not seen lastPast")
            // retry from the same startTime, we need a run where we get both elements from our source AND a realtime element.
            // but put in a delay before re-running the query, where we CAN receive the realtime elems.
            readMoreFromPast()
            ctx.pull()
          }

        case mustBeElem => // everything else is forwarded from the incoming "past" stream
          println("  push() " + mustBeElem)
          val elem = mustBeElem.asInstanceOf[Elem]
          lastPast = Some(chronology.getTime(elem))
          ctx.push(elem)
      }

      def readMoreFromPast() {
        val nextFrom = lastPast.getOrElse(from)
    		lastRealtime = None
    		lastPast = None
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
  def source[Elem,Time](getPast: (Time, Time) => Source[Elem,Any],
                        realtime: Source[Elem,Any],
                        pollInterval_ : FiniteDuration = 5.seconds)
                       (implicit sys:ActorSystem, ch:Chronology[Elem,Time]): Source[Elem, ActorRef] = {
    new RealTime[Elem,Time] {
      override def pollInterval = pollInterval_
      override def chronology = ch
      override def system = sys
    }.source(getPast, realtime)
  }
}
