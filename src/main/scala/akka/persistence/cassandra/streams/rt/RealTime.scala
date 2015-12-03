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
import com.typesafe.scalalogging.StrictLogging

object RealTime extends StrictLogging {
  /**
   * Returns a source that repeatedly creates [past] Sources (e.g. reading known, persisted elements from disk),
   * which are expected to catch up with a realtime-broadcasting source [rt] that will inform of elements
   * coming in in real-time. Eventually, the returned source will only return the broadcasted events
   * from [rt].
   *
   * The stream completes after having transitioned to real-time, and the [realtime] completes.
   */
  def apply[Elem,Time](getPast: (Time, Time) => Source[Elem,Any],
                       realtime: Source[Elem,Any],
                       offset: Time,
                       pollInterval : FiniteDuration = 5.seconds)
                      (implicit system:ActorSystem, chronology:Chronology[Elem,Time]): Source[Elem, ActorRef] = {

    val past = Source.actorRef[Time](2, OverflowStrategy.dropHead).map { startTime =>
      val endTime = chronology.endOfTime
      logger.info("Reading past from {} to {}", startTime.asInstanceOf[AnyRef], endTime.asInstanceOf[AnyRef])
      getPast(startTime, endTime) concat Source.single(SourceCompleted)
    }.flatten(FlattenStrategy.concat)

    case class RealTimeElem(elem:Elem)
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

    class ControlStage extends StatefulStage[Any,Elem] {
      /**
       * The first element on the stream must always be the ActorRef of the time actor
       * sending out start times. This is achieved by the MergePreferred node above.
       */
      def initial = new State {
        def onPush(msg: Any, ctx: Context[Elem]) = msg match {
          case ref:ActorRef =>
            ref ! chronology.latest(chronology.beginningOfTime, offset)
            become(catchingUp(ref, offset))
            ctx.pull()
        }
      }

      /**
       * We are still reading events from the persistent storage, trying to catch up
       * with real-time.
       */
      def catchingUp(timeActor: ActorRef, from: Time): State = new State {
        logger.info(s"Now catching up from $from") 
        var lastRealtime: Option[Time] = None
        var lastPast: Option[Time] = None
        var realtimeCompleted:Boolean = false

        def onPush(msg: Any, ctx: Context[Elem]) = msg match {
          case RealtimeCompleted =>
            logger.debug("RealtimeCompleted")
            realtimeCompleted = true
            ctx.pull()

          case RealTimeElem(elem) =>
            logger.debug(s"Real-time elem $elem")
            lastRealtime = Some(chronology.getTime(elem))
            ctx.pull()

          case SourceCompleted =>
            logger.debug("SourceCompleted")
            if (realtimeCompleted) {
              logger.debug("Past and real-time completed, finishing.")
              // our current run from past is complete, but the real-time source has gone away. Let's just end.
              ctx.finish()
            } else if (lastPast.isDefined) {
              if (lastRealtime.isDefined) {
                if (lastPast.equals(lastRealtime)) {
                  logger.info("Caught up with real-time.")
                  // we're in sync, precisely.
                  become(realtime)
                } else if (chronology.isBefore(lastPast.get, lastRealtime.get)) {
                  logger.debug("LastPast {} is before lastRealtime {}", lastPast, lastRealtime)
                  // the "past" source hasn't seen that realtime element yet.
                  // simply continue reading from past.
                  readMoreFromPast()
                } else if (chronology.isBefore(lastRealtime.get, lastPast.get)) {
                  logger.debug("LastPast {} is after lastRealtime {}", lastPast, lastRealtime)
                  // the "past" source has seen the realtime element, but our actor hasn't gotten it
                  // directly from the broadcaster yet. Shouldn't really happen, but we'll continue
                  // from past, since that's the best we can do. Let's hope realtime catches up.
                  //logger.warn(""
                  readMoreFromPast()
                }
              } else {
               logger.debug("not seen lastRealtime")
               // what to do with elems that have same Time? i.e. only IndexEntry
               // - pass the outgoing Source past a deduplicate filter
               // - take a param "maxElemsWithSameTime", which is the max buffer
              	readMoreFromPast()
              }
              ctx.pull()
            } else {
              logger.debug("not seen lastPast")
              // retry from the same startTime, we need a run where we get both elements from our source AND a realtime element.
              // but put in a delay before re-running the query, where we CAN receive the realtime elems.
              readMoreFromPast()
              ctx.pull()
            }

          case mustBeElem => // everything else is forwarded from the incoming "past" stream
            logger.debug(s"Past elem $mustBeElem")
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

    merged.transform(() => new ControlStage()).transform(() => Deduplicate())
  }

  private[rt] case object SourceCompleted
  private[rt] case object RealtimeCompleted
}
