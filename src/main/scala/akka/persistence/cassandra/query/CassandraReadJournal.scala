package akka.persistence.cassandra.query

import akka.actor.ExtendedActorSystem
import com.typesafe.config.Config
import akka.persistence.query.scaladsl.ReadJournal
import akka.persistence.query.Hint
import akka.stream.scaladsl.Source
import akka.persistence.query.Query
import akka.persistence.query.EventsByTag
import CassandraReadJournal._
import java.time.Instant
import akka.persistence.serialization.MessageFormats
import akka.persistence.cassandra.Cassandra.RowMapper
import java.nio.ByteBuffer
import org.reactivestreams.Publisher
import akka.persistence.cassandra.streams.rt.RealTime
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.persistence.cassandra.streams.rt.Chronology
import akka.persistence.cassandra.streams.SortedFilterDuplicate
import akka.persistence.cassandra.streams.FanoutAndMerge
import akka.stream.Materializer
import akka.persistence.query.EventEnvelope
import akka.persistence.cassandra.query.CassandraOps.IndexEntry

/**
 * Implementation of akka persistence read journal, for the akka-persistence-cassandra plugin
 * that is extended by writing to a time index table.
 *
 * @param realtimeIndex A publisher that publishes `IndexEntry` values whenever new index entries
 * appear in cassandra, in real-time (i.e. eventually, but in order, without duplicates and without skipped entries).
 */
class CassandraReadJournal(
    realtimeIndex: Publisher[IndexEntry],
    nowFunc: => Instant = clockNowFunc
)(implicit system: ActorSystem, m: Materializer) extends ReadJournal {
  def query[T, M](q: Query[T, M], hints: Hint*): Source[T, M] = {
    q match {
    	// TODO actually use the tag to query for events of a different type. Requires an extra column in cassandra.
      case EventsByTag("_all", offset) =>
        val (sink, source) = FanoutAndMerge(byPersistenceId, getEvents)

        // Combine past index entries and new, real-time ones into a single logical Source
        RealTime.source(pastIndex, realtimeIndex)

        // Filter out duplicate persistenceIds within the same time window
        .transform{ () => new SortedFilterDuplicate[IndexEntry,Instant,String](_.window_start)(_.persistenceId) }

        // Drop it into the fanout [sink] to fetch nested StoredEvent entries, so they come out merged at the other [source] end.
        .runWith(sink)

        source
    }
  }

  private def byPersistenceId(i:IndexEntry) = i.persistenceId

  /**
   * Queries cassandra for the given time window interval, once.
   */
  private def pastIndex(from: Instant, to: Instant): Source[IndexEntry,Unit] = ???

  /**
   * Queries the cassandra index, and then gets the actual events, for the given time window interval, once.
   */
  private def pastEvents(persistenceId:String)(fromTimeWindow: Long, toTimeWindow: Long): Source[EventEnvelope,Unit] = ???

  /**
   * Returns the publisher that emits real-time events for the given persistenceId.
   *
   * TODO Shut down publishers when the time window for their last seen real-time event has been closed.
   */
  private def realtimeEvents(persistenceId:String): Publisher[EventEnvelope] = ???

  /**
   * Returns a source that combines all past events for [entry.persistenceId] and then
   * turns to real-time.
   */
  private def getEvents(entry: IndexEntry): Source[EventEnvelope,Any] = {
    RealTime.source(pastEvents(entry.persistenceId), realtimeEvents(entry.persistenceId))
  }

  private implicit val indexChronology = new Chronology[IndexEntry,Instant] {
    def getTime(elem: IndexEntry) = elem.window_start
    def beginningOfTime = Instant.MIN
    def now = nowFunc
    def isBefore(a: Instant, b: Instant) = a.isBefore(b)
  }

  private implicit val eventChronology = new Chronology[EventEnvelope,Long] {
    def getTime(elem:EventEnvelope) = elem.offset
    def beginningOfTime = 0l
    def now = nowFunc.toEpochMilli()
    def isBefore(a: Long, b: Long) = a < b
  }
}


object CassandraReadJournal {

  def clockNowFunc = Instant.now()
}
