package akka.persistence.cassandra.query

import akka.actor.ExtendedActorSystem
import com.typesafe.config.Config
import akka.persistence.query.scaladsl.ReadJournal
import akka.stream.scaladsl.Source
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
import akka.persistence.query.scaladsl.EventsByTagQuery
import akka.actor.Props

/**
 * Implementation of akka persistence read journal, for the akka-persistence-cassandra plugin
 * that is extended by writing to a time index table.
 *
 * @param realtimeIndex A publisher that publishes `IndexEntry` values whenever new index entries
 * appear in cassandra, in real-time (i.e. eventually, but in order, without duplicates and without skipped entries).
 */
class CassandraReadJournal(
    cassandraOps: CassandraOps,
    realtimeIndex: Source[IndexEntry,Any],
    nowFunc: => Instant = clockNowFunc
)(implicit system: ActorSystem, m: Materializer) extends EventsByTagQuery {

  override def eventsByTag(tag: String, offset: Long): Source[EventEnvelope, Unit] = {
   	// TODO actually use the tag to query for events of a different type. Requires an extra column in cassandra.
    val (sink, source) = FanoutAndMerge(byPersistenceId, getEvents)

    // Combine past index entries and new, real-time ones into a single logical Source
    RealTime.source(cassandraOps.pastIndex, realtimeIndex)

      // Filter out duplicate persistenceIds within the same time window
      .transform{ () => new SortedFilterDuplicate[IndexEntry,Instant,String](_.window_start)(_.persistenceId) }

      // Drop it into the fanout [sink] to fetch nested StoredEvent entries, so they come out merged at the other [source] end.
      .runWith(sink)

    source
  }

  private def byPersistenceId(i:IndexEntry) = i.persistenceId

  /**
   * Queries the cassandra index, and then gets the actual events, for the given time window interval, once.
   */
  private def pastEvents(persistenceId:String)(fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope,Unit] =
    cassandraOps.readEvents(persistenceId)(fromSequenceNr, toSequenceNr)

  /**
   * Manages the pool of sources that emit real-time events for a given persistenceId.
   */
  private val realtimeEvents = SourcePool { persistenceId: String =>
    Source.actorPublisher(Props(new PersistenceIdEventsPoller(cassandraOps, persistenceId)))
  }

  /**
   * Returns a source that combines all past events for [entry.persistenceId] and then
   * turns to real-time.
   */
  private def getEvents(entry: IndexEntry): Source[EventEnvelope,Any] = {
    RealTime.source(cassandraOps.readEvents(entry.persistenceId), realtimeEvents(entry.persistenceId))
  }

  private implicit val indexChronology = new Chronology[IndexEntry,Instant] {
    def getTime(elem: IndexEntry) = elem.window_start
    def beginningOfTime = Instant.MIN
    def endOfTime = nowFunc
    def isBefore(a: Instant, b: Instant) = a.isBefore(b)
  }

  private implicit val eventChronology = new Chronology[EventEnvelope,Long] {
    def getTime(elem:EventEnvelope) = elem.sequenceNr
    def beginningOfTime = 0l
    def endOfTime = Long.MaxValue
    def isBefore(a: Long, b: Long) = a < b
  }
}


object CassandraReadJournal {

  def clockNowFunc = Instant.now()
}
