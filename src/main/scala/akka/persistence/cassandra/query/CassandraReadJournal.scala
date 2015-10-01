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

class CassandraReadJournal(nowFunc: => Instant = clockNowFunc)(implicit system: ActorSystem, m: Materializer) extends ReadJournal {
  def query[T, M](q: Query[T, M], hints: Hint*): Source[T, M] = {
    q match {
    	// TODO actually use the tag to query for events of a different type. Requires an extra column in cassandra.
      case EventsByTag("_all", offset) =>
        val (sink, source) = FanoutAndMerge(byPersistenceId, getEvents)

        RealTime.source(pastIndex, realtimeIndex)

        // filter out duplicate persistenceIds within the same time window
        .transform{ () => new SortedFilterDuplicate[IndexEntry,Instant,String](_.window_start)(_.persistenceId) }

        // drop it into the realtime [sink], so merged StoredEvents come out the other [source] end.
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
   * Periodically polls for new index entries
   */
  private val realtimeIndex: Publisher[IndexEntry] = ???

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
  case class StoredEvent(sequenceNr: Long, windowStart: Instant, msg: MessageFormats.PersistentMessage) {
    override def toString = s"StoredEvent(${sequenceNr}, ${windowStart}, ...)"
  }

  case class IndexEntry(yearMonthDay: Int, window_start: Instant,
      persistenceId: String, firstSequenceNrInWindow: Long, partitionNr: Long)

  implicit val indexEntryRowMapper: RowMapper[IndexEntry] = row => IndexEntry(
      row.getInt("year_month_day"),
      Instant.ofEpochMilli(row.getDate("window_start").getTime),
      row.getString("persistence_id"),
      row.getLong("first_sequence_nr_in_window"),
      row.getLong("partition_nr"))


  implicit val storedEventRowMapper: RowMapper[StoredEvent] = { row =>
    val event = persistentFromByteBuffer(row.getBytes("message"))
    StoredEvent(row.getLong("sequence_nr"), Instant.ofEpochMilli(row.getDate("window_start").getTime), event)
  }

  private def persistentFromByteBuffer(b: ByteBuffer): MessageFormats.PersistentMessage =
    MessageFormats.PersistentMessage.parseFrom(akka.protobuf.ByteString.copyFrom(b))

  def clockNowFunc = Instant.now()
}
