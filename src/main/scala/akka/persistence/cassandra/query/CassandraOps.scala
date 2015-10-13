package akka.persistence.cassandra.query

import java.nio.ByteBuffer
import java.time.Instant
import java.util.{ Calendar, TimeZone }
import akka.actor.ActorRef
import scala.collection.AbstractIterator
import akka.persistence.cassandra.Cassandra
import akka.persistence.cassandra.Cassandra.RowMapper
import akka.persistence.query.EventEnvelope
import akka.persistence.serialization.MessageFormats
import akka.stream.scaladsl.{ FlattenStrategy, Source }
import CassandraOps._
import akka.persistence.cassandra.streams.ConcatWhenEmpty
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

class CassandraOps(
  cassandra: Cassandra,
  tableName: String,
  metadataTableName: String,
  timeIndexTableName: String,
  targetPartitionSize: Int
) {
  retryWithin(30.seconds, 1.second) {
    // Once this query can run, our tables are created and ready.
    cassandra.prepareSelect(s"SELECT year_month_day FROM ${timeIndexTableName} WHERE year_month_day = 0")(r => r).execute()
  }


  /**
   * Queries cassandra for the given time window interval, once.
   */
  def pastIndex(from: Instant, to: Instant): Source[IndexEntry,Unit] = {
    println("*** indexing from " + from + " to " + to)

    val startDay = toYearMonthDay(from)
    val endDay = toYearMonthDay(to)

    Source(startDay.to(endDay))
      .map(day => entriesForDay(day, from, to))
      .flatten(FlattenStrategy.concat)
      .log("pastIndex from " + from + " to " + to)
  }

  private def entriesForDay(day: Int, from: Instant, to: Instant): Source[IndexEntry,Any] = {
    selectEventsForDay.execute(day, from, to)
  }

  def readIndexEntriesOnSameDaySince(start: Instant): Source[IndexEntry,Any] =
    selectEventsSince.execute(toYearMonthDay(start), start)

  private case object WasEmpty
  /**
   * Queries the cassandra index, and then gets the actual events, for the given time window interval, once.
   */
  def readEvents(persistenceId: String)(fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope,Unit] = {
    val startNr = math.max(highestDeletedSequenceNumber(persistenceId) + 1, fromSequenceNr)
    Source(() => from(partitionNr(startNr))).map { partitionNr =>
      selectMessages.execute(persistenceId, partitionNr, startNr).transform(() => ConcatWhenEmpty(WasEmpty))
    }.flatten(FlattenStrategy.concat).takeWhile(_ != WasEmpty).asInstanceOf[Source[EventEnvelope,Unit]]
  }

  def findHighestSequenceNr(persistenceId: String, fromSequenceNr: Long = 0) = {
    @annotation.tailrec
    def find(currentPnr: Long, currentSnr: Long): Long = {
      // if every message has been deleted and thus no sequence_nr the driver gives us back 0 for "null" :(
      selectHightestSequenceNr.executeBlocking(persistenceId, currentPnr).head match {
        // never been to this partition
        case None => currentSnr
        // don't currently explicitly set false
        case Some(SequenceNr(false, _)) => currentSnr
        // everything deleted in this partition, move to the next
        case Some(SequenceNr(true, 0)) => find(currentPnr+1, currentSnr)
        case Some(SequenceNr(_, nextHighest)) => find(currentPnr+1, nextHighest)
      }
    }
    find(partitionNr(fromSequenceNr), fromSequenceNr)
  }

  private def highestDeletedSequenceNumber(persistenceId: String): Long = {
    selectDeletedTo.executeBlocking(persistenceId).head.getOrElse(0)
  }

  private def partitionNr(sequenceNr: Long): Long =
    (sequenceNr - 1L) / targetPartitionSize

  private val selectHightestSequenceNr = cassandra.prepareSelect[SequenceNr](
      s"SELECT sequence_nr, used FROM ${tableName} WHERE persistence_id = ? AND partition_nr = ? ORDER BY sequence_nr DESC LIMIT 1")

  private val selectDeletedTo = cassandra.prepareSelect[Long](
      s"SELECT deleted_to FROM ${metadataTableName} WHERE persistence_id = ?") { _.getLong(0) }

  private val selectEventsSince = cassandra.prepareSelect[IndexEntry](
      s"SELECT * FROM ${timeIndexTableName} WHERE year_month_day = ? AND window_start >= ?")

  private val selectEventsForDay = cassandra.prepareSelect[IndexEntry](
      s"SELECT * FROM ${timeIndexTableName} WHERE year_month_day = ? AND window_start >= ? and window_start <= ?")

  private val selectMessages = cassandra.prepareSelect[EventEnvelope](
      s"SELECT * FROM ${tableName} WHERE persistence_id = ? AND partition_nr = ? AND sequence_nr >= ?",
      fetchSize = 100)
}

object CassandraOps {
  case class SequenceNr(used: Boolean, seqNr: Long)

  //case class StoredEvent(sequenceNr: Long, windowStart: Instant, msg: MessageFormats.PersistentMessage) {
  //  override def toString = s"StoredEvent(${sequenceNr}, ${windowStart}, ...)"
  //}

  case class IndexEntry(yearMonthDay: Int, window_start: Instant,
      persistenceId: String, firstSequenceNrInWindow: Long, partitionNr: Long)

  def toYearMonthDay(instant: Instant): Int = {
	  import Calendar._

    val cal = Calendar.getInstance
    cal.setTimeZone(TimeZone.getTimeZone("UTC"))
    cal.setTimeInMillis(instant.toEpochMilli())
    cal.get(YEAR) * 10000 + cal.get(MONTH) * 100 + cal.get(DAY_OF_MONTH)
  }

  implicit val indexEntryRowMapper: RowMapper[IndexEntry] = row => IndexEntry(
      row.getInt("year_month_day"),
      Instant.ofEpochMilli(row.getDate("window_start").getTime),
      row.getString("persistence_id"),
      row.getLong("first_sequence_nr_in_window"),
      row.getLong("partition_nr"))


  implicit val eventRowMapper: RowMapper[EventEnvelope] = { row =>
    val event = persistentFromByteBuffer(row.getBytes("message"))
    EventEnvelope(
        offset = row.getDate("window_start").getTime,
        persistenceId = row.getString("persistence_id"),
        sequenceNr = row.getLong("sequence_nr"),
        event = akka.util.ByteString(event.getPayload().getPayload().asReadOnlyByteBuffer()))
  }

  private def persistentFromByteBuffer(b: ByteBuffer): MessageFormats.PersistentMessage =
    MessageFormats.PersistentMessage.parseFrom(akka.protobuf.ByteString.copyFrom(b))

  private implicit val sequenceNrRowMapper: RowMapper[SequenceNr] = { row =>
    SequenceNr(row.getBool("used"), row.getLong("sequence_nr"))
  }

  implicit class ExtIterator[T](i: Iterator[T]) {
    /**
     * Returns the first element of the iterator, or None if the iterator is empty
     */
    def head: Option[T] = i.take(1).toList match {
      case Nil => None
      case x :: Nil => Some(x)
      case _ => throw new RuntimeException("This won't occur since we do take(1) above")
    }

    /**
     * Returns the first element of the iterator if it has exactly one element, None if the iterator is empty.
     * Throws exception if the iterator has more than one element.
     */
    def toOption: Option[T] = i.take(2).toList match {
      case Nil => None
      case x :: Nil => Some(x)
      case _ => throw new RuntimeException("More than 1 result but expected exactly one")
    }
  }

  private def from(start: Long, step: Long = 1): Iterator[Long] = new AbstractIterator[Long] {
    private var i = start
    def hasNext: Boolean = true
    def next(): Long = { val result = i; i += step; result }
  }

  private def retryWithin[T](timeout: FiniteDuration, delayBetweenRetries: FiniteDuration)(f: => T): T = {
    val deadline = timeout.fromNow
    var lastError: Option[Throwable] = None

    while (deadline.hasTimeLeft()) {
      try {
        return f
      } catch {
        case x: Throwable => lastError = Some(x)
      }
    }

    throw lastError.getOrElse(new RuntimeException("timeout"))
  }

}
