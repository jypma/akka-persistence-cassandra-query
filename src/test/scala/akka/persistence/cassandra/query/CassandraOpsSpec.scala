package akka.persistence.cassandra.query

import akka.persistence.cassandra.test.SharedActorSystem
import org.scalatest.Matchers
import org.scalatest.WordSpec
import akka.persistence.cassandra.Cassandra
import akka.persistence.cassandra.Cassandra.PreparedSelectStatement
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.Sink
import org.scalatest.concurrent.Futures
import org.scalatest.concurrent.ScalaFutures
import java.time.Instant
import akka.persistence.cassandra.query.CassandraOps.IndexEntry
import akka.persistence.query.EventEnvelope
import org.scalatest.time.Seconds
import org.scalatest.time.Span
import akka.testkit.TestProbe
import scala.concurrent.duration.DurationInt 

class CassandraOpsSpec extends WordSpec with Matchers with ScalaFutures with SharedActorSystem {
  implicit val patience = PatienceConfig(timeout = Span(2, Seconds))

  import CassandraOpsSpec._

  class Fixture (
      index: Seq[IndexEntry] = Seq.empty,
      events: Seq[EventEnvelope] = Seq.empty
    ) {
    val partitionSize = 5

    def getPartitionNr(e: EventEnvelope) = (e.sequenceNr - 1) / partitionSize

    val cassandra = new Cassandra {
      override def prepareSelect[T: Cassandra.RowMapper](cql: String, fetchSize: Int = 0): PreparedSelectStatement[T] = cql match {
        case "SELECT year_month_day FROM timeIndex WHERE year_month_day = 0" =>
          mockStatement {
            case _ =>
              Seq.empty
          }
        case "SELECT * FROM timeIndex WHERE year_month_day = ? AND window_start >= ? and window_start <= ?" =>
          mockStatement {
            case (ymd: Int) :: (from: Instant) :: (to: Instant) :: Nil =>
              index.filter(i => i.yearMonthDay == ymd && !i.window_start.isBefore(from) && !i.window_start.isAfter(to))
          }
        case "SELECT sequence_nr, used FROM messages WHERE persistence_id = ? AND partition_nr = ? ORDER BY sequence_nr DESC LIMIT 1" =>
          mockStatement {
            case (persistenceId: String) :: (partitionNr: Long) :: Nil =>
              events.filter(e => e.persistenceId == persistenceId && getPartitionNr(e) == partitionNr).map { e =>
                CassandraOps.SequenceNr(true, e.sequenceNr)
              }.lastOption.toSeq
          }
        case "SELECT deleted_to FROM metadata WHERE persistence_id = ?" =>
          mockStatement {
            case (persistenceId: String) :: Nil =>
              Seq.empty
          }
        case "SELECT * FROM timeIndex WHERE year_month_day = ? AND window_start >= ?" =>
          mockStatement {
            case (ymd: Int) :: (from: Instant) :: Nil =>
              index.filter(i => i.yearMonthDay == ymd && !i.window_start.isBefore(from))
          }
        case "SELECT * FROM messages WHERE persistence_id = ? AND partition_nr = ? AND sequence_nr >= ?" =>
          mockStatement {
            case (persistenceId: String) :: (partitionNr: Long) :: (sequenceNr: Long) :: Nil =>
              events.filter(e => e.persistenceId == persistenceId && getPartitionNr(e) == partitionNr && e.sequenceNr >= sequenceNr)
          }
      }
    }

    val ops = new CassandraOps(system, cassandra, "messages", "metadata", "timeIndex", targetPartitionSize = partitionSize)
  }

  "CassandraOps.readEvents" when {
    "having events stored across several partitions" should {
      "read all events and then complete" in new Fixture(
          index = Seq(IndexEntry(20151013, Instant.ofEpochSecond(1444727657l), 60000, "doc-1", 1, 0)),
          events = Seq(EventEnvelope(1444727657l, "doc-1", 1, 1),
                       EventEnvelope(1444727657l, "doc-1", 2, 2),
                       EventEnvelope(1444727657l, "doc-1", 3, 3),
                       EventEnvelope(1444727657l, "doc-1", 4, 4),
                       EventEnvelope(1444727657l, "doc-1", 5, 5),
                       EventEnvelope(1444727657l, "doc-1", 6, 6),
                       EventEnvelope(1444727657l, "doc-1", 7, 7),
                       EventEnvelope(1444727657l, "doc-1", 8, 8),
                       EventEnvelope(1444727657l, "doc-1", 9, 9),
                       EventEnvelope(1444727657l, "doc-1", 10, 10))
        ) {
        val result = ops.readEvents("doc-1")(0, Long.MaxValue).runWith(toSequence).futureValue

        result should have size(10)
      }
    }
    
    "encountering gaps in the sequence number for read events, due to a rebalanced journal's write arriving out-of-order" should {
      // when a journal writer gets rebalanced to a different node, writes from the new node could technically reach
      // the query client before the original node's writes. In such a situation, the best thing to do is ignore the
      // messages after the gap, assuming that the other write will eventually make it to the node. We're pretty much
      // polling from all calls to .readEvents() anyways.
      
      "stop reading when it sees a gap" in new Fixture(
          index = Seq(IndexEntry(20151013, Instant.ofEpochSecond(1444727657l), 60000, "doc-1", 1, 0)),
          events = Seq(EventEnvelope(1444727657l, "doc-1", 1, 1),
                       EventEnvelope(1444727657l, "doc-1", 3, 3))
        ) {
        val result = ops.readEvents("doc-1")(0, Long.MaxValue).runWith(toSequence).futureValue
        result should be (Seq(EventEnvelope(1444727657l, "doc-1", 1, 1)))
      }
    }
  }
}

object CassandraOpsSpec {
  def mockStatement[T](f: PartialFunction[List[Any], Iterable[Any]]): PreparedSelectStatement[T] = {
    val fail:PartialFunction[List[Any],Nothing] = { case args => throw new MatchError(s"Unhandled mock cql with arguments ${args}") }
    val exec = f orElse fail

    new PreparedSelectStatement[Any] {
      def execute(args: Any*) = Source(executeBlocking(args: _*).toList)
      def executeBlocking(args: Any*) = f(args.toList).iterator
    }.asInstanceOf[PreparedSelectStatement[T]]
  }

  def toSequence[T] = Sink.fold[Seq[T],T](Seq.empty)((seq, elem) => seq :+ elem)


}
