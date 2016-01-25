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
import akka.persistence.query.PersistenceQuery
import akka.persistence.cassandra.journal.CassandraJournalConfig
import akka.persistence.cassandra.Cassandra
import akka.stream.ActorMaterializer
import akka.persistence.query.ReadJournalProvider
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Keep
import scala.concurrent.Future
import akka.persistence.cassandra.streams.Reaper
import akka.stream.OverflowStrategy
import com.typesafe.scalalogging.StrictLogging
import scala.concurrent.duration.DurationInt
import akka.persistence.cassandra.CassandraReadJournalConfig

/**
 * Implementation of akka persistence read journal, for the akka-persistence-cassandra plugin
 * that is extended by writing to a time index table.
 *
 * Scala API.
 */
class CassandraReadJournal(
    system: ExtendedActorSystem,
    cfg: Config
) extends ReadJournalProvider with ReadJournal with EventsByTagQuery with StrictLogging {
  import system.dispatcher

  val config= CassandraReadJournalConfig(cfg)
  override def scaladslReadJournal() = this

  override def javadslReadJournal() = new japi.CassandraReadJournal(this)

  def now: Instant = Instant.now()

  private implicit val s = system
  private implicit val m = ActorMaterializer()

  private val journalConfig = new CassandraJournalConfig(system.settings.config.getConfig("cassandra-journal"))

  private val cassandraOps = new CassandraOps(system, Cassandra(system),
      s"${journalConfig.keyspace}.${journalConfig.table}",
      s"${journalConfig.keyspace}.${journalConfig.metadataTable}",
      s"${journalConfig.keyspace}.${journalConfig.timeIndexTable}",
      journalConfig.targetPartitionSize)

  private val realtimeActorWorker = Props(new IndexEntryPollerWorker(cassandraOps, 
    config.extendedTimeWindowLength, config.pollDelay, config.indexPollerQueueSize))
  private val realtimeActor = system.actorOf(Props(new IndexEntryPoller(realtimeActorWorker)))
  private val realtimeIndex = Source.actorRef(16, OverflowStrategy.fail).mapMaterializedValue { actor =>
    realtimeActor.tell(IndexEntryPoller.Subscribe, actor)
  }

  /**
   * Manages the pool of sources that emit real-time events for a given persistenceId.
   */
  private val realtimeEvents = SourcePool(PersistenceIdEventsPoller.Subscribe, 256) { persistenceId: String =>
    PersistenceIdEventsPoller.props(cassandraOps, persistenceId, 
      config.extendedTimeWindowLength, config.pollDelay, config.eventsPollerQueueSize)
  }

  /**
   * Returns ALL events added to the journal that implement Timestamped, where their
   * timestamp falls into the same or later time window as [offset].
   * 
   * The returned `EventEnvelope` items have their `offset` set to the event's timestamp,
   * and `payload` set to an instance of {@link EventPayload}
   */
  
  override def eventsByTag(tag: String, offset: Long): Source[EventEnvelope, Unit] = {
    val start = indexChronology.latest(
      indexChronology.beginningOfTime, 
      Instant.ofEpochMilli(offset) minus journalConfig.timeWindowLength)
    
    val (sink, source) = FanoutAndMerge(byTimeWindow, getEvents)

    // Combine past index entries and new, real-time ones into a single logical Source
    RealTime(cassandraOps.pastIndex, realtimeIndex, start)

      // Filter out duplicate persistenceIds within the same time window
      .transform{ () => new SortedFilterDuplicate[IndexEntry,Instant,String](_.window_start)(_.persistenceId) }

      // Drop it into the fanout [sink] to fetch nested StoredEvent entries, so they come out merged at the other [source] end.
      .runWith(sink)

    source
  }

  private def byTimeWindow(i:IndexEntry) = i.window_start

  /**
   * Queries the cassandra index, and then gets the actual events, for the given time window interval, once.
   */
  private def pastEvents(persistenceId:String)(fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope,Unit] =
    cassandraOps.readEvents(persistenceId)(fromSequenceNr, toSequenceNr)

  /**
   * Returns a source that returns the events made for the time window of the given
   * index entry. If that time window might still be open, the source will pause and become real-time,
   * until the time window is no longer open. If the time window is historic, the source will  
   * just complete after returning all the events.
   */
  private def getEvents(entry: IndexEntry): Source[EventEnvelope,Any] = {
    val remaining = entry.remainingTimeAt(now.minus(config.allowedClockDrift))
    
    if (remaining.isDefined) {
      RealTime(
        cassandraOps.readEvents(entry.persistenceId), 
        realtimeEvents(entry.persistenceId), 
        entry.firstSequenceNrInWindow
      )
      .takeWithin(remaining.get)
    } else {
      cassandraOps.readEvents(entry.persistenceId)(entry.firstSequenceNrInWindow, eventChronology.endOfTime)
    }
    .takeWhile(entry.isEventInTimeWindow(_))
  }

  /**
   * Starts shutting down global services for this CassandraReadJournal. Actual shutdown is asynchronous.
   */
  def shutdown(): Future[Unit] = {
    system.stop(realtimeActor)
    realtimeEvents.shutdown() zip Reaper(realtimeActor) map (_ => Unit)
  }

  private implicit val indexChronology = new Chronology[IndexEntry,Instant] {
    def getTime(elem: IndexEntry) = elem.window_start
    def beginningOfTime = Instant.ofEpochSecond(1444435200) // there are no entries before Oct. 10, 2015
    def endOfTime = now
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
  /**
   *  The argument you should give to <pre>PersistenceQuery(system).readJournalFor</pre> in order to
   *  get an instance of CassandraReadJournal
   */
  val identifier = "akka.persistence.query.journal.cassandra"

  def instance(implicit system: ActorSystem):CassandraReadJournal = PersistenceQuery(system).readJournalFor(identifier)
}
