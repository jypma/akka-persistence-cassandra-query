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

/**
 * Implementation of akka persistence read journal, for the akka-persistence-cassandra plugin
 * that is extended by writing to a time index table.
 *
 * Scala API.
 */
class CassandraReadJournal(
    system: ExtendedActorSystem,
    config: Config
) extends ReadJournalProvider with ReadJournal with EventsByTagQuery {
  import system.dispatcher

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

  private val realtimeActor = system.actorOf(Props(new IndexEntryPoller(cassandraOps)))
  private val realtimeIndex = Source.actorRef(16, OverflowStrategy.fail).mapMaterializedValue { actor =>
    realtimeActor.tell(IndexEntryPoller.Subscribe, actor)
  }.log("realtimeIndex")

  /**
   * Manages the pool of sources that emit real-time events for a given persistenceId.
   */
  private val realtimeEvents = SourcePool(PersistenceIdEventsPoller.Subscribe, 256) { persistenceId: String =>
    Props(new PersistenceIdEventsPoller(cassandraOps, persistenceId))
  }

  /**
   * Returns ALL events added to the journal that implement Timestamped, where their
   * timestamp falls into the same or later time window as [offset].
   * 
   * The returned `EventEnvelope` items have their `offset` set to the event's timestamp,
   * and `payload` set to an instance of {@link EventPayload}
   */
  override def eventsByTag(tag: String, offset: Long): Source[EventEnvelope, Unit] = {
    //FIXME uhm we should be using offset somewhere???

    val (sink, source) = FanoutAndMerge(byPersistenceId, getEvents)

    // Combine past index entries and new, real-time ones into a single logical Source
    RealTime(cassandraOps.pastIndex, realtimeIndex)

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
   * Returns a source that combines all past events for [entry.persistenceId] and then
   * turns to real-time.
   */
  private def getEvents(entry: IndexEntry): Source[EventEnvelope,Any] = {
    RealTime(cassandraOps.readEvents(entry.persistenceId), realtimeEvents(entry.persistenceId))
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
