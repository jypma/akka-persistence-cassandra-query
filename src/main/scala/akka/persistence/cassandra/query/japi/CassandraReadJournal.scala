package akka.persistence.cassandra.query.japi

import akka.persistence.query.javadsl.EventsByTagQuery
import akka.persistence.query.javadsl.ReadJournal
import akka.stream.javadsl.Source
import akka.persistence.query.EventEnvelope
import akka.persistence.query.PersistenceQuery
import akka.actor.ActorSystem
import akka.persistence.cassandra.query.{CassandraReadJournal => ScalaApi}

/**
 * Implementation of akka persistence read journal, for the akka-persistence-cassandra plugin
 * that is extended by writing to a time index table.
 *
 * Java API
 */
class CassandraReadJournal(delegate: ScalaApi) extends ReadJournal with EventsByTagQuery {
  def eventsByTag(tag: String, offset: Long): Source[EventEnvelope, Unit] = Source.adapt(delegate.eventsByTag(tag, offset))
}

object CassandraReadJournal {
  def get(system: ActorSystem): CassandraReadJournal = PersistenceQuery.get(system).getReadJournalFor(
    classOf[CassandraReadJournal], ScalaApi.identifier)
}
