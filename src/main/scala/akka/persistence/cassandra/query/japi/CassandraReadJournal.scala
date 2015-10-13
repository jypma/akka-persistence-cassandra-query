package akka.persistence.cassandra.query.japi

import akka.persistence.query.javadsl.EventsByTagQuery
import akka.persistence.query.javadsl.ReadJournal
import akka.stream.javadsl.Source
import akka.persistence.query.EventEnvelope

/**
 * Implementation of akka persistence read journal, for the akka-persistence-cassandra plugin
 * that is extended by writing to a time index table.
 *
 * Java API
 */
class CassandraReadJournal(delegate: akka.persistence.cassandra.query.CassandraReadJournal) extends ReadJournal with EventsByTagQuery {
  def eventsByTag(tag: String, offset: Long): Source[EventEnvelope, Unit] = Source.adapt(delegate.eventsByTag(tag, offset))
}
