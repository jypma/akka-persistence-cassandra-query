package akka.persistence.cassandra

import akka.stream.scaladsl.Source
import akka.persistence.cassandra.journal.CassandraJournalConfig
import akka.actor.{ActorRef, ActorSystem}
import com.datastax.driver.core.Row
import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.concurrent.blocking

trait Cassandra {
  import Cassandra._

  def prepareSelect[T: RowMapper](cql: String, fetchSize: Int = 0): PreparedSelectStatement[T]

  def close() {}
}

object Cassandra {
  type RowMapper[T] = Row => T

  trait PreparedStatement {}

  trait PreparedSelectStatement[T] extends PreparedStatement {
    def execute(args: Any*): Source[T, ActorRef]

    def executeBlocking(args: Any*): Iterator[T]
  }

  /**
   * Creates a cassandra instance that uses the same settings as the akka-persistence-cassandra plugin
   * for the given actor system.
   */
  def apply(system: ActorSystem) = new Cassandra {
    import system.dispatcher

    val config = new CassandraJournalConfig(system.settings.config.getConfig("cassandra-journal"))
    val cluster = config.clusterBuilder.build
    val session = cluster.connect()

    override def prepareSelect[T: RowMapper](cql: String, fetchSize: Int = 0) = new PreparedSelectStatement[T] {
      val preparedStmt = session.prepare(cql).setConsistencyLevel(config.readConsistency)

      def mkStatement(args: Seq[Any]) = {
        val stmt = preparedStmt.bind(args.map(_.asInstanceOf[AnyRef]) : _*)
        if (fetchSize > 0) {
          stmt.setFetchSize(fetchSize)
        }
        stmt
      }

      override def execute(args: Any*): Source[T, ActorRef] = {
    	  ResultSetActorPublisher.source(session.executeAsync(mkStatement(args)), implicitly[RowMapper[T]])
      }

      override def executeBlocking(args: Any*): Iterator[T] = blocking {
        session.execute(mkStatement(args)).asScala.iterator.map(implicitly[RowMapper[T]])
      }
    }
  }
}

