package akka.persistence.cassandra

import com.typesafe.config.Config
import akka.actor.ActorSystem

object CassandraReadJournalConfig {
  def apply(system: ActorSystem) = new CassandraReadJournalConfig (
    system.settings.config.getConfig("akka.persistence.query.journal.cassandra")
  )
}

case class CassandraReadJournalConfig(config: Config) {
  val extendedTimeWindowLength = config.getDuration("extended-time-window-length")
  val allowedClockDrift = config.getDuration("allowed-clock-drift")
  val selectRetries = config.getInt("select-retries")
  val execTimeout = config.getDuration("execute-timeout")
}
