package akka.persistence.cassandra

import akka.stream.actor.ActorPublisher
import com.datastax.driver.core.Row
import ResultSetActorPublisher._
import akka.stream.actor.ActorPublisherMessage._
import com.datastax.driver.core.ResultSet
import akka.pattern.pipe
import akka.actor.Props
import scala.concurrent.Future
import akka.actor.Stash
import akka.stream.scaladsl.Source
import Cassandra.RowMapper
import akka.actor.ActorLogging
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeoutException
import com.datastax.driver.core.ResultSetFuture
import java.util.concurrent.TimeUnit

object ResultSetActorPublisher {
  def source[T](resultSet: ResultSetFuture, config: CassandraReadJournalConfig, rowMapper: RowMapper[T]) =
    Source.actorPublisher[T](Props(new ResultSetActorPublisher(resultSet, config, rowMapper)))

  private case object RowsFetched
  private case class ResultSetReady(resultSet: ResultSet)
  private case object ExecTimeout
}

/**
 * An ActorPublisher that turns a Cassandra ResultSet into a flow of events, so it can serve as a source,
 * by invoking Source.actorPublisher(...).
 */
class ResultSetActorPublisher[T](
    resultSetFuture: ResultSetFuture,
    config: CassandraReadJournalConfig,
    implicit val rowMapper: RowMapper[T]
) extends ActorPublisher[T] with Stash with ActorLogging {

  import context.dispatcher
  val execTimeout = Duration(config.execTimeout.toMillis(), TimeUnit.MILLISECONDS)

  resultSetFuture map ResultSetReady pipeTo self
  val timeoutSchedule = context.system.scheduler.scheduleOnce(execTimeout, self, ExecTimeout)

  def receive = {
    case ResultSetReady(resultSet) =>
      timeoutSchedule.cancel()
      log.debug("Resultset is ready with {} available results, exhausted: {}",
          resultSet.getAvailableWithoutFetching, resultSet.isExhausted)
      context become ready(resultSet)
      deliver(resultSet)
      unstashAll()
      
    case ExecTimeout =>
      resultSetFuture.cancel(true)
      onErrorThenStop(new TimeoutException(s"Timed out after ${execTimeout} while waiting for query to start executing"))         

    case Request(_) =>
      // ignore for now

    case other =>
      stash()
  }

  private def ready(resultSet: ResultSet): Receive = {
    case Request(_) =>
      deliver(resultSet)

    case Cancel =>
      context.stop(self)

    case RowsFetched =>
      deliver(resultSet)
  }

  private def deliver(resultSet: ResultSet): Unit = {
    log.debug("delivering, {} available, exhausted: {}, demand: {}, completed: {}", resultSet.getAvailableWithoutFetching, resultSet.isExhausted, totalDemand, isCompleted)
    if (resultSet.isExhausted()) {
      if (!isCompleted) {
        onCompleteThenStop()
      }
    } else {
      if (totalDemand > 0) {
        resultSet.getAvailableWithoutFetching() match {
          case 0 =>
            log.info("Demanded more results, but no available. Fetching.")
            resultSet.fetchMoreResults() map (_ => RowsFetched) pipeTo self
          case availableRowCount =>
            val demand: Int = if (totalDemand < Int.MaxValue) totalDemand.toInt else Int.MaxValue
            log.debug("Have {} rows available, demand is {}", availableRowCount, demand)
            (1 to Math.min(availableRowCount, demand)) foreach { _ =>
              val elem = resultSet.one(): T
              onNext(elem)
            }
            deliver(resultSet)
        }
      }
    }
  }

}
