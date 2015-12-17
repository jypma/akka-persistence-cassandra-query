package akka.persistence.cassandra

import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.WordSpec
import akka.persistence.cassandra.test.SharedActorSystem
import org.scalatest.Matchers
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.actor.Props
import com.datastax.driver.core.ResultSetFuture
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.util.concurrent.TimeoutException
import java.util.concurrent.Executor

class ResultSetActorPublisherSpec extends WordSpec with Matchers with ScalaFutures with Eventually with MockitoOps 
  with SharedActorSystem {
  def toSequence[T] = Sink.fold[Seq[T],T](Seq.empty)((seq, elem) => seq :+ elem)
  
  "ResultSetActorPublisher" when {
    "created with a future that does not complete" should {
      "blow up with a timeout error" in {
        val config = CassandraReadJournalConfig(ConfigFactory.parseString("""
          execute-timeout = 10 milliseconds
        """).withFallback(ConfigFactory.load().getConfig("akka.persistence.query.journal.cassandra")))
        
        // no need to mock any methods, since the only thing invoked on the future will be addListener
        val future = mock[ResultSetFuture]
        val source = ResultSetActorPublisher.source(future, config, row => 0)
        val x = the [TimeoutException] thrownBy Await.result(source.runWith(toSequence), 1.second)
        
        verify(future).addListener(?, ?)
        x.getMessage should endWith ("while waiting for query to start executing")
      }
    }
  }
}
