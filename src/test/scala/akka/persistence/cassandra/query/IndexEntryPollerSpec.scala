package akka.persistence.cassandra.query

import akka.persistence.cassandra.MockitoOps
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.WordSpec
import akka.persistence.cassandra.test.SharedActorSystem
import org.scalatest.Matchers
import akka.testkit.TestProbe
import akka.actor.Actor
import akka.actor.Props
import java.util.concurrent.atomic.AtomicInteger
import akka.actor.ActorRef
import akka.testkit.TestKit
import scala.concurrent.duration._
import org.scalatest.time.Seconds
import org.scalatest.time.Span

class IndexEntryPollerSpec extends WordSpec with Matchers with ScalaFutures with Eventually with SharedActorSystem {
  implicit val patience = PatienceConfig(timeout = Span(2, Seconds))
  
  "IndexEntryPoller" when {
    "handling failure in its worker" should {
      val instantiations = new AtomicInteger
      
      class FailingWorker extends Actor {
        instantiations.incrementAndGet()
        
        def receive = {
          case _ =>
            Thread.sleep(100)
            throw new RuntimeException("Simulated failure")
        }
      }
      
      class Subscriber(poller: ActorRef) extends Actor {
        poller ! IndexEntryPoller.Subscribe 
        def receive = { case _ => }
      }
      
      "stop all subscribers and restart the worker" in {
        val poller = system.actorOf(Props(new IndexEntryPoller(Props(new FailingWorker), restartDelay = 100.milliseconds)))
        val subscriber = system.actorOf(Props(new Subscriber(poller)))
        val observer = TestProbe()
        observer.watch(subscriber)
        observer.expectTerminated(subscriber, 1.second)
        
        eventually {
          instantiations.get should be > 1
        }
        
        system.stop(poller)
      }
    }
    
    "receiving messages" should {
      class Worker(delegate: ActorRef) extends Actor {
        def receive = {
          case msg =>
            delegate forward msg
        }
      }
      
      "forward subscriptions to its worker" in {
        val worker = TestProbe()
        val poller = system.actorOf(Props(new IndexEntryPoller(Props(new Worker(worker.ref)))))
        val subscriber = TestProbe()
        
        subscriber.send(poller, IndexEntryPoller.Subscribe)
        worker.expectMsg(IndexEntryPoller.Subscribe)
        worker.sender() should be (subscriber.ref)
        
        system.stop(poller)
      }
    }
  }
}
