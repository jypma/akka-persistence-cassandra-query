package akka.persistence.cassandra.streams

import akka.stream.stage.PushStage
import akka.stream.stage.Context

object WhenComplete {
  /**
   * Executes a callback when a stream completes (synchronously, i.e. the actual stage won't complete until the callback returns)
   */
  def apply[T,U](callback: => U) = new PushStage[T,T] {
    override def onPush(elem: T, ctx: Context[T]) = ctx.push(elem)

    override def onUpstreamFinish(ctx: Context[T]) = {
      callback
      ctx.finish()
    }
  }
}
