package akka.persistence.cassandra.streams

import akka.stream.stage.PushStage
import akka.stream.stage.Context
import akka.stream.stage.PushPullStage

object ConcatWhenEmpty {
  def apply[T](msg: T) = new PushPullStage[T,T] {
    var empty = true
    var pushed = false

    override def onPush(elem: T, ctx: Context[T]) = {
      empty = false
      ctx.push(elem)
    }

    override def onPull(ctx: Context[T]) = {
      if (ctx.isFinishing && empty) {
        if (pushed) {
          ctx.finish()
        } else {
          pushed = true
        	ctx.push(msg)
        }
      } else {
        ctx.pull()
      }
    }

    override def onUpstreamFinish(ctx: Context[T]) = {
      if (empty) {
        ctx.absorbTermination()
      } else {
        ctx.finish()
      }
    }
  }
}
