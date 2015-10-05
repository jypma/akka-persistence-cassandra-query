package akka.persistence.cassandra.streams.rt

import akka.stream.stage.PushStage
import akka.stream.stage.StatefulStage
import akka.stream.stage.Context

case class OnComplete[T](completeMsg:T) extends StatefulStage[T,T] {
  def initial = new State {
    override def onPush(elem: T, ctx: Context[T]) = ctx.push(elem)
  }

  override def onUpstreamFinish(ctx: Context[T]) = terminationEmit(Seq(completeMsg).iterator, ctx)
}
