package akka.persistence.cassandra.streams

import akka.stream.stage.PushStage
import akka.stream.stage.Context
import akka.stream.stage.StatefulStage
import akka.stream.stage.PushPullStage

case class TakeWhileTupled2[T](predicate: (T,T) => Boolean) extends PushPullStage[T,T] {
  private var previous: Option[T] = None
  
  override def onPush(elem: T, ctx: Context[T]) = {
    previous match {
      case None =>
        previous = Some(elem)
        ctx.pull()
        
      case Some(p) if predicate(p, elem) =>
        previous = Some(elem)
        ctx.push(p)
        
      case Some(p) =>
        ctx.pushAndFinish(p)
    }
  }
  
  override def onUpstreamFinish(ctx: Context[T]) = {
    previous match {
      case Some(p) =>
        ctx.absorbTermination()
      case None =>
        ctx.finish()
    }
  }
  
  override def onPull(ctx: Context[T]) = {
    previous match {
      case Some(p) if ctx.isFinishing =>
        ctx.pushAndFinish(p)
      case _ =>
        ctx.pull()
    }
  }
}

