package akka.persistence.cassandra.streams

import akka.stream.scaladsl.Source
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.FlowGraph
import akka.stream.scaladsl.MergePreferred
import akka.stream.stage.StatefulStage
import akka.stream.stage.Context
import akka.actor.ActorRef
import akka.stream.scaladsl.FlattenStrategy

object Sequential {
  /**
   * Reads a sequence of sources sequentially, until an empty source is encountered.
   */
  def forEachUntilEmpty[I,T](initialValue: I)(increment: I => I)(factory: I => Source[T,Any]): Source[T,Unit] = {
    val src = Source.actorRef[I](4, OverflowStrategy.fail).map { i =>
      factory(i) concat Source.single(SourceCompleted)
    }.flatten(FlattenStrategy.concat)

    val withMat = Source(src) { implicit b =>
      in =>
        import FlowGraph.Implicits._
        val merge = b.add(MergePreferred[Any](1))

        b.materializedValue ~> merge.preferred
        in ~> merge.in(0)
        merge.out
    }

    withMat.transform(() => new StatefulStage[Any,T] {
      def initial = new State {
        def onPush(msg: Any, ctx: Context[T]) = msg match {
          case head:ActorRef =>
            become(reading(head, initialValue))
            ctx.pull()
        }
      }

      def reading(head: ActorRef, i: I): State = {
        head ! i

        new State {
          var empty = true

          def onPush(msg: Any, ctx: Context[T]) = msg match {
            case SourceCompleted =>
              if (empty) {
                ctx.finish()
              } else {
                become(reading(head, increment(i)))
                ctx.pull()
              }

            case elem =>
              empty = false
              ctx.push(elem.asInstanceOf[T]) // must be T, since we only injected SourceCompleted, which we filtered out
          }
        }
      }
    }).mapMaterializedValue { _ => Unit }
  }

  private case object SourceCompleted
}
