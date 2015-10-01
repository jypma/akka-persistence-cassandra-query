package akka.persistence.cassandra.streams

import akka.stream.stage.PushStage
import akka.stream.stage.Context
import akka.stream.stage.SyncDirective

/**
 * For a stream that is sorted by key K (subsequent items will have the same key K), will
 * only allow the first item through for each unique key of type D. After an element arrives
 * with a new key K, the filter is reset.
 */
class SortedFilterDuplicate[T,K,D](keyFunction: T => K)(duplicateFunction: T => D) extends PushStage[T, T] {
  var prevKey: Option[K] = None
  val seen = collection.mutable.Set.empty[D]

  override def onPush(elem: T, ctx: Context[T]): SyncDirective = {
    val elemKey = keyFunction(elem)
    if (prevKey.map(_ != elemKey).getOrElse(true)) {
      seen.clear()
    }

    prevKey = Some(elemKey)
    val first:Boolean = seen.add(duplicateFunction(elem))
    if (first)
      ctx.push(elem)
    else
      ctx.pull()
  }

}
