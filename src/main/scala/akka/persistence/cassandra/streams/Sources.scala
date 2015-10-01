package akka.persistence.cassandra.streams

import akka.stream.scaladsl.Source
import akka.stream.scaladsl.FlowGraph
import akka.stream.scaladsl.Merge
import akka.stream.scaladsl.Keep

object Sources {
  /**
   * Returns a new source that merges the two given sources, using [combineMat] to pick which materialized
   * value to use for the returned source.
   */
  def merged[T,M1,M2,Mat](source1:Source[_ <: T,M1], source2:Source[_ <: T,M2])(combineMat: (M1, M2) ⇒ Mat): Source[T,Mat] = {
    Source(source1, source2)(combineMat) { implicit b =>
      (in1, in2) =>
        import FlowGraph.Implicits._

        val merge = b.add(Merge[T](2))
        in1 ~> merge.in(0)
        in2 ~> merge.in(1)

        merge.out
    }

  }
}
