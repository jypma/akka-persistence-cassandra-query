package akka.persistence.cassandra.streams.rt

import akka.stream.stage.PushStage
import akka.stream.stage.Context

case class Deduplicate[Elem,Time]()(implicit ch:Chronology[Elem,Time]) extends PushStage[Elem,Elem] {
  def maxBufferSize = 1000
  var lastTime:Option[Time] = None
  val seen = collection.mutable.Set.empty[Elem]

  override def onPush(elem:Elem, ctx:Context[Elem]) = {
    if (lastTime.isEmpty || lastTime.get != ch.getTime(elem)) {
      // time has changed since last element
      lastTime = Some(ch.getTime(elem))
      seen.clear()
      seen.add(elem)
      ctx.push(elem)
    } else {
      if (seen.contains(elem)) {
        // we've seen this element for this time key
        ctx.pull()
      } else {
        // first time this element occurs for this time key
        seen.add(elem)
        if (seen.size > maxBufferSize) {
          throw new RuntimeException ("Buffer size " + maxBufferSize + " exceeded")
        }
        ctx.push(elem)
      }
    }
  }
}

