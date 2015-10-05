package akka.persistence.cassandra.streams.rt

trait Chronology[Elem,Time] {
  def getTime(elem: Elem): Time
  def beginningOfTime: Time
  def endOfTime: Time
  def isBefore(a: Time, b: Time): Boolean
}
