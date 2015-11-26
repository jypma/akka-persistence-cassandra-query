package akka.persistence.cassandra.streams.rt

trait Chronology[Elem,Time] {
  def getTime(elem: Elem): Time
  def beginningOfTime: Time
  def endOfTime: Time
  /** Returns whether A is before B */
  def isBefore(a: Time, b: Time): Boolean
  def latest(a: Time, b: Time): Time = if (isBefore(a, b)) b else a 
}
