package akka.persistence.cassandra.test

import akka.persistence.cassandra.Cassandra.PreparedSelectStatement
import akka.stream.scaladsl.Source

trait MockCassandra {
  def mockStatement[T](f: PartialFunction[List[Any], Iterable[Any]]): PreparedSelectStatement[T] = {
    val fail:PartialFunction[List[Any],Nothing] = { case args => throw new MatchError(s"Unhandled mock cql with arguments ${args}") }
    val exec = f orElse fail

    new PreparedSelectStatement[Any] {
      def execute(args: Any*) = Source(executeBlocking(args: _*).toList)
      def executeBlocking(args: Any*) = f(args.toList).iterator
    }.asInstanceOf[PreparedSelectStatement[T]]
  }
}
