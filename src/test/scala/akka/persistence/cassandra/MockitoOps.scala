package akka.persistence.cassandra

import org.mockito.Mockito
import org.mockito.Mockito.verify
import org.mockito.Matchers
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import org.mockito.verification.VerificationMode

trait MockitoOps {
  implicit val postfixOps = language.postfixOps
  
  /**
   * Returns a mock of the given type
   */
  def mock[T](implicit tag:TypeTag[T]): T = Mockito.mock(tag.mirror.runtimeClass(tag.tpe.typeSymbol.asClass)).asInstanceOf[T]
  
  /**
   * A matcher that matches any object instance or boxed value.
   * 
   * This method does require a box/unbox operation for primitives so it isn't particularly fast, 
   * but it does allow for very convenient stubbing, and for that performance isn't so important.
   */
  def ?[T](implicit tag:TypeTag[T]): T = (typeOf[T] match {
    case t if t =:= typeOf[Boolean] =>
      Matchers.anyBoolean()
    case t if t =:= typeOf[Byte] =>
      Matchers.anyByte()
    case t if t =:= typeOf[Char] =>
      Matchers.anyChar()
    case t if t =:= typeOf[Double] =>
      Matchers.anyDouble()
    case t if t =:= typeOf[Float] =>
      Matchers.anyFloat()
    case t if t =:= typeOf[Int] =>
      Matchers.anyInt()
    case t if t =:= typeOf[Long] =>
      Matchers.anyLong()
    case t if t <:< typeOf[AnyRef] =>
      Matchers.any()
  }).asInstanceOf[T]
    
  /**
   * A matcher that matches any object instance or boxed value.
   */
  def any[T] = Matchers.any()

  implicit class ExactOps[T](value: T) {
    /**
     * A matcher that matches the given value exactly.
     */
    def := : T = Matchers.eq(value)    
  }
  
  /**
   * Starts a mockito verification given a mock.
   */
  def verify[T] (mock: T): T = Mockito.verify(mock)
  
  /**
   * Starts a mockito verification given a mock.
   */
  def verify[T] (mock: T, mode: VerificationMode): T = Mockito.verify(mock, mode)
  
  /**
   * Starts a mockito subbing invocation.
   */
  def when[T] (invocation: T) = Mockito.when(invocation)
  
  def atLeastOnce = Mockito.atLeastOnce
}
