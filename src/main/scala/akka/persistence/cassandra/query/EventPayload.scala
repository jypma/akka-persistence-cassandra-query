package akka.persistence.cassandra.query

import akka.util.ByteString

/**
 * Payload wrapper for an EventEnvelope, allowing access to both the serialized and deserialized form.
 */
case class EventPayload[T](serialized: ByteString, deserialized: T)
