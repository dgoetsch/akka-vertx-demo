package dev.yn.event.aggregator

import java.nio.ByteBuffer

/**
 * Aggregation is the method by which the current state of an entity is computed from the event log
 */
interface Aggregator {
    /**
     * The base state of the aggregated entity.  This is the initial state upon which all events are a transformation.
     */
    val baseEventBytes: ByteBuffer

    /**
     * Aggregate a single binary event against the current state
     *
     * @return the new incorporate state in binary
     */
    fun incorporate(event: ByteBuffer, state: ByteBuffer): ByteBuffer

    /**
     * Aggregegate several binary events against the current state
     */
    fun aggregate(events: List<ByteBuffer>, state: ByteBuffer): ByteBuffer
}