package dev.yn.event.aggregator

import dev.yn.entity.domain.EntityError
import org.funktionale.either.getOrElse

abstract class SimpleAggregator<State, Event>(
        val stateSerialization: dev.yn.event.serialization.Serialization<EntityError, State>,
        val eventSerialization: dev.yn.event.serialization.Serialization<EntityError, Event>): Aggregator {

    override val baseEventBytes: java.nio.ByteBuffer = java.nio.ByteBuffer.allocate(0)

    override fun incorporate(event: java.nio.ByteBuffer, state: java.nio.ByteBuffer): java.nio.ByteBuffer {
        return eventSerialization.deserialize(event)
                .right().map(incorporate(stateSerialization.deserialize(state).right().getOrElse { null }))
                .right().map { it?.let { stateSerialization.serialize(it) } ?: state }
                .right().getOrElse { state }
    }

    override fun aggregate(events: List<java.nio.ByteBuffer>, state: java.nio.ByteBuffer): java.nio.ByteBuffer {
        return events.fold(stateSerialization.deserialize(state).right().getOrElse { null }) {
            state: State?, eventBytes: java.nio.ByteBuffer ->
            eventSerialization.deserialize(eventBytes)
                    .right().map(incorporate(state))
                    .right().getOrElse { state }
        }?.let(stateSerialization::serialize) ?: state
    }

    abstract fun incorporate(state: State?): (Event) -> State?
}