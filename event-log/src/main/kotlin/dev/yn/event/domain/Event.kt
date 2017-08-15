package dev.yn.event.domain

import java.nio.ByteBuffer
import java.util.*

sealed abstract class EventError(message: String): Throwable(message) {
    class UnhandledEventType(val eventType: String): EventError(eventType)
    class InvalidState(): EventError("cannot process event at this time")
    class Conflict(id: UUID): EventError("conflict on resource: $id")
}

interface EventDomain {
    val eventType: String
}

data class Event(val resourceId: UUID, val eventTime: Date, val eventType: String, val userId: UUID, val body: ByteBuffer)

data class AggregateEvent(val resourceId: UUID, val date: Date, val eventTypes: List<String>, val userIds: List<UUID>, val body: ByteBuffer)

data class EventHistory(val today: List<Event>, val past: AggregateEvent?, val currentState: AggregateEvent?)
