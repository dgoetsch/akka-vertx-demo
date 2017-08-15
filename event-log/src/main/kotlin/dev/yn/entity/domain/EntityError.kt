package dev.yn.entity.domain

import dev.yn.event.domain.EventError
import java.util.*

sealed class EntityError {
    data class DatabaseError(val cause: Throwable): EntityError()
    data class IllegalEventType(val name: String): EntityError()
    data class JsonError(val throwable: Throwable): EntityError()
    data class MissingField(val fieldName: String): EntityError()
    data class NotFound(val userId: UUID): EntityError()
    data class EventLogError(val eventError: EventError): EntityError()
}
