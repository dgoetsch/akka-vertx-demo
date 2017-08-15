package dev.yn.entity.serialization

import com.fasterxml.jackson.databind.ObjectMapper
import dev.yn.entity.domain.EntityError
import dev.yn.event.serialization.JacksonSerialization

open class EntityJacksonSerialization<T>(objectMapper: ObjectMapper, override val clazz: Class<T>): JacksonSerialization<EntityError, T>(objectMapper) {
    override val jsonError: (Throwable) -> EntityError = { EntityError.JsonError(it) }
}