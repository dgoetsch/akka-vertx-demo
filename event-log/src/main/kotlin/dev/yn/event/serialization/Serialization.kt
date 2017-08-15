package dev.yn.event.serialization

import com.fasterxml.jackson.databind.ObjectMapper
import dev.yn.util.string.toByteBuffer
import org.funktionale.either.Either
import org.funktionale.tries.Try
import java.nio.ByteBuffer

interface Serialization<E, T> {
    fun serialize(thing: T): ByteBuffer
    fun deserialize(bytes: ByteBuffer): Either<E, T>
}

abstract class JacksonSerialization<E, T>(val objectMapper: ObjectMapper): Serialization<E, T> {
    abstract val clazz: Class<T>
    abstract val jsonError: (Throwable) -> E

    override fun serialize(thing: T): ByteBuffer {

        return objectMapper.writeValueAsString(thing).toByteBuffer()
    }

    override fun deserialize(bytes: ByteBuffer): Either<E, T> {
        return Try { objectMapper.readValue(bytes.array(), clazz) }
                .toEither()
                .left().map(jsonError)
    }
}