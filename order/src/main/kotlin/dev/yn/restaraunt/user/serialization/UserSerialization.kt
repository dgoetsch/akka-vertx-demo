/*
 *    Copyright 2017 Devyn Goetsch
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package dev.yn.restauant.user.serialization

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import dev.yn.entity.domain.EntityError
import dev.yn.event.serialization.JacksonSerialization
import dev.yn.event.serialization.Serialization
import org.funktionale.either.Either
import org.funktionale.either.flatMap
import org.funktionale.tries.Try
import dev.yn.restauant.user.domain.UserEvent
import dev.yn.restauant.user.domain.UserEventType
import java.nio.ByteBuffer


class UserEventSerialization(val objectMapper: ObjectMapper,
                             val createUserEventSerialization: Serialization<EntityError, UserEvent.Create>,
                             val updateUserNameSerialization: Serialization<EntityError, UserEvent.UpdateName>,
                             val updateUserEmailSerialization: Serialization<EntityError, UserEvent.UpdateEmail>,
                             val updateUserRolesSerialization: Serialization<EntityError, UserEvent.UpdateRoles>,
                             val deleteUserEventSerialization: Serialization<EntityError, UserEvent.DeleteUser>): Serialization<EntityError, UserEvent> {
    fun readEventType(event: ByteBuffer): Either<EntityError, UserEventType> {
        return Try { objectMapper.readValue(event.array(), ObjectNode::class.java).get("eventType") }
                .map { it?.let { it.asText() } }
                .toEither()
                .left().map { EntityError.JsonError(it) }
                .right().flatMap { it?.let { Either.Right<EntityError, String>(it) } ?: Either.Left<EntityError, String>(EntityError.MissingField("eventType")) }
                .right().flatMap { UserEventType.fromName(it) }
    }

    override fun serialize(userEvent: UserEvent): ByteBuffer {
        return when(userEvent) {
            is UserEvent.Create -> createUserEventSerialization::serialize
            is UserEvent.UpdateName -> updateUserNameSerialization::serialize
            is UserEvent.UpdateEmail -> updateUserEmailSerialization::serialize
            is UserEvent.UpdateRoles -> updateUserRolesSerialization::serialize
            is UserEvent.DeleteUser -> deleteUserEventSerialization::serialize
        }(userEvent)
    }

    override fun deserialize(byteBuffer: ByteBuffer): Either<EntityError, UserEvent> {
        return readEventType(byteBuffer)
                .right().flatMap {
            when(it) {
                is UserEventType.CreateUser ->  createUserEventSerialization::deserialize
                is UserEventType.UpdateName -> updateUserNameSerialization::deserialize
                is UserEventType.UpdateEmail -> updateUserEmailSerialization::deserialize
                is UserEventType.UpdateRoles -> updateUserRolesSerialization::deserialize
                is UserEventType.DeleteUser -> deleteUserEventSerialization::deserialize
            }(byteBuffer)
        }
    }
}
