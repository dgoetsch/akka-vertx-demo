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

package dev.yn.restauant.user.domain

import dev.yn.entity.domain.EntityError
import dev.yn.event.domain.EventDomain
import dev.yn.event.domain.EventError
import org.funktionale.either.Either
import java.util.*


sealed class UserEvent: EventDomain {
    abstract val user: UserRef

    data class Create(override val user: UserRef, val initialState: UserState): UserEvent() {
        override val eventType = "Create"
    }

    data class UpdateName(override val user: UserRef, val name: String): UserEvent() {
        override val eventType = "UpdateName"
    }

    data class UpdateEmail(override val user: UserRef, val email: Email): UserEvent() {
        override val eventType: String = "UpdateEmail"
    }

    data class UpdateRoles(override val user: UserRef, val add: List<String>, val remove: List<String>): UserEvent() {
        override val eventType: String = "UpdateRoles"
    }

    data class DeleteUser(override val user: UserRef): UserEvent() {
        override val eventType: String = "Delete"
    }
}
sealed class UserError {
    data class IllegalEventType(val name: String): UserError()
    data class JsonError(val throwable: Throwable): UserError()
    data class MissingField(val fieldName: String): UserError()
    data class NotFound(val userId: UUID): UserError()
    data class EventLogError(val eventError: EventError): UserError()
}

sealed class UserEventType(val name: String) {
    object CreateUser: UserEventType("Create")
    object UpdateName: UserEventType("UpdateName")
    object UpdateEmail: UserEventType("UpdateEmail")
    object UpdateRoles: UserEventType("UpdateRoles")
    object DeleteUser: UserEventType("Delete")

    companion object {
        fun fromName(name: String): Either<EntityError, UserEventType> {
            return when(name) {
                CreateUser.name -> Either.Right(CreateUser)
                UpdateName.name -> Either.Right(UpdateName)
                UpdateEmail.name -> Either.Right(UpdateEmail)
                UpdateRoles.name -> Either.Right(UpdateRoles)
                DeleteUser.name -> Either.Right(DeleteUser)
                else -> Either.Left(EntityError.IllegalEventType(name))
            }
        }
    }
}