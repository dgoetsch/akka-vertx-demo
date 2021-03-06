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


data class Email(val address: String, val domain: String)

data class UserRef(val id: java.util.UUID, val name: String, val roles: List<String>)

data class UserState(val userName: String, val email: Email, val roles: Set<String>, val deleted: Boolean = false) {
    fun updateName(event: UserEvent.UpdateName): UserState {
        return this.copy(userName = event.name)
    }

    fun updateEmail(event: UserEvent.UpdateEmail): UserState {
        return this.copy(email = event.email)
    }

    fun updateRoles(event: UserEvent.UpdateRoles): UserState {
        return this.copy(roles = (this.roles?:emptySet()).plus(event.add).minus(event.remove))
    }

    fun delete(event: UserEvent.DeleteUser): UserState {
        return this.copy(deleted = true)
    }
}
