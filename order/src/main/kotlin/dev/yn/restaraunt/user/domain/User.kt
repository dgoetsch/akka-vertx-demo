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
