package dev.yn.restauant.user.aggregation

import dev.yn.entity.domain.EntityError
import dev.yn.event.aggregator.SimpleAggregator
import dev.yn.event.serialization.Serialization
import dev.yn.restauant.user.domain.UserEvent
import dev.yn.restauant.user.domain.UserState
import dev.yn.restauant.user.serialization.UserEventSerialization

class UserAggregation(userStateSerialization: Serialization<EntityError, UserState>,
                      userEventSerialization: UserEventSerialization): SimpleAggregator<UserState, UserEvent>(userStateSerialization, userEventSerialization){
    override fun incorporate(state: UserState?): (UserEvent) -> UserState? = { event ->
        state?.let {
            if(state.deleted) {
                state
            } else {
                when (event) {
                    is UserEvent.UpdateEmail -> state.updateEmail(event)
                    is UserEvent.UpdateName -> state.updateName(event)
                    is UserEvent.UpdateRoles -> state.updateRoles(event)
                    is UserEvent.DeleteUser -> state.delete(event)
                    else -> state
                }
            }
        } ?: when(event) {
            is UserEvent.Create -> event.initialState
            else -> state
        }
    }
}