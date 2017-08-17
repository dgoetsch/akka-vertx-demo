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