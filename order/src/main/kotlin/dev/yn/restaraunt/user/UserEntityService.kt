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

package dev.yn.restauant.user

import dev.yn.entity.EntityService
import dev.yn.entity.domain.EntityError
import dev.yn.event.service.EventService
import dev.yn.event.serialization.Serialization
import dev.yn.restauant.user.domain.*

class UserEntityService(userEventSerialization: Serialization<EntityError, UserEvent>,
                        userStateSerialization: Serialization<EntityError, UserState>,
                        eventService: EventService): EntityService<UserState, UserEvent>(userEventSerialization, userStateSerialization, eventService)
