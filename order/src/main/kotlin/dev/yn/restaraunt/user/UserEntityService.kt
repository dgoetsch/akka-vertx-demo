package dev.yn.restauant.user

import dev.yn.entity.EntityService
import dev.yn.entity.domain.EntityError
import dev.yn.event.service.EventService
import dev.yn.event.serialization.Serialization
import dev.yn.restauant.user.domain.*

class UserEntityService(userEventSerialization: Serialization<EntityError, UserEvent>,
                        userStateSerialization: Serialization<EntityError, UserState>,
                        eventService: EventService): EntityService<UserState, UserEvent>(userEventSerialization, userStateSerialization, eventService)
