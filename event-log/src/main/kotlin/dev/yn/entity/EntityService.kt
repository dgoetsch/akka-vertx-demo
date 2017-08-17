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

package dev.yn.entity

import dev.yn.entity.domain.EntityError
import dev.yn.entity.domain.EventContainer
import dev.yn.entity.domain.HistoryContainer
import dev.yn.entity.domain.StateContainer
import dev.yn.event.service.EventService
import dev.yn.event.domain.AggregateEvent
import dev.yn.event.domain.EventDomain
import dev.yn.event.serialization.Serialization
import org.funktionale.either.Either
import org.funktionale.either.flatMap
import java.time.Instant
import java.util.*

/**
 * EntityService provides de/serialization on top of eventService
 */

abstract class EntityService<State, Event: EventDomain>(val eventSerialization: Serialization<EntityError, Event>,
                                           val stateSerialization: Serialization<EntityError, State>,
                                           val eventService: EventService) {
    fun logEvent(entityID: UUID, event: Event): Either<EntityError, Unit> {
        return eventService.logEvent(dev.yn.event.domain.Event(entityID, Date.from(Instant.now()), event.eventType, UUID.randomUUID(), eventSerialization.serialize(event)))
                .left().map { EntityError.EventLogError(it) }
    }

    fun getEvents(entityId: UUID, day: Date): List<Either<EntityError, EventContainer<Event>>> {
        return eventService.getEvents(entityId, day).deserialize()
    }

    fun getEntity(entityId: UUID): Either<EntityError, StateContainer<State>> {
        return eventService.getCurrentAggregate(entityId).deserialize()
                .right().flatMap { it?.let{ Either.right(it) } ?: Either.left(EntityError.NotFound(entityId)) }
    }

    fun getEntityHistory(entityId: UUID): Either<EntityError, HistoryContainer<State, Event, EntityError>> {
        return eventService.getEventHistory(entityId)
                .left().map { EntityError.EventLogError(it) }
                .right().flatMap { eventHistory ->
            eventHistory.currentState.deserialize().right().flatMap { current ->
                eventHistory.past.deserialize().right().map { last ->
                    HistoryContainer<State, Event, EntityError>(eventHistory.today.deserialize(), last, current)
                }
            }
        }
    }

    protected fun AggregateEvent?.deserialize(): Either<EntityError, StateContainer<State>?> {
        return this?.let { event ->
            stateSerialization.deserialize(event.body)
                    .right().map { StateContainer(it, event.date, event.userIds, event.eventTypes) } } ?: Either.right(null)
    }

    protected fun List<dev.yn.event.domain.Event>.deserialize(): List<Either<EntityError, EventContainer<Event>>> {
        return this.map { event ->
            eventSerialization.deserialize(event.body)
                    .right().map { EventContainer(it, event.eventTime, event.userId) }
        }
    }
}