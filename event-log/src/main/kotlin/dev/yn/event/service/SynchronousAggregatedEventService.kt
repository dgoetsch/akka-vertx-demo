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

package dev.yn.event.service

import dev.yn.event.aggregator.Aggregator
import dev.yn.event.domain.*
import dev.yn.event.repository.*
import java.util.*
import dev.yn.util.date.*
import org.funktionale.either.Either

/**
 * Event repository that provides an optimistic locking implementation to prevent out of order events
 */
class SynchronousAggregatedEventService(val currentAggregateRepository: CurrentAggregateEventRepository,
                                        val aggregateRepository: AggregateEventRepository,
                                        val eventRepository: EventRepository,
                                        override val aggregator: Aggregator,
                                        val createEventTypeKey: String,
                                        val deleteEventTypeKey: String): EventService {
    /**
     * log an event of one of the three designated types
     */
    override fun logEvent(event: Event): Either<EventError, Unit> =
        when {
            event.eventType.contains(createEventTypeKey) -> logCreateEvent(event)
            event.eventType.contains(deleteEventTypeKey) -> logDeleteEvent(event)
            else -> logUpdateEvent(event)
        }

    /**
     * log a create event - only log if there is no existing incorporate
     */
    private fun logCreateEvent(event: Event): Either<EventError, Unit> {
        val baseAggregate = AggregateEvent(
                event.resourceId,
                event.eventTime,
                listOf(event.eventType),
                listOf(event.userId),
                aggregator.incorporate(event.body, aggregator.baseEventBytes))
        return when(currentAggregateRepository.createIfNotExists(baseAggregate)) {
            true -> {
                eventRepository.create(event)
                Either.Right(Unit)
            }
            false -> Either.Left(EventError.Conflict(event.resourceId))
        }
    }

    /**
     * log an update event - only log events that are later than the current latest, and incorporate the day if necessary.
     */
    private fun logUpdateEvent(event: Event): Either<EventError, Unit> {
        val aggregated = currentAggregateRepository.get(event.resourceId)?.let { currentAggregate ->
            if(event.eventTime.after(currentAggregate.date)) {
                if(dayFormat.format(currentAggregate.date) < dayFormat.format(event.eventTime)) {
                    currentAggregateRepository.updateAndResetEventTypeAndUserIdColumns(incorporateAndResetEventTypesAndUserIds(event, currentAggregate), currentAggregate.date)
                            && aggregateRepository.create(currentAggregate)
                            && eventRepository.create(event)
                } else {
                    currentAggregateRepository.update(incorporateAndResetEventTypesAndUserIds(event, currentAggregate), currentAggregate.date)
                            && eventRepository.create(event)
                }
            } else {
                false
            }
        } ?: false

        return when(aggregated) {
            true -> Either.Right(Unit)
            false -> Either.Left(EventError.Conflict(event.resourceId))
        }
    }


    private fun logDeleteEvent(event: Event): Either<EventError, Unit> {
        val aggregated = currentAggregateRepository.get(event.resourceId)?.let { currentAggregate ->
            if(event.eventTime.after(currentAggregate.date)) {
                currentAggregateRepository.delete(event) && eventRepository.create(event) && aggregateRepository.createIfNotExists(incorporate(event, currentAggregate))
            } else {
                false
            }
        } ?: false

        return when(aggregated) {
            true -> Either.Right(Unit)
            false -> Either.Left(EventError.Conflict(event.resourceId))
        }
    }

    override fun getEvents(entityId: UUID, day: Date): List<Event> {
        return eventRepository.getOnDate(entityId, dayFormat.format(day))
    }

    override fun getCurrentAggregate(resourceId: UUID): AggregateEvent? {
        return currentAggregateRepository.get(resourceId)?:aggregateRepository.getLatest(resourceId)
    }

    /**
     * get events since the altest daily incorporate, the latest incorporate, and the currentAggregate
     */
    override fun getEventHistory(resourceId: UUID): Either<EventError, EventHistory> {
        return currentAggregateRepository.get(resourceId)?.let { currentAggregate ->
            aggregateRepository.getLatest(resourceId)?.let { aggregatedEvent ->
                Either.Right<EventError, EventHistory>(EventHistory(eventRepository.getAfterDate(resourceId, aggregatedEvent.date), aggregatedEvent, currentAggregate))
            } ?: Either.Right<EventError, EventHistory>(EventHistory(eventRepository.getAll(resourceId), null, currentAggregate))
        }?: Either.Left(EventError.InvalidState())
    }

    private fun aggregateAndResetEventTypeAndUserId(event: Event, aggregateEvent: AggregateEvent): AggregateEvent {
        return incorporate(event, aggregateEvent).copy(userIds = listOf(event.userId), eventTypes = listOf(event.eventType))
    }

    override fun getLatest(resourceId: UUID): Event? {
        return eventRepository.getLatest(resourceId)
    }

    override fun processLoggedEvent(event: Event): AggregateEvent? = currentAggregateRepository.get(event.resourceId)
}