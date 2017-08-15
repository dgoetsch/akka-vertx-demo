package dev.yn.event.service

import dev.yn.event.domain.AggregateEvent
import dev.yn.event.domain.Event
import dev.yn.event.domain.EventError
import dev.yn.event.domain.EventHistory
import java.util.*

/**
 * BAse class for event service.
 *
 * Provides basic get and log methods as well as a framework for aggregation.
 */
interface EventService {

    /**
     * Aggregation logic to be implemented by the user
     */
    val aggregator: dev.yn.event.aggregator.Aggregator

    /**
     * Aggregate a single event against the current Aggregate
     */
    fun incorporate(event: Event, aggregateEvent: AggregateEvent): AggregateEvent {
        return aggregateEvent.copy(
                date = event.eventTime,
                eventTypes = aggregateEvent.eventTypes + event.eventType,
                userIds = aggregateEvent.userIds + event.userId,
                body = aggregator.incorporate(event.body, aggregateEvent.body))

    }

    fun incorporateAndResetEventTypesAndUserIds(event: Event, aggregateEvent: AggregateEvent) : AggregateEvent {
        return aggregateEvent.copy(
                date = event.eventTime,
                eventTypes = listOf(event.eventType),
                userIds = listOf(event.userId),
                body = aggregator.incorporate(event.body, aggregateEvent.body))

    }

    /**
     * Aggregate many events against the current optional state.
     *
     * Will be null if and only if incorporate event is null and events is empty
     *
     */
    fun aggregate(events: List<Event>, aggregateEvent: AggregateEvent?): AggregateEvent? {
        if (events.isNotEmpty()) {
            return aggregateEvent?.let { aggregated ->
                aggregated.copy(
                        date = events.last().eventTime,
                        eventTypes = aggregated.eventTypes + events.map { it.eventType },
                        userIds = aggregated.userIds + events.map { it.userId },
                        body = aggregator.aggregate(events.map { it.body }, aggregated.body))
            } ?:
                    AggregateEvent(
                            resourceId = events.first().resourceId,
                            date = events.last().eventTime,
                            eventTypes = events.map { it.eventType },
                            userIds = events.map { it.userId },
                            body = aggregator.aggregate(events.map { it.body }, aggregator.baseEventBytes))
        } else {
            return aggregateEvent
        }
    }

    fun logEvent(event: Event): org.funktionale.either.Either<EventError, Unit>
    fun getEventHistory(resourceId: java.util.UUID): org.funktionale.either.Either<EventError, EventHistory>
    fun getEvents(entityId: java.util.UUID, day: java.util.Date): List<Event>
    fun getCurrentAggregate(resourceId: java.util.UUID): AggregateEvent?
    fun getLatest(resourceId: UUID): Event?
    fun processLoggedEvent(event: Event)
}