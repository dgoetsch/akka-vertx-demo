package dev.yn.event.service

import dev.yn.event.domain.AggregateEvent
import dev.yn.event.domain.Event
import dev.yn.event.domain.EventError
import dev.yn.event.domain.EventHistory
import org.funktionale.either.Either
import java.util.*

/**
 * Extend this class to get high write throughput with no validation.
 *
 * An event is logged to the event log and is later aggregated
 */
class AsynchronousAggregatedEventService(val currentAggregateRepository: dev.yn.event.repository.CurrentAggregateEventRepository,
                                                  val aggregateRepository: dev.yn.event.repository.AggregateEventRepository,
                                                  val eventRepository: dev.yn.event.repository.EventRepository,
                                                  override val aggregator: dev.yn.event.aggregator.Aggregator): EventService {
    /**
     * Write the event to the event table
     */
    override fun logEvent(event: dev.yn.event.domain.Event): Either<EventError, Unit> {
        return when(eventRepository.create(event)) {
            true -> {
                Either.Right(Unit)
            }
            false -> Either.Left(EventError.Conflict(event.resourceId))
        }
    }

    /**
     * Process events, reaggregate daily incorporate for all events and and current incorporate since either the latest daily
     * incorporate or the event time, whichever is earlier
     *
     * This should be called by a verticle when an event is published and possibly on regular intervals
     *
     */
    override fun processLoggedEvent(event: dev.yn.event.domain.Event) {
        val now = java.util.Date()
        aggregateRepository.getLatest(event.resourceId)?.let { latestAggregate ->
            val daysSinceAggregate = dev.yn.util.date.constructDayList(dev.yn.util.date.earliest(latestAggregate.date, event.eventTime), now)
            val lastAggregate = daysSinceAggregate.fold(latestAggregate) { lastAggregate, day ->
                //We may want to do this a bit differently - i.e. load the whole day and then incorporate.
                // The current implementation will only load rows as the driver pages them, so it is slower but
                // will handle days with a lot of event well - i.e. shouldn't run out of memory
                val newAggregate = eventRepository.getOnDateRs(event.resourceId, day).fold(lastAggregate.copy(eventTypes = emptyList(), userIds = emptyList())) { dayAggregate, event ->
                    incorporate(dev.yn.event.repository.EventCQL.rowToEvent(event), dayAggregate)
                }

                aggregateRepository.create(newAggregate)
                newAggregate
            }
            currentAggregateRepository.create(lastAggregate)
        } ?: processWhenNoAggregate(event)
    }

    /**
     * Get all events and reaggregate for all days since the beginning of time
     */
    private fun processWhenNoAggregate(event: dev.yn.event.domain.Event) {
        eventRepository.getEarliest(event.resourceId)?.let { earliestEvent ->
            val daysSinceBeginning = dev.yn.util.date.constructDayList(earliestEvent.eventTime, Date())
            val lastAggregate = daysSinceBeginning.fold(dev.yn.event.domain.AggregateEvent(event.resourceId, event.eventTime, emptyList(), emptyList(), aggregator.baseEventBytes)) { lastAggregate, day ->
                //We may want to do this a bit differently - i.e. load the whole day and then incorporate.
                // The current implementation will only load rows as the driver pages them, so it is slower but
                // will handle days with a lot of event well - i.e. shouldn't run out of memory
                val newAggregate = eventRepository.getOnDateRs(event.resourceId, day).fold(lastAggregate.copy(eventTypes = emptyList(), userIds = emptyList())) { dayAggregate, event ->
                    incorporate(dev.yn.event.repository.EventCQL.rowToEvent(event), dayAggregate)
                }

                aggregateRepository.create(newAggregate)
                newAggregate
            }
            currentAggregateRepository.create(lastAggregate)
        }
    }

    override fun getEvents(entityId: java.util.UUID, day: java.util.Date): List<dev.yn.event.domain.Event> {
        return eventRepository.getOnDate(entityId, dev.yn.util.date.dayFormat.format(day))
    }

    override fun getCurrentAggregate(resourceId: java.util.UUID): dev.yn.event.domain.AggregateEvent? {
        return currentAggregateRepository.get(resourceId)?:aggregateRepository.getLatest(resourceId)
    }

    /**
     * get events since the altest daily incorporate, the latest incorporate, and the currentAggregate
     */
    override fun getEventHistory(resourceId: java.util.UUID): Either<EventError, EventHistory> {
        return currentAggregateRepository.get(resourceId)?.let { currentAggregate ->
            aggregateRepository.getLatest(resourceId)?.let { aggregatedEvent ->
                Either.Right<EventError, EventHistory>(dev.yn.event.domain.EventHistory(eventRepository.getAfterDate(resourceId, aggregatedEvent.date), aggregatedEvent, currentAggregate))
            } ?: Either.Right<EventError, EventHistory>(dev.yn.event.domain.EventHistory(eventRepository.getAll(resourceId), null, currentAggregate))
        }?: Either.Right<EventError, EventHistory>(dev.yn.event.domain.EventHistory(eventRepository.getAll(resourceId), null, null))
    }

    override fun getLatest(resourceId: UUID): Event? {
        return eventRepository.getLatest(resourceId)
    }
}
