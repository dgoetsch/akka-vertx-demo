package dev.yn.entity.domain

import org.funktionale.either.Either
import java.util.*

data class StateContainer<State>(val state: State, val time: Date, val userIds: List<UUID>, val eventTypes: List<String>)
data class EventContainer<Event>(val event: Event, val eventTime: Date, val userId: UUID)
data class HistoryContainer<State, Event, Error>(val today: List<Either<Error, EventContainer<Event>>>, val past: StateContainer<State>?, val currentState: StateContainer<State>?)
