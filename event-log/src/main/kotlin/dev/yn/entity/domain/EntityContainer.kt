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

package dev.yn.entity.domain

import org.funktionale.either.Either
import java.util.*

data class StateContainer<State>(val state: State, val time: Date, val userIds: List<UUID>, val eventTypes: List<String>)
data class EventContainer<Event>(val event: Event, val eventTime: Date, val userId: UUID)
data class HistoryContainer<State, Event, Error>(val today: List<Either<Error, EventContainer<Event>>>, val past: StateContainer<State>?, val currentState: StateContainer<State>?)
