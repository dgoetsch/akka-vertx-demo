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

package dev.yn.event.domain

import java.nio.ByteBuffer
import java.util.*

sealed abstract class EventError(message: String): Throwable(message) {
    class UnhandledEventType(val eventType: String): EventError(eventType)
    class InvalidState(): EventError("cannot process event at this time")
    class Conflict(id: UUID): EventError("conflict on resource: $id")
}

interface EventDomain {
    val eventType: String
}

data class Event(val resourceId: UUID, val eventTime: Date, val eventType: String, val userId: UUID, val body: ByteBuffer)

data class AggregateEvent(val resourceId: UUID, val date: Date, val eventTypes: List<String>, val userIds: List<UUID>, val body: ByteBuffer)

data class EventHistory(val today: List<Event>, val past: AggregateEvent?, val currentState: AggregateEvent?)
