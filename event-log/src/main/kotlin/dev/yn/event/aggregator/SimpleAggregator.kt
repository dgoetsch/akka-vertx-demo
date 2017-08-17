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

package dev.yn.event.aggregator

import dev.yn.entity.domain.EntityError
import org.funktionale.either.getOrElse

abstract class SimpleAggregator<State, Event>(
        val stateSerialization: dev.yn.event.serialization.Serialization<EntityError, State>,
        val eventSerialization: dev.yn.event.serialization.Serialization<EntityError, Event>): Aggregator {

    override val baseEventBytes: java.nio.ByteBuffer = java.nio.ByteBuffer.allocate(0)

    override fun incorporate(event: java.nio.ByteBuffer, state: java.nio.ByteBuffer): java.nio.ByteBuffer {
        return eventSerialization.deserialize(event)
                .right().map(incorporate(stateSerialization.deserialize(state).right().getOrElse { null }))
                .right().map { it?.let { stateSerialization.serialize(it) } ?: state }
                .right().getOrElse { state }
    }

    override fun aggregate(events: List<java.nio.ByteBuffer>, state: java.nio.ByteBuffer): java.nio.ByteBuffer {
        return events.fold(stateSerialization.deserialize(state).right().getOrElse { null }) {
            state: State?, eventBytes: java.nio.ByteBuffer ->
            eventSerialization.deserialize(eventBytes)
                    .right().map(incorporate(state))
                    .right().getOrElse { state }
        }?.let(stateSerialization::serialize) ?: state
    }

    abstract fun incorporate(state: State?): (Event) -> State?
}