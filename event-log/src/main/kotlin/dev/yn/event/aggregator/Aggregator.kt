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

import java.nio.ByteBuffer

/**
 * Aggregation is the method by which the current state of an entity is computed from the event log
 */
interface Aggregator {
    /**
     * The base state of the aggregated entity.  This is the initial state upon which all events are a transformation.
     */
    val baseEventBytes: ByteBuffer

    /**
     * Aggregate a single binary event against the current state
     *
     * @return the new incorporate state in binary
     */
    fun incorporate(event: ByteBuffer, state: ByteBuffer): ByteBuffer

    /**
     * Aggregegate several binary events against the current state
     */
    fun aggregate(events: List<ByteBuffer>, state: ByteBuffer): ByteBuffer
}