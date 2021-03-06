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

package dev.yn.restauraunt.order.serialization

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import dev.yn.entity.domain.EntityError
import dev.yn.event.serialization.Serialization
import org.funktionale.either.Either
import org.funktionale.either.flatMap
import org.funktionale.tries.Try
import dev.yn.restauraunt.order.domain.OrderEvent
import dev.yn.restauraunt.order.domain.OrderEventType
import dev.yn.util.either.nonNull
import java.nio.ByteBuffer

class OrderEventSerialization(
        val openEventSerialization: Serialization<EntityError, OrderEvent.Open>,
        val addLineItemsEventSerialization: Serialization<EntityError, OrderEvent.AddLineItems>,
        val changeItemStateEventSerialization: Serialization<EntityError, OrderEvent.ChangeItemState>,
        val paymentReceivedEventSerialization: Serialization<EntityError, OrderEvent.PaymentReceived>,
        val completeEventSerialization: Serialization<EntityError, OrderEvent.Complete>,
        val mapper: ObjectMapper): Serialization<EntityError, OrderEvent> {
    override fun serialize(orderEvent: OrderEvent): ByteBuffer {
        return when (orderEvent) {
            is OrderEvent.Open -> openEventSerialization.serialize(orderEvent)
            is OrderEvent.AddLineItems -> addLineItemsEventSerialization.serialize(orderEvent)
            is OrderEvent.ChangeItemState -> changeItemStateEventSerialization.serialize(orderEvent)
            is OrderEvent.PaymentReceived -> paymentReceivedEventSerialization.serialize(orderEvent)
            is OrderEvent.Complete -> completeEventSerialization.serialize(orderEvent)
        }
    }

    fun readEventType(event: ByteBuffer): Either<EntityError, OrderEventType> {
        return Try { mapper.readValue(event.array(), ObjectNode::class.java).get("eventType") }
                .map { it?.let { it.asText() } }
                .toEither()
                .left().map { EntityError.JsonError(it) }
                .nonNull()
                .right().flatMap { OrderEventType.fromName(it) }
    }

    override fun deserialize(event: ByteBuffer): Either<EntityError, OrderEvent> {
        return readEventType(event)
                .right().flatMap { eventType ->
            when (eventType) {
                OrderEventType.Open -> openEventSerialization.deserialize(event)
                OrderEventType.AddLineItems -> addLineItemsEventSerialization.deserialize(event)
                OrderEventType.ChangeItemState -> changeItemStateEventSerialization.deserialize(event)
                OrderEventType.PaymentReceived -> paymentReceivedEventSerialization.deserialize(event)
                OrderEventType.Complete -> completeEventSerialization.deserialize(event)
            }
        }
    }
}