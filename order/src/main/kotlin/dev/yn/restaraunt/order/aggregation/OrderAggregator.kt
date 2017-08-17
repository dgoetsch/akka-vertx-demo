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

package dev.yn.restauraunt.order.aggregation

import dev.yn.entity.domain.EntityError
import dev.yn.event.aggregator.SimpleAggregator
import dev.yn.event.serialization.Serialization
import dev.yn.restauraunt.order.domain.OrderEvent
import dev.yn.restauraunt.order.domain.OrderState

class OrderAggregator(
        val orderStateSerialization: Serialization<EntityError, OrderState>,
        val orderEventSerialization: Serialization<EntityError, OrderEvent>): SimpleAggregator<OrderState, OrderEvent>(orderStateSerialization, orderEventSerialization) {
    val emptyState =  OrderState(emptyList(), emptyMap(), 0L, emptyList())

    override fun incorporate(state: OrderState?): (OrderEvent) -> OrderState? = { event ->
        (state?:emptyState).let {
            if (!it.closed) {
                when (event) {
                    is OrderEvent.Open -> it.openEvent(event)
                    is OrderEvent.AddLineItems -> it.addLineItem(event)
                    is OrderEvent.ChangeItemState -> it.changeState(event)
                    is OrderEvent.PaymentReceived -> it.receivePayment(event)
                    is OrderEvent.Complete -> it.completeEvent(event)
                }
            } else {
                it
            }
        }
    }
    
}