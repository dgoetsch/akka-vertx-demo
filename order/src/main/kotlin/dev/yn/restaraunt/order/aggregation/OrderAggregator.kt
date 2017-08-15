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