package dev.yn.restauraunt.order.domain

import dev.yn.event.domain.EventError
import org.funktionale.either.Either

data class OrderState(val lineItems: List<LineItem>,
                      val lineItemsState: Map<String, Set<Int>>,
                      val amountPaid: Long,
                      val events: List<Either<EventError, OrderEvent>>,
                      val closed: Boolean = false) {
    fun openEvent(event: OrderEvent.Open): OrderState {
        return this
    }

    fun completeEvent(event: OrderEvent.Complete): OrderState {
        return copy(closed = true)
    }

    fun addLineItem(event: OrderEvent.AddLineItems): OrderState {
        val range = (lineItems.size..lineItems.size + event.lineItems.size - 1)
        return copy(
                lineItems = lineItems + event.lineItems,
                lineItemsState = lineItemsState + Pair(LineItemState.Queued.name, getStates(LineItemState.Queued.name) + range))
    }

    fun receivePayment(event: OrderEvent.PaymentReceived): OrderState {
        return copy(amountPaid = amountPaid + event.amount)
    }

    fun changeState(event: OrderEvent.ChangeItemState): OrderState {
        return moveLineItems(event.lineItemIndices, event.newState)
    }

    private fun moveLineItem(index: Int, target: String): OrderState {
        return if(index < lineItems.size) {
            copy(lineItemsState = filterFromStateMap(index) +
                    kotlin.Pair(target, getStates(target).plus(index)))
        } else {
            this
        }
    }

    private fun moveLineItems(indices: Set<Int>, target: String): OrderState {
        val filteredIndices = indices.filter { it < lineItems.size }
        return if(filteredIndices.isNotEmpty()) {
            copy(lineItemsState = filterFromStateMap(indices) +
                    kotlin.Pair(target, getStates(target).plus(indices)))
        } else {
            this
        }
    }

    private fun filterFromStateMap(index: Int): Map<String, Set<Int>> {
        return lineItemsState.mapValues { entry ->
            when(entry.value.contains(index)) {
                true -> entry.value.minus(index)
                else -> entry.value
            }
        }
    }

    private fun filterFromStateMap(indices: Set<Int>): Map<String, Set<Int>> {
        return lineItemsState.mapValues { entry ->
            when (entry.value.containsAny(indices)) {
                true -> entry.value.minus(indices)
                else -> entry.value
            }
        }
    }

    private fun <T> Iterable<T>.containsAny(other: Iterable<T>): Boolean {
        return other.any { contains(it) }
    }

    private fun getStates(state: String): Set<Int> {
        return lineItemsState.get(state) ?: emptySet()
    }
}

data class LineItem(val itemId: String, val instructions: List<String>)
