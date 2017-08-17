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

package dev.yn.restauraunt.order.domain

import dev.yn.entity.domain.EntityError
import dev.yn.event.domain.EventDomain
import dev.yn.restauant.user.domain.UserRef
import org.funktionale.either.Either

sealed class OrderEvent: EventDomain {
    abstract val user: UserRef

    data class Open(override val user: UserRef) : OrderEvent() {
        override val eventType: String = OrderEventType.Open.name
    }

    data class AddLineItems(val lineItems: List<LineItem>, override val user: UserRef) : OrderEvent() {
        override val eventType: String = OrderEventType.AddLineItems.name
    }

    data class ChangeItemState(val lineItemIndices: Set<Int>, override val user: UserRef, val newState: String) : OrderEvent() {
        override val eventType: String = OrderEventType.ChangeItemState.name
    }

    data class PaymentReceived(val amount: Long, override val user: UserRef): OrderEvent() {
        override val eventType: String = OrderEventType.PaymentReceived.name
    }

    data class Complete(override val user: UserRef): OrderEvent() {
        override val eventType: String = OrderEventType.Complete.name
    }
}

sealed abstract class OrderEventType(val name: String) {
    object Open: OrderEventType("open")
    object ChangeItemState: OrderEventType("changeItemState")
    object AddLineItems: OrderEventType("addLineItem")
    object PaymentReceived: OrderEventType("paymentReceived")
    object Complete: OrderEventType("complete")

    companion object {
        fun fromName(name: String): Either<EntityError.IllegalEventType, OrderEventType> {
            return when(name) {
                Open.name -> Either.right(Open)
                AddLineItems.name -> Either.right(AddLineItems)
                ChangeItemState.name -> Either.right(ChangeItemState)
                PaymentReceived.name -> Either.right(PaymentReceived)
                Complete.name -> Either.right(Complete)
                else -> Either.left(EntityError.IllegalEventType(name))
            }
        }

    }
}

sealed abstract class LineItemState(val name: String) {
    object Queued: LineItemState("queued")
    object InPrep: LineItemState("prep")
    object PrepComplete: LineItemState("prepComplete")
    object InStage: LineItemState("stage")
    object StageComplete: LineItemState("stageComplete")
    object Delivery: LineItemState("delivery")
    object DeliveryComplete: LineItemState("deliveryComplete")
    object Refunded: LineItemState("refunded")

    companion object {
        fun fromName(name: String): Either<EntityError, LineItemState> {
            return when (name) {
                Queued.name -> Either.Right(Queued)
                InPrep.name -> Either.Right(InPrep)
                PrepComplete.name -> Either.Right(PrepComplete)
                InStage.name -> Either.Right(InStage)
                StageComplete.name -> Either.Right(StageComplete)
                Delivery.name -> Either.Right(Delivery)
                DeliveryComplete.name -> Either.Right(DeliveryComplete)
                Refunded.name -> Either.Right(Refunded)
                else -> Either.Left(EntityError.IllegalEventType(name))
            }
        }
    }
}