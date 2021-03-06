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

package dev.yn.restauraunt.order

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import dev.yn.cassandra.CassandraConnector
import dev.yn.entity.EntityService
import dev.yn.entity.domain.EntityError
import dev.yn.entity.serialization.EntityJacksonSerialization
import dev.yn.event.repository.AggregateEventRepository
import dev.yn.event.repository.CurrentAggregateEventRepository
import dev.yn.event.repository.EventRepository
import dev.yn.event.serialization.Serialization
import dev.yn.event.service.AsynchronousAggregatedEventService
import dev.yn.event.service.EventService
import dev.yn.restauraunt.order.aggregation.OrderAggregator
import dev.yn.restauraunt.order.domain.OrderEvent
import dev.yn.restauraunt.order.domain.OrderState
import dev.yn.restauraunt.order.serialization.OrderEventSerialization

class OrderEntityService(orderEventSerialization: Serialization<EntityError, OrderEvent>,
                         orderStateSerialization: Serialization<EntityError, OrderState>,
                         eventService: EventService): EntityService<OrderState, OrderEvent>(orderEventSerialization, orderStateSerialization, eventService) {

}
object OrderEntityServiceFactory {
    fun default(): OrderEntityService {
        val mapper = jacksonObjectMapper()

        val orderEventSerialization = OrderEventSerialization(
                EntityJacksonSerialization<OrderEvent.Open>(mapper, OrderEvent.Open::class.java),
                EntityJacksonSerialization<OrderEvent.AddLineItems>(mapper, OrderEvent.AddLineItems::class.java),
                EntityJacksonSerialization<OrderEvent.ChangeItemState>(mapper, OrderEvent.ChangeItemState::class.java),
                EntityJacksonSerialization<OrderEvent.PaymentReceived>(mapper, OrderEvent.PaymentReceived::class.java),
                EntityJacksonSerialization<OrderEvent.Complete>(mapper, OrderEvent.Complete::class.java),
                mapper
        )

        val orderStateSerialization = EntityJacksonSerialization<OrderState>(mapper, OrderState::class.java)

        val currentAggregateEventRepository = CurrentAggregateEventRepository(CassandraConnector.singleton)
        val aggregateEventRepository = AggregateEventRepository(CassandraConnector.singleton)
        val eventRepository = EventRepository(CassandraConnector.singleton)
        val eventService = AsynchronousAggregatedEventService(
                currentAggregateEventRepository,
                aggregateEventRepository,
                eventRepository,
                OrderAggregator(orderStateSerialization, orderEventSerialization))
        return OrderEntityService(orderEventSerialization, orderStateSerialization, eventService)
    }
}


