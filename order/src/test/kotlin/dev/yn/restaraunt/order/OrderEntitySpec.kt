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

package dev.yn.restautant.order

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import dev.yn.event.domain.Event
import dev.yn.event.repository.*
import dev.yn.util.string.toByteBuffer
import io.kotlintest.Spec
import io.kotlintest.matchers.shouldBe
import io.kotlintest.specs.StringSpec
import org.funktionale.either.Either
import dev.yn.cassandra.CassandraConnector
import dev.yn.entity.domain.EntityError
import dev.yn.entity.serialization.EntityJacksonSerialization
import dev.yn.event.service.AsynchronousAggregatedEventService
import dev.yn.event.service.EventService
import dev.yn.event.service.SynchronousAggregatedEventService
import dev.yn.restauant.user.domain.UserRef
import dev.yn.restauraunt.order.OrderEntityService
import dev.yn.restauraunt.order.aggregation.OrderAggregator
import dev.yn.restauraunt.order.domain.*
import dev.yn.restauraunt.order.serialization.*
import io.kotlintest.TestCaseContext
import java.util.*

class OrderEntitySpec : StringSpec() {
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

    override fun interceptTestCase(test: TestCaseContext, spec: () -> Unit) {
        aggregateEventRepository.initialize()
        currentAggregateEventRepository.initialize()
        eventRepository.initialize()

        spec()

        CassandraConnector.singleton.session().execute(AggregateEventCQL.dropTableStatement)
        CassandraConnector.singleton.session().execute(CurrentAggregateEventCQL.dropTableStatement)
        CassandraConnector.singleton.session().execute(EventCQL.dropTableStatement)
        CassandraConnector.singleton.session().execute(EventCQL.dropEventTypeIndexStatment)
        CassandraConnector.singleton.session().execute(EventCQL.dropUserIdIndexStatment)
        CassandraConnector.singleton.session().execute(CurrentAggregateEventCQL.dropEventTypeIndexStatment)
        CassandraConnector.singleton.session().execute(CurrentAggregateEventCQL.dropUserIdIndexStatment)
        CassandraConnector.singleton.session().execute(AggregateEventCQL.dropEventTypeIndexStatment)
        CassandraConnector.singleton.session().execute(AggregateEventCQL.dropUserIdIndexStatment)
    }

    private fun runTest(eventService: EventService) {
        println(eventService.javaClass.simpleName)
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        val orderEntityService = OrderEntityService(
                orderEventSerialization,
                orderStateSerialization,
                eventService)


        val customer1 = UserRef(UUID.randomUUID(), "jibby", listOf("customer"))
        val customer2 = UserRef(UUID.randomUUID(), "jibby2", listOf("customer"))
        val customer3 = UserRef(UUID.randomUUID(), "jimmy3", listOf("customer"))

        val grillCook = UserRef(UUID.randomUUID(), "jack", listOf("grill-cook"))
        val fryCook = UserRef(UUID.randomUUID(), "greg", listOf("fry-cook"))
        val sushiChef = UserRef(UUID.randomUUID(), "toryama", listOf("sushi-chef"))

        val kitchenManager = UserRef(UUID.randomUUID(), "jenny", listOf("mgmt"))
        val sushiApprentice = UserRef(UUID.randomUUID(), "koji", listOf("sushi-stage"))

        val waitStaff1 = UserRef(UUID.randomUUID(), "kim", listOf("wait-staff"))
        val waitStaff2 = UserRef(UUID.randomUUID(), "kim clone", listOf("wait-staff"))
        val items = listOf(
                LineItem("fries", emptyList()),
                LineItem("sausage", emptyList()),
                LineItem("sushi", listOf("extra ginger")),
                LineItem("steak", listOf("rare", "super rare", "no really, like don't even cook it")),
                LineItem("sushi", listOf("ebi")),
                LineItem("cheesecake", emptyList()))

        val events = listOf(
                OrderEvent.Open(customer1),
                OrderEvent.AddLineItems(
                        listOf(items.get(0), items.get(1)),
                        customer1),
                OrderEvent.AddLineItems(
                        listOf(items.get(2), items.get(3)),
                        customer2),
                OrderEvent.ChangeItemState(setOf(2), sushiChef, LineItemState.InPrep.name),
                OrderEvent.ChangeItemState(setOf(1, 3), grillCook, LineItemState.InPrep.name),
                OrderEvent.AddLineItems(
                        listOf(items.get(4)),
                        customer3),
                OrderEvent.ChangeItemState(setOf(0), fryCook, LineItemState.PrepComplete.name),
                OrderEvent.ChangeItemState(setOf(4), sushiChef, LineItemState.InPrep.name),
                OrderEvent.ChangeItemState(setOf(2, 4), sushiChef, LineItemState.InStage.name),
                OrderEvent.ChangeItemState(setOf(1), grillCook, LineItemState.PrepComplete.name),
                OrderEvent.ChangeItemState(setOf(3), grillCook, LineItemState.PrepComplete.name),
                OrderEvent.ChangeItemState(setOf(0), kitchenManager, LineItemState.StageComplete.name),
                OrderEvent.ChangeItemState(setOf(2, 4), sushiApprentice, LineItemState.InStage.name),
                OrderEvent.ChangeItemState(setOf(1, 3), kitchenManager, LineItemState.StageComplete.name),
                OrderEvent.ChangeItemState(setOf(0, 1, 3), waitStaff1, LineItemState.DeliveryComplete.name),
                OrderEvent.ChangeItemState(setOf(2, 4), waitStaff1, LineItemState.Delivery.name),
                OrderEvent.ChangeItemState(setOf(2, 4), waitStaff2, LineItemState.DeliveryComplete.name),
                OrderEvent.AddLineItems(
                        listOf(items.get(5)),
                        customer1),
                OrderEvent.PaymentReceived(800L, customer3),
                OrderEvent.ChangeItemState(setOf(5), kitchenManager, LineItemState.StageComplete.name),
                OrderEvent.ChangeItemState(setOf(5), waitStaff1, LineItemState.DeliveryComplete.name),
                OrderEvent.PaymentReceived(5000L, customer1))
        val orderId = UUID.randomUUID()


        val logTime = events.map { event ->
            time {
                    orderEntityService.logEvent(orderId, event)
            }
        }.average()

        println("took and average of ${logTime.toLong()} ns to log an event")

        orderEntityService.getEvents(orderId, Date()).map { it.right().map { it.event }} shouldBe events.map { Either.Right<OrderEvent, OrderEvent>(it) }

        val fetchTime = time { orderEntityService.getEvents(orderId, Date()) }
        println("took $fetchTime ns to get the events")
        when(eventService) {
            is AsynchronousAggregatedEventService -> {

                val processTime = time({
                        eventService.processLoggedEvent(Event(orderId, Date(), "event", UUID.randomUUID(), "anything".toByteBuffer()))
                })
                println("took $processTime ns to incorporate the state")
            }

            else -> {}
        }


        orderEntityService.getEntity(orderId).right().map { it.state } shouldBe Either.Right<EntityError, OrderState>(
                OrderState(
                        items,
                        mapOf(LineItemState.Queued.name to emptySet(),
                                LineItemState.InPrep.name to emptySet(),
                                LineItemState.PrepComplete.name to emptySet(),
                                LineItemState.InStage.name to emptySet(),
                                LineItemState.StageComplete.name to emptySet(),
                                LineItemState.Delivery.name to emptySet(),
                                LineItemState.DeliveryComplete.name to (0..5).toSet()), 5800L, emptyList(), false))
    }

    fun time(f: () -> Unit): Long {
        val start: Long = System.nanoTime()
        f()
        return System.nanoTime() - start
    }

    init {
        "should write an open event, add items, and move them with the AsynchronousAggregatedEventService" {
            val eventService = AsynchronousAggregatedEventService(
                    currentAggregateEventRepository,
                    aggregateEventRepository,
                    eventRepository,
                    OrderAggregator(orderStateSerialization, orderEventSerialization))

            runTest(eventService)
        }

        "should write an open event, add items, and move them with the SynchronousAggregatedEventService" {
            val eventService = SynchronousAggregatedEventService(
                    currentAggregateEventRepository,
                    aggregateEventRepository,
                    eventRepository,
                    OrderAggregator(orderStateSerialization, orderEventSerialization), "open", "delete")

            runTest(eventService)
        }
    }


}
