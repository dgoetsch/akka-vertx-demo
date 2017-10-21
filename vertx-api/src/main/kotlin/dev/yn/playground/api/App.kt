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

package dev.yn.playground.api

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import dev.yn.cassandra.CassandraConnector
import dev.yn.entity.domain.EntityError
import dev.yn.entity.serialization.EntityJacksonSerialization
import dev.yn.event.repository.*
import dev.yn.event.service.AsynchronousAggregatedEventService
import dev.yn.restauraunt.order.OrderEntityService
import dev.yn.restauraunt.order.aggregation.OrderAggregator
import dev.yn.restauraunt.order.domain.OrderEvent
import dev.yn.restauraunt.order.domain.OrderState
import dev.yn.restauraunt.order.serialization.OrderEventSerialization
import io.vertx.core.*
import io.vertx.core.eventbus.Message
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageCodec
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import org.funktionale.either.Either
import org.funktionale.either.flatMap
import org.funktionale.either.getOrElse
import org.funktionale.tries.Try
import java.nio.ByteBuffer
import java.util.*

fun main(args: Array<String>) {

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

    listOf(
        AggregateEventCQL.dropTableStatement,
        CurrentAggregateEventCQL.dropTableStatement,
        EventCQL.dropTableStatement,
        EventCQL.dropEventTypeIndexStatment,
        EventCQL.dropUserIdIndexStatment,
        CurrentAggregateEventCQL.dropEventTypeIndexStatment,
        CurrentAggregateEventCQL.dropUserIdIndexStatment,
        AggregateEventCQL.dropEventTypeIndexStatment,
        AggregateEventCQL.dropUserIdIndexStatment)
            .forEach { Try {CassandraConnector.singleton.session().execute(it) } }

    Try { currentAggregateEventRepository.initialize() }
    Try { aggregateEventRepository.initialize() }
    Try { eventRepository.initialize() }

    val eventService = AsynchronousAggregatedEventService(
            currentAggregateEventRepository,
            aggregateEventRepository,
            eventRepository,
            OrderAggregator(orderStateSerialization, orderEventSerialization))
    val orderEntityService = OrderEntityService(orderEventSerialization, orderStateSerialization, eventService)

    val vertx = Vertx.vertx()
    vertx.eventBus()
            .registerDefaultCodec(IdAndEvent::class.java, OrderEventCodec(orderEventSerialization))
            .registerDefaultCodec(UUID::class.java, UUIDCodec())

    val logOrderEventVerticle = LogOrderEventVerticle(orderEntityService)
    val orderAggregateManagementVerticle = OrderAggregateManagementVerticle(eventService, 16)
    val logOrderController = LogOrderController(orderEntityService, orderEventSerialization)
    val broadcastUpdateManagementVerticle = BroadcastUpdateManagementVerticle(orderEntityService, mapper, 16)
    vertx.deployVerticle(logOrderEventVerticle, DeploymentOptions().setWorker(true).setWorkerPoolSize(1))
    vertx.deployVerticle(orderAggregateManagementVerticle)
    vertx.deployVerticle(logOrderController)
    vertx.deployVerticle(broadcastUpdateManagementVerticle)
}

object OrderEntityServiceEvents {
    val LogEvent = "event.log"
    val EventLogged = "event.logged"
    val AggregateEvent = "aggregate.event"
    val BroadcastUpdate = "braodcast.update"
    fun eventLoggedResource(id: UUID) = "event.logged.$id"
    fun eventAggregatedResource(id: UUID) = "event.aggregated.$id"
}

class LogOrderController(val orderEntityService: OrderEntityService,
                         val orderEventSerialization: OrderEventSerialization): AbstractVerticle() {

    val deserialize: (RoutingContext) -> IdAndEvent? = { routingContext ->
        orderEventSerialization.deserialize(ByteBuffer.wrap(routingContext.body.bytes))
                .right()
                .flatMap { body ->
                    routingContext.pathParam("orderId")
                            ?.let { Try { UUID.fromString(it) }.getOrElse { UUID.randomUUID() } }
                            ?.let { Either.Right<EntityError, IdAndEvent>(IdAndEvent(it, body)) }
                            ?:Either.Right<EntityError, IdAndEvent>(IdAndEvent(UUID.randomUUID(), body)) }
                .right()
                .getOrElse { null }
    }
    override fun start() {
        val router = Router.router(vertx)
        router.route().handler(BodyHandler.create())

        router.put("/orders/:orderId").handler { routingContext ->
            deserialize(routingContext)
                    ?.let {
                        vertx.eventBus().publish(OrderEntityServiceEvents.LogEvent, it)
                        it
                    }
                    ?.let {
                        routingContext.request().response().setStatusCode(200).end()
                        it
                    } ?:routingContext.request().response().setStatusCode(404).end()
        }

        router.post("/orders").handler { routingContext ->
            deserialize(routingContext)
                    ?.let {
                        when(it.event) {
                            is OrderEvent.Open -> {
                                vertx.eventBus().publish(OrderEntityServiceEvents.LogEvent, it)
                                it.id
                            }
                            else -> null
                        } }
                    ?.let { routingContext.request().response().setStatusCode(200).end(it.toString()) }
                    ?:routingContext.request().response().setStatusCode(404).end("Could not deserialize")
        }

        router.get("/orders/:orderId").handler { routingContext ->
            routingContext.pathParam("orderId")
                    ?.let{ Try{UUID.fromString(it)}.getOrElse { null } }
                    ?.let { id ->
                        orderEntityService.getEntityHistory(id)
                                .right().map { routingContext.request().response().setStatusCode(200).end(JsonObject.mapFrom(it).toBuffer()) }
                                .left().map {routingContext.request().response().setStatusCode(404).end(id.toString()) }
                    } ?: routingContext.request().response().setStatusCode(400).end()
        }


        vertx.createHttpServer()
                .requestHandler(router::accept)
                .websocketHandler { webSocket: ServerWebSocket ->
                    val pathParts = webSocket.path().split("/").filter { it.isNotBlank() }
                    if(pathParts.size == 2 && pathParts.get(0) == "orders") {
                        Try { UUID.fromString(pathParts.get(1)) }
                                .onFailure { webSocket.reject() }
                                .onSuccess { id ->
                                    val eventConsumer = vertx.eventBus().consumer<IdAndEvent>(OrderEntityServiceEvents.eventLoggedResource(id)) {
                                        webSocket.writeTextMessage(String(orderEventSerialization.serialize(it.body().event).array()))
                                    }
                                    val historyConsumer = vertx.eventBus().consumer<String>(OrderEntityServiceEvents.eventAggregatedResource(id)) {
                                        webSocket.writeTextMessage(it.body())
                                    }

                                    webSocket.closeHandler {
                                        eventConsumer.unregister()
                                        historyConsumer.unregister()
                                    }

                                    webSocket.exceptionHandler {
                                        println("caught exception for $id: $it")
                                    }
                                }
                    } else {
                        webSocket.reject()
                    }
                }.listen(8080)
    }
}


class LogOrderEventVerticle(val orderEntityService: OrderEntityService): AbstractVerticle() {
    override fun start() {
        vertx.eventBus()
                .consumer<IdAndEvent>(OrderEntityServiceEvents.LogEvent) { message ->
                    orderEntityService.logEvent(message.body().id, message.body().event)
                            .right().map {
                        vertx.eventBus().publish(OrderEntityServiceEvents.EventLogged, message.body().id)
                        vertx.eventBus().publish(OrderEntityServiceEvents.eventLoggedResource(message.body().id), message.body())
                    }
                }
    }
}

class OrderAggregateManagementVerticle(val eventService: AsynchronousAggregatedEventService, val numWorkers: Int): AbstractVerticle() {
    val actualNumWorkers = maxOf(1, numWorkers)

    val workers = (0..maxOf(0, actualNumWorkers - 1)).map {
        OrderAggregateeVerticle(eventService, it)
    }
    override fun start() {
        workers.forEach {
            vertx.deployVerticle(it, DeploymentOptions()
                    .setWorker(true))
        }
        vertx.eventBus()
                .consumer<UUID>(OrderEntityServiceEvents.EventLogged) {
                    vertx.eventBus().send(OrderEntityServiceEvents.AggregateEvent + ".${Math.abs(it.body().hashCode()) % actualNumWorkers}", it.body())
                }

    }

    override fun stop() {
        workers.forEach { vertx.undeploy(it.deploymentID()) }
    }
}

class GenericConsumerVerticle<T> (val consumerBody: (Message<T>) -> Unit, val consumerAddress: String): AbstractVerticle() {
    override fun start() {
        vertx.eventBus()
                .consumer<T>(consumerAddress, consumerBody)
    }
}

fun <T, U> Future<T>.thenPublish(vertx: Vertx, address: String): Future<U> {
    val future = Future.future<AsyncResult<U>>()
    return this.compose { t ->
        vertx.eventBus().send(address, future.completer())
        future
    }.compose {
        if(it.succeeded()) {
            Future.succeededFuture(it.result())
        } else {
            Future.failedFuture(it.cause())
        }
    }
}

class OrderAggregateeVerticle(val eventService: AsynchronousAggregatedEventService, val workerNumber: Int): AbstractVerticle() {
    override fun start() {
        vertx.eventBus()
                .consumer<UUID>(OrderEntityServiceEvents.AggregateEvent + ".$workerNumber") { id ->
                    println("${workerNumber} aggregating for ${id.body()}")
                    eventService.processLoggedEvent(id.body())
                            ?.let { vertx.eventBus().send(OrderEntityServiceEvents.BroadcastUpdate, id.body()) }
                }
    }
}

class BroadcastUpdateManagementVerticle(val entityService: OrderEntityService, val objectMapper: ObjectMapper, val numWorkers: Int): AbstractVerticle() {
    val actualNumWorkers = maxOf(1, numWorkers)

    val workers = (0..maxOf(0, actualNumWorkers - 1)).map {
        BroadcastUpdateWorkerVerticle(entityService, objectMapper, it)
    }

    override fun start() {
        workers.forEach {
            vertx.deployVerticle(it, DeploymentOptions()
                    .setWorker(true))
        }
        vertx.eventBus()
                .consumer<UUID>(OrderEntityServiceEvents.BroadcastUpdate) {
                    vertx.eventBus().send(OrderEntityServiceEvents.BroadcastUpdate + ".${Math.abs(it.body().hashCode()) % actualNumWorkers}", it.body())
                }
    }
}

class BroadcastUpdateWorkerVerticle(val entityService: OrderEntityService, val mapper: ObjectMapper, val workerNumber: Int): AbstractVerticle() {
    override fun start() {
        vertx.eventBus()
                .consumer<UUID>(OrderEntityServiceEvents.BroadcastUpdate + ".$workerNumber") { message ->
                    println("${workerNumber} broadcasting for ${message.body()}")
                    entityService.getEntityHistory(message.body())
                            .right().getOrElse { null }
                            ?.let {
                                vertx.eventBus().publish(
                                    OrderEntityServiceEvents.eventAggregatedResource(message.body()),
                                    mapper.writeValueAsString(it))
                            }
                }
    }
}
data class IdAndEvent(val id: UUID, val event: OrderEvent)
class OrderEventCodec(val orderEventSerialization: OrderEventSerialization): MessageCodec<IdAndEvent, IdAndEvent> {
    private val uuidLength = 36
    private val intLenght = 4

    override fun systemCodecID(): Byte = -1

    override fun name(): String = this.javaClass.simpleName

    override fun decodeFromWire(pos: Int, buffer: Buffer): IdAndEvent? {
        return try {
            val id = UUID.fromString(buffer.getString(pos, pos + uuidLength))
            val bodyLen = buffer.getInt(pos + uuidLength)
            val body = orderEventSerialization.deserialize(ByteBuffer.wrap(buffer.getBytes(pos + uuidLength + intLenght, pos + uuidLength + intLenght + bodyLen)))
            body.right().map { IdAndEvent(id, it) }.right().getOrElse { null }
        } catch(e: Throwable) {
            null
        }
    }

    override fun transform(s: IdAndEvent?): IdAndEvent? {
        return s
    }

    override fun encodeToWire(buffer: Buffer, s: IdAndEvent) {
        val id = s.id.toString()
        buffer.appendInt(id.length)
        buffer.appendString(id)
        val json = orderEventSerialization.serialize(s.event).array()
        buffer.appendInt(json.size)
        buffer.appendBytes(json)
    }
}

class UUIDCodec: MessageCodec<UUID, UUID> {
    val uuidLength = 36

    override fun decodeFromWire(pos: Int, buffer: Buffer?): UUID? {
        return buffer?.let { buf -> buf.getString(pos, pos + 36) }
                ?.let { Try { UUID.fromString(it) }.getOrElse { null} }
    }

    override fun systemCodecID(): Byte = -1

    override fun name(): String = this.javaClass.simpleName

    override fun transform(s: UUID?): UUID? {
        return s
    }

    override fun encodeToWire(buffer: Buffer?, s: UUID?) {
        buffer?.let{ buf -> s?.let{ id -> buf.appendString(id.toString())}}
    }
}