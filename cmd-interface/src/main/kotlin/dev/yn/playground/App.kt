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

package dev.yn.playground

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import dev.yn.restauant.user.domain.UserRef
import dev.yn.restauraunt.order.domain.LineItem
import dev.yn.restauraunt.order.domain.OrderEvent
import dev.yn.restauraunt.order.domain.OrderEventType
import dev.yn.restauraunt.order.serialization.OrderEventSerialization
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.http.HttpClient
import io.vertx.core.http.HttpClientResponse
import io.vertx.core.http.RequestOptions
import io.vertx.core.json.JsonObject
import java.util.*


fun main(args: Array<String>): Unit {
    val host = "localhost"
    val port = 8080
    val ssl = false
    val uri= "/order"
    val baseRequestOptions = RequestOptions().setHost(host).setPort(port).setSsl(ssl).setURI(uri)
    val vertx = Vertx.vertx()
    val mapper = jacksonObjectMapper()
    fun randomAddItem(): JsonObject {
        val rand: Double = Math.random()
        val lineItems = (0..Math.floor(rand * 4.0).toInt()).map {
            LineItem("item:${it*rand}", emptyList())
        }

        val userRef = UserRef(UUID.randomUUID(), "User-$rand", listOf("guest"))
        return JsonObject.mapFrom(OrderEvent.AddLineItems(lineItems, userRef))
    }
    fun HttpClient.webSocket(id: UUID) {
        vertx.createHttpClient().websocket(
                baseRequestOptions.setURI("/orders/$id"),
                { socket ->
                    socket.textMessageHandler {
                        println("received websocket message: ${it}")
                    }
                    socket.exceptionHandler {
                        println("websocketException: $it")
                    }
                }
        )
    }
    val httpClient = vertx.createHttpClient()
    val orderEventJson = JsonObject(mutableMapOf("eventType" to OrderEventType.Open.name, "user" to mutableMapOf("id" to UUID.randomUUID(), "name" to "Patty", "roles" to listOf("customer"))))
    httpClient
            .openOrder(baseRequestOptions, orderEventJson)
            .map { id ->
                httpClient.webSocket(id)
                vertx.setPeriodic(2000) {
                    httpClient.putOrder(baseRequestOptions, id, randomAddItem())
                            .setHandler {
                                if(it.succeeded()) {
                                    it.result().bodyHandler {
                                        println("current state: ${String(it.bytes)}")
                                    }
                                } else {
                                    println("failed to get item because ${it.cause()}")
                                }
                            }
                }
                id }
            .setHandler {
                if(it.succeeded()) {
                    println("open result: ${it.toString()}")
                } else {
                    println("request failed : ${it.cause()}")
                }
            }

}



fun HttpClient.getOrder(requestOptions: RequestOptions, id: UUID): Future<HttpClientResponse> {
    val getFuture = Future.future<HttpClientResponse>()
    get(requestOptions.setURI("/orders/${id}"))
            .handler {
                if(it.statusCode() / 100 == 2) {
                    getFuture.complete(it)
                } else {
                    getFuture.fail("Get: Unsuccessful status code: ${it.statusCode()}")
                }
            }.end()
    return getFuture
}

fun HttpClient.putOrder(requestOptions: RequestOptions, id: UUID, json: JsonObject): Future<HttpClientResponse> {
    val future = Future.future<HttpClientResponse>()
    put(requestOptions.setURI("/orders/$id"))
            .handler {
                future.complete(it)
            }.end(json.encode())
    return future
}

fun HttpClient.openOrder(requestOptions: RequestOptions, json: JsonObject): Future<UUID> {
    val openFuture = Future.future<UUID>()
    post(requestOptions.setURI("/orders"))
            .handler {
                if(it.statusCode() / 100 == 2) {
                    it.bodyHandler {
                        try {
                            openFuture.complete(UUID.fromString(String(it.bytes)))
                        } catch(e: Throwable) {
                            openFuture.fail(e)
                        }
                    }
                } else {
                    openFuture.fail("Open: Unsuccessful status code: ${it.statusCode()}")
                }
            }.end(json.encode())
    return openFuture
}