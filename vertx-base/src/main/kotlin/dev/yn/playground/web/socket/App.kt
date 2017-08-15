package dev.yn.playground.web.socket

import io.vertx.core.Vertx

fun main(args: Array<String>) {
    val vertx = Vertx.vertx()

    vertx.deployVerticle(WebSocketServer(8091))

    vertx.deployVerticle(WebSocketClient(8091, "client-01"))
}