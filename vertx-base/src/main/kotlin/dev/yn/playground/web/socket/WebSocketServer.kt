package dev.yn.playground.web.socket

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.http.ServerWebSocket
import java.util.*

class WebSocketServer(val port: Int): AbstractVerticle() {

    override fun start(startFuture: Future<Void>?) {
        vertx.createHttpServer().websocketHandler { webSocket: ServerWebSocket ->
            val context = UUID.randomUUID()
            println("[SERVER][$context] received request" +
                    "\t\npath:\t${webSocket.path()}" +
                    "\t\nquery:\t${webSocket.query()}" +
                    "\t\nheaders:\t${webSocket.headers()}" +
                    "\t\nuri:\t${webSocket.uri()}")
            webSocket.textMessageHandler { textMessage ->
               respond(context, textMessage, webSocket)
            }
        }.listen(port)
    }

    fun respond(context: UUID, message: String, webSocket: ServerWebSocket) {
        println("[SERVER] writing message to client")
        webSocket.writeTextMessage("response: $context")
    }
}