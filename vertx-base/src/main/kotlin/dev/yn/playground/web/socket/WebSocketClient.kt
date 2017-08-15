package dev.yn.playground.web.socket

import io.vertx.core.AbstractVerticle
import io.vertx.core.http.RequestOptions
import io.vertx.core.http.WebSocket

class WebSocketClient(val port: Int, val name: String): AbstractVerticle() {

    val host = "localhost"

    val requestOptions = RequestOptions()
            .setHost(host)
            .setPort(port)
            .setURI("/path/to/service")
            .setSsl(false)

    override fun start() {
        vertx.createHttpClient().websocket(requestOptions, { webSocket: WebSocket ->
            webSocket.textMessageHandler { textMessage: String ->
                println("[$name] received: $textMessage")
                webSocket.close()
            }


            println("[$name] writing to server")
            webSocket.writeTextMessage("Message to start!")
        })

    }

}