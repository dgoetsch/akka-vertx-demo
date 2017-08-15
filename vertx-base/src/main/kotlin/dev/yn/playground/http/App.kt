package dev.yn.playground.http

import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx

fun main(args: Array<String>) {
    val vertx = Vertx.vertx()

    val config = HttpClientConfig(
            host = "yugiohprices.com",
            URI = "/api/card_sets",
            headers = mutableMapOf("content-type" to listOf("application/json"))
    )

    vertx.deployVerticle(YugiohHttpClientVerticle(config), DeploymentOptions().setWorker(true).setWorkerPoolSize(8))
//    Thread.sleep(5000)
//    vertx.close()
}