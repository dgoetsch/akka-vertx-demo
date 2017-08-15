package dev.yn.playground.http

import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.*
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import org.funktionale.either.Either
import java.net.URLEncoder
//import dev.yn.util.either.*
data class HttpClientConfig(
        val host: String,
        val port: Int? = 443,
        val ssl: Boolean = true,
        val URI: String = "/",
        val headers: MutableMap<String, Iterable<String>> = mutableMapOf()
)
class YugiohHttpClientVerticle(val config: HttpClientConfig): AbstractVerticle() {

    val LOG = LoggerFactory.getLogger(this.javaClass)
    val requestOptions: RequestOptions = RequestOptions()
            .setHost(config.host)
            .setURI(config.URI)
            .setSsl(config.ssl)
            .let { config.port?.let { port -> it.setPort(port) }?:it }

    override fun start() {
        val client = vertx.createHttpClient(HttpClientOptions().setSsl(config.ssl))
        val yugiohClient = YuGiOhClientRequests(client, config)
        val fileSystem = vertx.fileSystem()
        fileSystem.mkdirBlocking("sets/")
        fileSystem.mkdirBlocking("cards/")
        yugiohClient.getSets()
                .map {
                    it.map {
                        when(it) {
                            is String -> Either.right(it)
                            else -> Either.left(ClientError.ParseError("could not handle set name", it))
                        }
                    }
                }

//        yugiohClient.getSets({ setName ->
//            fileSystem.mkdir("sets/$setName", { it -> it.map {
//                fileSystem.mkdir("cards/$setName", { it -> it.map {
//                        yugiohClient.getSetData(setName, { card ->
//                            card.getString("name")?.let { cardName ->
//                                fileSystem.writeFile(
//                                        "sets/$setName/$cardName", card.toBuffer(),
//                                        { LOG.info("wrote file: sets/$setName/$cardName") })
//                                yugiohClient.getCardData(cardName, {
//                                    println(cardName)
//                                })
//                            }
//
//                        })
//                    }
//                })
//            }})
//        })
    }
}

sealed class ClientError {
    data class Wrapper(val error: ClientError): Throwable()

    companion object {
        val logError: (ClientError) -> Unit = { clientError ->
            val LOG = LoggerFactory.getLogger(this.javaClass)

            when(clientError) {
                is ParseError -> {
                    LOG.error("Could not parse response because ${clientError.body}"
                     + "\n${clientError.body}")
                }
                is ErrorStatus -> LOG.error("Bad Response Status: ${clientError.httpClientResponse.statusCode()}")
                is ParseExceptionError -> {
                    LOG.error("Could not parse response: ${clientError.message}")
                    LOG.error(clientError.error)
                    LOG.debug(clientError.error.stackTrace.joinToString("\n\t\t"))
                }
                is ErrorResponse ->
                        LOG.error("Received an error response: ${clientError.message}" +
                                "\n${clientError.body}")

            }
        }

        fun <T> futureHandler(future: Future<T>): (ClientError) -> Unit = { future.fail(Wrapper(it)) }
    }
    class ParseError(val message: String, val body: Any): ClientError()
    class ErrorResponse(val message: String, val body: Any): ClientError()
    class ErrorStatus(val httpClientResponse: HttpClientResponse): ClientError()
    class ParseExceptionError(val message: String, val error: Throwable, body: Buffer, val httpClientResponse: HttpClientResponse): ClientError()
}
class YuGiOhClientRequests(val httpClient: HttpClient, val config: HttpClientConfig) {
    val LOG = LoggerFactory.getLogger(this.javaClass)

    val defaultHeaders: Map<String, Iterable<String>> = mapOf(
            "content-type" to listOf("application/json"))

    fun getSets(): Future<JsonArray> {
        val future: Future<JsonArray> = Future.future()
//        val handler = { jsonArray: JsonArray -> future.complete(jsonArray)
//            jsonArray.forEach {
//                when(it) {
//                    is String -> doWithSetName(it)
//                    else -> onError(ClientError.ParseError("could not handle set name", it))
//                }
//            }
//        }

        httpClient.get(
                RequestOptions()
                        .setHost(config.host)
                        .setURI("/api/card_sets")
                        .setSsl(config.ssl)
                        .let { config.port?.let { port -> it.setPort(port) }?:it })
                .handler(jsonArrayHandler(future::complete, ClientError.futureHandler(future)))
                .let(addHeaders(defaultHeaders))
                .let(addHeaders(config.headers))
                .end()

        return future
    }

    fun getSetData(setName: String): Future<JsonObject> {
        val future = Future.future<JsonObject>()

//        val handler: (JsonObject) -> Unit = {jsonObject: JsonObject ->
//            when(jsonObject.getString("status")) {
//                "success" ->
//                    jsonObject
//                            .getJsonObject("data")
//                            ?.getJsonArray("cards")
//                            ?.map{
//                                when(it) {
//                                    is JsonObject -> doWithCards(it)
//                                    else -> onError(ClientError.ParseError("card is not an object", it))
//                                }
//                            }?:onError(ClientError.ParseError("could not parse set data", jsonObject))
//                else -> onError(ClientError.ErrorResponse("response status was not a success", jsonObject))
//            }
//        }
        httpClient.get(
                RequestOptions()
                        .setHost(config.host)
                        .setURI("/api/set_data/${setName}")
                        .setSsl(config.ssl)
                        .let { config.port?.let { port -> it.setPort(port) }?:it })
                .handler(jsonObjectHandler(future::complete, ClientError.futureHandler(future)))
                .let(addHeaders(defaultHeaders))
                .let(addHeaders(config.headers))
                .end()

        return future
    }

    fun getCardData(cardName: String): Future<JsonObject> {
        val future = Future.future<JsonObject>()
        httpClient.get(
                RequestOptions()
                        .setHost(config.host)
                        .setURI("/api/card_data/${cardName}")
                        .setSsl(config.ssl)
                        .let { config.port?.let { port -> it.setPort(port) } ?: it })
                .handler(jsonObjectHandler(future::complete, ClientError.futureHandler(future)))
                .let(addHeaders(defaultHeaders))
                .let(addHeaders(config.headers))
                .end()

        return future
    }

    private fun <T> baseHandler(parse: (HttpClientResponse, Buffer) -> T):
            (HttpClientResponse, (T) -> Unit, (ClientError) -> Unit) -> Unit = {
        response: HttpClientResponse, handle: (T) -> Unit, onError: (ClientError) -> Unit ->
        when(response.statusCode()/100) {
            2 ->
                response.bodyHandler { body ->
                    try {
                        handle(parse(response, body))
                    } catch(e: Throwable) {
                        onError(ClientError.ParseExceptionError("could not parse body", e, body, response))
                    }
                }
            else -> onError(ClientError.ErrorStatus(response))
        }
    }

    private fun jsonArrayHandler(
            handle: (JsonArray) -> Unit,
            onError: (ClientError) -> Unit
    ): (HttpClientResponse) -> Unit  = { response: HttpClientResponse ->
        baseHandler { httpClientResponse, buffer ->  buffer.toJsonArray() } (response, handle, onError)
    }

    private fun jsonObjectHandler(
            handle: (JsonObject) -> Unit,
            onError: (ClientError) -> Unit
    ): (HttpClientResponse) -> Unit = { response: HttpClientResponse ->
        baseHandler { httpClientResponse, buffer ->  buffer.toJsonObject() } (response, handle, onError)
    }



    fun addHeaders(headers: Map<String, Iterable<String>>): (HttpClientRequest) -> HttpClientRequest  = { request ->
        headers.forEach { headerName, headerValues ->
            request.putHeader(headerName, headerValues)
        }
        request
    }
}