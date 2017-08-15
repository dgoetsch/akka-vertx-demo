package dev.yn.akka

import java.nio.ByteBuffer
import java.util.{Date, UUID}

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpEntity, _}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, Materializer}
import com.fasterxml.jackson.databind.ObjectMapper
import dev.yn.akka.OrderAggregateManagementActor.EventLogged
import dev.yn.entity.domain.EntityError
import dev.yn.event.service.EventService
import dev.yn.restauraunt.order.{OrderEntityService, OrderEntityServiceFactory}
import dev.yn.restauraunt.order.domain.OrderEvent

import scala.concurrent.{ExecutionContext, Future}
import scala.io.StdIn


object AkkaApp {
  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("order")
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

    val mapper = new ObjectMapper()
    mapper.registerModule(new com.fasterxml.jackson.module.kotlin.KotlinModule())

    val orderEntityService: OrderEntityService = OrderEntityServiceFactory.INSTANCE.default()
    val orderAggregateManagementActor = system.actorOf(Props(new OrderAggregateManagementActor(orderEntityService.getEventService)), "aggregate")
    orderAggregateManagementActor ! OrderAggregateManagementActor.Start(16)
    val logOrderEventActorRef = system.actorOf(Props(new LogOrderEventActor(orderEntityService, orderAggregateManagementActor)), "log")
    val readOrderActor = system.actorOf(Props(new ReadOrderActor(orderEntityService)), "read")


    val unmarshaller = new Unmarshaller[HttpRequest, Either[EntityError, OrderEvent]]() {
      import KotlinAdapters.FunktionaleEitherAdapter
      override def apply(value: HttpRequest)(implicit ec: ExecutionContext, materializer: Materializer): Future[Either[EntityError, OrderEvent]] = {
        value.entity.dataBytes.runFold[Either[EntityError, OrderEvent]](Left(new EntityError.JsonError(new RuntimeException("no body")))) { case (a, b) =>
          orderEntityService.getEventSerialization
            .deserialize(ByteBuffer.wrap(b.decodeString("UTF-8").getBytes()))
            .toScala()
        }
      }
    }

    import KotlinAdapters.FunktionaleEitherAdapter
    val routes = {
      path("orders" / JavaUUID ) { orderId =>
        put {
          entity(unmarshaller) {
            case Right(orderEvent) =>
              logOrderEventActorRef ! IdAndEvent(orderId, orderEvent)
              complete(200, {
                Future.successful(HttpEntity(ContentTypes.`text/plain(UTF-8)`, orderId.toString))
              })
            case Left(error) =>
              failWith(new RuntimeException("bad request body"))
          }
        } ~
        get {

          complete(200, {
            Future.successful(orderEntityService.getEntityHistory(orderId).toScala())
                .flatMap {
                  case Right(history) => Future.successful(HttpEntity(ContentTypes.`application/json`, mapper.writeValueAsString(history)))
                  case _ => Future.successful(HttpEntity(ContentTypes.`application/json`, "{\"error\":\"Not Found\"}"))
                }
          })
        }
      } ~
      path("orders") {
        post {
          entity(unmarshaller) {
            case Right(orderEvent) =>
              val orderId = UUID.randomUUID()
              logOrderEventActorRef ! IdAndEvent(orderId, orderEvent)
              complete(201, {
                Future.successful(HttpEntity(ContentTypes.`text/plain(UTF-8)`, orderId.toString))
              })
            case Left(error) =>
              failWith(new RuntimeException("bad request body"))
          }
        }
      }
    }

    val bindingFuture = Http().bindAndHandle(routes, "localhost", 8080)

    println(s"Server online at http://localhost:8080/\nType 'exit\\n' to stop...")
    while(!Option(StdIn.readLine()).exists(_.contains("exit"))) {}// let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }
}

case class IdAndEvent(id: UUID, event: OrderEvent)

class LogOrderEventActor(val entityService: OrderEntityService, val orderAggregateManagementActor: ActorRef) extends Actor with ActorLogging {
  import KotlinAdapters.FunktionaleEitherAdapter

  override def receive: Receive =  {
    case IdAndEvent(id, event) => {
      entityService.logEvent(id, event)
          .toScala()
          .right.map { _ => orderAggregateManagementActor ! EventLogged(id) }
    }
  }
}

class ReadOrderActor(val entityService: OrderEntityService) extends Actor with ActorLogging {
  import ReadOrderActor._
  import KotlinAdapters.FunktionaleEitherAdapter

  override def receive: Receive = {
    case ReadStateAndHistory(id) => entityService.getEntityHistory(id)
        .toScala()
        .right.map { history => sender() ! history }
        .right.getOrElse { sender() ! "failure" }
  }
}

object ReadOrderActor {
  sealed trait Message
  case class ReadStateAndHistory(id: UUID) extends Message
}

class OrderAggregateManagementActor(val eventService: EventService) extends Actor with ActorLogging {
  import OrderAggregateManagementActor._

  override def receive: Receive = eventLoggers(0)

  def eventLoggers(numWorkers: Int): Receive = {
    case Start(newNumWorkers) =>
      log.info(s"changing workers from $numWorkers to $newNumWorkers")
      if(newNumWorkers > 0) {
        if (newNumWorkers > numWorkers) {
          Range(numWorkers, newNumWorkers).foreach { workerNumber =>
            context.actorOf(Props(new OrderAggregateWorkerActor(eventService)), s"worker-$workerNumber")
          }
        } else if (newNumWorkers < numWorkers) {
          Range(newNumWorkers - 1, numWorkers)
            .flatMap { workerNumber => context.child(s"worker-$workerNumber")}
            .foreach(context.stop)
        }
        context.become(eventLoggers(newNumWorkers))
      }
    case EventLogged(id) => {
      context
        .child(s"worker-${math.abs(id.hashCode() % numWorkers)}")
        .foreach(_ ! EventLogged(id))
    }
  }
}

object OrderAggregateManagementActor {
  sealed trait Message
  case class Start(numWorkers: Int) extends Message
  case class EventLogged(id: UUID) extends Message
}

class OrderAggregateWorkerActor(val eventService: EventService) extends Actor with ActorLogging {
  //by default this will restart if there is an exception
  override def receive: Receive = {
    case OrderAggregateManagementActor.EventLogged(id) => {
      Option(eventService.getLatest(id))
        .foreach(eventService.processLoggedEvent)
    }
  }
}