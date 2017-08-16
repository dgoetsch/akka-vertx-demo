package dev.yn.akka

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import java.util.{Date, UUID}

import akka.pattern.ask
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpEntity.Strict
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.model.{HttpEntity, _}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import com.fasterxml.jackson.databind.ObjectMapper
import dev.yn.entity.domain.{EntityError, HistoryContainer}
import dev.yn.event.serialization.Serialization
import dev.yn.restauraunt.order.{OrderEntityService, OrderEntityServiceFactory}
import dev.yn.restauraunt.order.domain.{OrderEvent, OrderState}

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
    val orderAggregateManagementActor = system.actorOf(Props(new OrderAggregateManagementActor(orderEntityService)), "aggregate")
    orderAggregateManagementActor ! Events.Start(16)
    val logOrderEventActorRef = system.actorOf(Props(new LogOrderEventActor(orderEntityService, orderAggregateManagementActor)), "log")
    val readOrderActor = system.actorOf(Props(new ReadOrderActor(orderEntityService)), "read")
    implicit val timeout = akka.util.Timeout(1, TimeUnit.SECONDS)

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

    def readEvent(orderId: UUID): Future[Strict] = {
      (readOrderActor ? Events.ReadStateAndHistory(orderId)).map {
        case Events.StateAndHistory(history) => HttpEntity(ContentTypes.`application/json`, mapper.writeValueAsString(history))
        case _ => HttpEntity(ContentTypes.`application/json`, "{\"error\":\"Not Found\"}")
      }
    }

    val routes = {
      path("orders" / JavaUUID ) { orderId =>
        handleWebSocketMessages(Flow.fromSinkAndSource[Message, Message](
          Sink.ignore,
          Source.actorPublisher[Message](Props(new WebSocketPublisher(orderId, orderEntityService.getEventSerialization, mapper))))) ~
        put {
          entity(unmarshaller) {
            case Right(orderEvent) =>
              logOrderEventActorRef ! Events.IdAndEvent(orderId, orderEvent)
              complete(200, {
                Future.successful(HttpEntity(ContentTypes.`text/plain(UTF-8)`, orderId.toString))
              })
            case Left(error) =>
              failWith(new RuntimeException("bad request body"))
          }
        } ~
        get {
          complete {
            readEvent(orderId)
          }
        }
      } ~
      path("orders") {
        post {
          entity(unmarshaller) {
            case Right(orderEvent) =>
              val orderId = UUID.randomUUID()
              logOrderEventActorRef ! Events.IdAndEvent(orderId, orderEvent)
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


object Events {
  case class OrderEventLogged(id: UUID, event: OrderEvent)
  case class OrderHistoryUpdated(history: HistoryContainer[OrderState, OrderEvent, EntityError])
  case class IdAndEvent(id: UUID, event: OrderEvent)
  case class ReadStateAndHistory(id: UUID)
  case class NotFound(id: UUID)
  case class StateAndHistory(history: HistoryContainer[OrderState, OrderEvent, EntityError])
  case class Start(numWorkers: Int)
  case class EventLogged(id: UUID)
  case class BroadCastUpdate(id: UUID)
}


class LogOrderEventActor(val entityService: OrderEntityService, val orderAggregateManagementActor: ActorRef) extends Actor with ActorLogging {
  import KotlinAdapters.FunktionaleEitherAdapter

  override def receive: Receive =  {
    case Events.IdAndEvent(id, event) => {
      entityService.logEvent(id, event)
        .toScala()
        .right.foreach { _ =>
        orderAggregateManagementActor ! Events.EventLogged(id)
        context.system.eventStream.publish(Events.OrderEventLogged(id, event))
      }
    }
  }
}

class ReadOrderActor(val entityService: OrderEntityService) extends Actor with ActorLogging {
  import KotlinAdapters.FunktionaleEitherAdapter

  override def receive: Receive = {
    case Events.ReadStateAndHistory(id) => entityService.getEntityHistory(id)
        .toScala()
        .right.map { history => sender() ! Events.StateAndHistory(history) }
        .right.getOrElse { sender() ! Events.NotFound(id) }
  }
}

class WebSocketPublisher(val id: UUID, val orderEventSerialization: Serialization[EntityError, OrderEvent], val mapper: ObjectMapper) extends ActorPublisher[Message] with ActorLogging {
  override def preStart(): Unit = {
    context.system.eventStream.subscribe(this.self, classOf[Events.OrderEventLogged])
    context.system.eventStream.subscribe(this.self, classOf[Events.OrderHistoryUpdated])
  }

  override def postStop(): Unit = {
    context.system.eventStream.unsubscribe(this.self)
  }

  override def receive: Receive = {
    case Events.OrderEventLogged(eventId, event) if eventId == id => {
      onNext(TextMessage(new String(orderEventSerialization.serialize(event).array())))
    }
    case Events.OrderHistoryUpdated(history) => {
      onNext(TextMessage(mapper.writeValueAsString(history)))
    }
    case _ =>
  }
}

class OrderAggregateManagementActor(val orderEntityService: OrderEntityService) extends Actor with ActorLogging {

  override def receive: Receive = eventLoggers(0)

  def eventLoggers(numWorkers: Int): Receive = {
    case Events.Start(newNumWorkers) =>
      log.info(s"changing workers from $numWorkers to $newNumWorkers")
      if(newNumWorkers > 0) {
        if (newNumWorkers > numWorkers) {
          Range(numWorkers, newNumWorkers).foreach { workerNumber =>
            context.actorOf(Props(new OrderAggregateWorkerActor(orderEntityService)), s"worker-$workerNumber")
          }
        } else if (newNumWorkers < numWorkers) {
          Range(newNumWorkers - 1, numWorkers)
            .flatMap { workerNumber => context.child(s"worker-$workerNumber")}
            .foreach(context.stop)
        }
        context.become(eventLoggers(newNumWorkers))
      }
    case Events.EventLogged(id) => {
      context
        .child(s"worker-${math.abs(id.hashCode() % numWorkers)}")
        .foreach(_ ! Events.EventLogged(id))
    }
  }
}


class OrderAggregateWorkerActor(val orderEntityService: OrderEntityService) extends Actor with ActorLogging {
  //by default this will restart if there is an exception
  override def preStart(): Unit = {
    context.actorOf(Props(new UpdateBroadcaster(orderEntityService)), s"broadcaster")
  }

  override def receive: Receive = {
    case Events.EventLogged(id) => {
      Option(orderEntityService.getEventService().getLatest(id))
        .foreach { event =>
          orderEntityService.getEventService.processLoggedEvent(event)
          context.child("broadcaster").foreach(_ ! Events.BroadCastUpdate(id))
        }
    }
  }
}

class UpdateBroadcaster(val orderEntityService: OrderEntityService) extends Actor with ActorLogging {
  import KotlinAdapters.FunktionaleEitherAdapter
  override def receive: Receive = {
    case Events.BroadCastUpdate(id) =>
      orderEntityService.getEntityHistory(id)
        .toScala()
        .right.foreach { history => context.system.eventStream.publish(Events.OrderHistoryUpdated(history)) }
  }
}