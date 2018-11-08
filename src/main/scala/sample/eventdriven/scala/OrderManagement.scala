package sample.eventdriven.scala

import akka.actor.typed.{ ActorRef, ActorSystem, Behavior }
import akka.actor.typed.scaladsl.{ Behaviors, EventStream }
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.typed.scaladsl.PersistentBehaviors
import akka.persistence.typed.scaladsl.Effect
import akka.util.Timeout

import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration._

import ActorContextExt.ops._

// ===============================================================
// Demo of an Event-driven Architecture in Akka and Scala.
//
// Show-casing:
//   1. Events-first Domain Driven Design using Commands and Events
//   2. Asynchronous communication through an Event Stream
//   3. Asynchronous Process Manager driving the workflow
//   4. Event-sourced Aggregates
//
// Used in my talk on 'How Events Are Reshaping Modern Systems'.
//
// NOTE: This is a very much simplified and dumbed down sample
//       that is by no means a template for production use.
//       F.e. in a real-world app I would not use Serializable
//       but a JSON, Protobuf, Avro, or some other good lib.
//       I would not use Akka's built in EventStream, but
//       probably Kafka or Kinesis. Etc.
// ===============================================================

object OrderManagement extends App {

  sealed trait ClientMessage
  sealed trait ClientCommand extends ClientMessage
  sealed trait ClientEvent extends ClientMessage

  sealed trait OrderMessage
  sealed trait OrderCommand extends OrderMessage
  sealed trait OrderEvent extends OrderMessage

  sealed trait InventoryCommand
  sealed trait InventoryEvent
  final case class InventoryState(nrOfProductsShipped: Int = 0)

  sealed trait PaymentCommand
  sealed trait PaymentEvent
  final case class PaymentState(uniqueTransactionNr: Int = 0)

  // =========================================================
  // Commands
  // =========================================================
  sealed trait Command
  final case class ClientCreateOrder(order: CreateOrder, replyTo: ActorRef[ClientEvent]) extends Command with ClientCommand
  final case class CreateOrder(userId: Int, productId: Int) extends Command with OrderCommand
  final case class ReserveProduct(userId: Int, productId: Int) extends Command with InventoryCommand
  final case class SubmitPayment(userId: Int, productId: Int) extends Command with PaymentCommand
  final case class ShipProduct(userId: Int, txId: Int) extends Command with InventoryCommand

  // =========================================================
  // Events
  // =========================================================
  sealed trait Event
  final case class ProductReserved(userId: Int, txId: Int) extends Event with OrderEvent with InventoryEvent
  final case class ProductOutOfStock(userId: Int, txId: Int) extends Event with OrderEvent
  final case class PaymentAuthorized(userId: Int, txId: Int) extends Event with OrderEvent with PaymentEvent
  final case class PaymentDeclined(userId: Int, txId: Int) extends Event with OrderEvent with PaymentEvent
  final case class ProductShipped(userId: Int, txId: Int) extends Event with OrderEvent with InventoryEvent
  final case class OrderFailed(userId: Int, txId: Int, reason: String) extends Event with ClientEvent
  final case class OrderCompleted(userId: Int, txId: Int) extends Event with ClientEvent

  // =========================================================
  // Top-level service functioning as a Process Manager
  // Coordinating the workflow on behalf of the Client
  // =========================================================
  def mkOrders(
      client: ActorRef[ClientEvent],
      inventory: ActorRef[InventoryCommand],
      payment: ActorRef[PaymentCommand],
  ): Behavior[OrderCommand] =
    Behaviors.setup[OrderMessage] { context =>
      val self = context.self.toUntyped

      context.eventStream.subscribe(classOf[OrderEvent]) // Subscribe to OrderEvent Events

      Behaviors.receiveMessage {
        case cmd: CreateOrder =>                                          // 1. Receive CreateOrder Command
          inventory.tell(ReserveProduct(cmd.userId, cmd.productId))       // 2. Send ReserveProduct Command to Inventory
          println(s"COMMAND:\t\t$cmd => ${self.path.name}")
          Behaviors.same

        case evt: ProductReserved =>                                      // 3. Receive ProductReserved Event
          payment.tell(SubmitPayment(evt.userId, evt.txId))               // 4. Send SubmitPayment Command to Payment
          println(s"EVENT:\t\t\t$evt => ${self.path.name}")
          Behaviors.same

        case evt: ProductOutOfStock =>                                    // ALT 3. Receive ProductOutOfStock Event
          client.tell(OrderFailed(evt.userId, evt.txId, "out of stock"))  // ALT 4. Send OrderFailed Event back to Client
          println(s"EVENT:\t\t\t$evt => ${self.path.name}")
          Behaviors.same

        case evt: PaymentAuthorized =>                                    // 5. Receive PaymentAuthorized Event
          inventory.tell(ShipProduct(evt.userId, evt.txId))               // 6. Send ShipProduct Command to Inventory
          println(s"EVENT:\t\t\t$evt => ${self.path.name}")
          Behaviors.same

        case evt: PaymentDeclined =>                                      // ALT 5. Receive PaymentDeclined Event
          client.tell(OrderFailed(evt.userId, evt.txId, "out of stock"))  // ALT 6. Send OrderFailed Event back to Client
          println(s"EVENT:\t\t\t$evt => ${self.path.name}")
          Behaviors.same

        case evt: ProductShipped =>                                       // 7. Receive ProductShipped Event
          client.tell(OrderCompleted(evt.userId, evt.txId))               // 8. Send OrderCompleted Event back to Client
          println(s"EVENT:\t\t\t$evt => ${self.path.name}")
          Behaviors.same
      }
    }.narrow

  // =========================================================
  // Event Sourced Aggregate
  // =========================================================
  def mkInventory: Behavior[InventoryCommand] = Behaviors.setup { context =>
    val persistenceId = "Inventory"

    val eventStream = EventStream.withEventType[InventoryEvent](context)

    val self = context.self.toUntyped

    def reserveProduct(userId: Int, productId: Int): InventoryEvent = {
      println(s"SIDE-EFFECT:\tReserving Product => ${self.path.name}")
      ProductReserved(userId, txId = productId) // TODO: txId = productId ???
    }

    def shipProduct(userId: Int, txId: Int): InventoryEvent = {
      println(s"SIDE-EFFECT:\tShipping Product => ${self.path.name}")
      ProductShipped(userId, txId)
    }

    def receiveCommand: (InventoryState, InventoryCommand) ⇒ Effect[InventoryEvent, InventoryState] =
      PersistentBehaviors.CommandHandler.command {
        case cmd: ReserveProduct =>                                     // Receive ReserveProduct Command
          val productStatus = reserveProduct(cmd.userId, cmd.productId) // Try to reserve the product
          println(s"COMMAND:\t\t$cmd => ${self.path.name}")
          Effect.persist(productStatus).thenRun { _ =>                  // Try to persist the Event
            eventStream.publish(productStatus)                          // If successful, publish Event to Event Stream
          }

        case cmd: ShipProduct =>                                        // Receive ShipProduct Command
          val shippingStatus = shipProduct(cmd.userId, cmd.txId)        // Try to ship the product
          println(s"COMMAND:\t\t$cmd => ${self.path.name}")
          Effect.persist(shippingStatus).thenRun { _ =>                 // Try to persist the Event
            eventStream.publish(shippingStatus)                         // If successful, publish Event to Event Stream
          }
      }

    def receiveRecover: (InventoryState, InventoryEvent) ⇒ InventoryState = {
      case (state, event: ProductReserved) => // Replay the ProductReserved events
        println(s"EVENT (REPLAY):\t$event => ${self.path.name}")
        state

      case (state, event: ProductShipped) =>  // Replay the ProductShipped events
        val newState = state.copy(nrOfProductsShipped = state.nrOfProductsShipped + 1)
        println(s"EVENT (REPLAY):\t$event => ${self.path.name} - ProductsShipped: ${newState.nrOfProductsShipped}")
        newState
    }

    PersistentBehaviors.receive[InventoryCommand, InventoryEvent, InventoryState](
      persistenceId = persistenceId,
      emptyState = InventoryState(),
      commandHandler = receiveCommand,
      eventHandler = receiveRecover,
    )
  }

  // =========================================================
  // Event Sourced Aggregate
  // =========================================================
  def mkPayment: Behavior[PaymentCommand] = Behaviors.setup { context =>
    val persistenceId = "Payment"

    val eventStream = EventStream.withEventType[PaymentEvent](context)

    val self = context.self.toUntyped

    def processPayment(userId: Int, txId: Int): PaymentEvent = {
      println(s"SIDE-EFFECT:\tProcessing Payment => ${self.path.name}")
      PaymentAuthorized(userId, txId)
    }

    def receiveCommand: (PaymentState, PaymentCommand) ⇒ Effect[PaymentEvent, PaymentState] =
      PersistentBehaviors.CommandHandler.command {
        case cmd: SubmitPayment =>                                      // Receive SubmitPayment Command
          val paymentStatus = processPayment(cmd.userId, cmd.productId) // Try to pay product
          println(s"COMMAND:\t\t$cmd => ${self.path.name}")
          Effect.persist(paymentStatus).thenRun { _ =>                  // Try to persist Event
            eventStream.publish(paymentStatus)                          // If successful, publish Event to Event Stream
          }
      }

    def receiveRecover: (PaymentState, PaymentEvent) ⇒ PaymentState = {
      case (state, evt: PaymentAuthorized) => // Replay the PaymentAuthorized events
        val newState = state.copy(uniqueTransactionNr = state.uniqueTransactionNr + 1)
        println(s"EVENT (REPLAY):\t$evt => ${self.path.name} - TxNumber: ${newState.uniqueTransactionNr}")
        newState

      case (state, evt: PaymentDeclined) =>   // Replay the PaymentDeclined events
        println(s"EVENT (REPLAY):\t$evt => ${self.path.name}")
        state
    }

    PersistentBehaviors.receive[PaymentCommand, PaymentEvent, PaymentState](
      persistenceId = persistenceId,
      emptyState = PaymentState(),
      commandHandler = receiveCommand,
      eventHandler = receiveRecover,
    )
  }

  // =========================================================
  // Running the Order Management simulation
  // =========================================================
  def mkOrderManagement: Behavior[ClientCommand] = Behaviors.setup[ClientCommand] { context =>
    val inventory = context.spawn(mkInventory, "Inventory")
    val payment = context.spawn(mkPayment, "Payment")

    Behaviors.receiveMessage {
      case cmd: ClientCreateOrder =>
        val orders = context.spawn(mkOrders(cmd.replyTo, inventory, payment), "Orders").toUntyped
        orders ! cmd.order
        Behaviors.same
    }
  }.narrow
  val system = ActorSystem(mkOrderManagement, "OrderManagement")

  import akka.actor.typed.scaladsl.AskPattern._
  implicit val ec: ExecutionContext = system.executionContext
  implicit val timeout = Timeout(5.seconds + 1.minute)
  implicit val scheduler = system.scheduler

  // Submit an order
  // Ask the system to create an order
  val result: Future[ClientEvent] = system ? (ref => ClientCreateOrder(CreateOrder(9, 1337), ref))

  Await.result(result, Duration.Inf) match { // Wait for OrderCompleted Event
    case confirmation: ClientEvent =>
      println(s"EVENT:\t\t\t$confirmation => Client")
  }

  system.terminate().foreach(_ => println("System has terminated"))
}
