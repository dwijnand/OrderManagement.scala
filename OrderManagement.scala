package sample.eventdriven.scala

import akka.actor.{Actor, ActorRef, ActorSystem, Inbox, Props}
import akka.persistence.PersistentActor

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration._

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

  // =========================================================
  // Commands
  // =========================================================
  sealed trait Command
  final case class CreateOrder(userId: Int, productId: Int) extends Command
  final case class ReserveProduct(userId: Int, productId: Int) extends Command
  final case class SubmitPayment(userId: Int, productId: Int) extends Command
  final case class ShipProduct(userId: Int, txId: Int) extends Command

  // =========================================================
  // Events
  // =========================================================
  sealed trait Event
  final case class ProductReserved(userId: Int, txId: Int) extends Event
  final case class ProductOutOfStock(userId: Int, productId: Int) extends Event
  final case class PaymentAuthorized(userId: Int, txId: Int) extends Event
  final case class PaymentDeclined(userId: Int, txId: Int) extends Event
  final case class ProductShipped(userId: Int, txId: Int) extends Event
  final case class OrderFailed(userId: Int, txId: Int, reason: String) extends Event
  final case class OrderCompleted(userId: Int, txId: Int) extends Event

  // =========================================================
  // Top-level service functioning as a Process Manager
  // Coordinating the workflow on behalf of the Client
  // =========================================================
  class Orders(client: ActorRef, inventory: ActorRef, payment: ActorRef) extends Actor {

    override def preStart = {
      system.eventStream.subscribe(self, classOf[ProductReserved])   // Subscribe to ProductReserved Events
      system.eventStream.subscribe(self, classOf[ProductOutOfStock]) // Subscribe to ProductOutOfStock Events
      system.eventStream.subscribe(self, classOf[ProductShipped])    // Subscribe to ProductShipped Events
      system.eventStream.subscribe(self, classOf[PaymentAuthorized]) // Subscribe to PaymentAuthorized Events
      system.eventStream.subscribe(self, classOf[PaymentDeclined])   // Subscribe to PaymentDeclined Events
    }

    def receive = {
      case cmd: CreateOrder =>                                          // 1. Receive CreateOrder Command
        inventory.tell(ReserveProduct(cmd.userId, cmd.productId), self) // 2. Send ReserveProduct Command to Inventory
        println(s"COMMAND:\t\t$cmd => ${self.path.name}")

      case evt: ProductReserved =>                                      // 3. Receive ProductReserved Event
        payment.tell(SubmitPayment(evt.userId, evt.txId), self)         // 4. Send SubmitPayment Command to Payment
        println(s"EVENT:\t\t\t$evt => ${self.path.name}")

      case evt: ProductOutOfStock =>                                         // ALT 3. Receive ProductOutOfStock Event
        client.tell(OrderFailed(evt.userId, evt.txId, "out of stock"), self) // ALT 4. Send OrderFailed Event back to Client
        println(s"EVENT:\t\t\t$evt => ${self.path.name}")
        Behaviors.same

      case evt: PaymentAuthorized =>                                    // 5. Receive PaymentAuthorized Event
        inventory.tell(ShipProduct(evt.userId, evt.txId), self)         // 6. Send ShipProduct Command to Inventory
        println(s"EVENT:\t\t\t$evt => ${self.path.name}")

      case evt: PaymentDeclined =>                                           // ALT 5. Receive PaymentDeclined Event
        client.tell(OrderFailed(evt.userId, evt.txId, "out of stock"), self) // ALT 6. Send OrderFailed Event back to Client
        println(s"EVENT:\t\t\t$evt => ${self.path.name}")
        Behaviors.same

      case evt: ProductShipped =>                                       // 7. Receive ProductShipped Event
        client.tell(OrderCompleted(evt.userId, evt.txId), self)         // 8. Send OrderCompleted Event back to Client
        println(s"EVENT:\t\t\t$evt => ${self.path.name}")
    }
  }

  // =========================================================
  // Event Sourced Aggregate
  // =========================================================
  class Inventory extends PersistentActor {
    val persistenceId = "Inventory"

    var nrOfProductsShipped = 0 // Mutable state, persisted in memory (AKA Memory Image)

    def reserveProduct(userId: Int, productId: Int): Event = {
      println(s"SIDE-EFFECT:\tReserving Product => ${self.path.name}")
      ProductReserved(userId, productId)
    }

    def shipProduct(userId: Int, txId: Int): Event = {
      println(s"SIDE-EFFECT:\tShipping Product => ${self.path.name}")
      ProductShipped(userId, txId)
    }

    def receiveCommand = {
      case cmd: ReserveProduct =>                                     // Receive ReserveProduct Command
        val productStatus = reserveProduct(cmd.userId, cmd.productId) // Try to reserve the product
        persist(productStatus) { event =>                             // Try to persist the Event
          context.system.eventStream.publish(event)                   // If successful, publish Event to Event Stream
        }
        println(s"COMMAND:\t\t$cmd => ${self.path.name}")

      case cmd: ShipProduct =>                                        // Receive ShipProduct Command
        val shippingStatus = shipProduct(cmd.userId, cmd.txId)        // Try to ship the product
        persist(shippingStatus) { event =>                            // Try to persist the Event
          context.system.eventStream.publish(event)                   // If successful, publish Event to Event Stream
        }
        println(s"COMMAND:\t\t$cmd => ${self.path.name}")
    }

    def receiveRecover = {
      case event: ProductReserved => // Replay the ProductReserved events
        println(s"EVENT (REPLAY):\t$event => ${self.path.name}")

      case event: ProductShipped =>  // Replay the ProductShipped events
        nrOfProductsShipped += 1
        println(s"EVENT (REPLAY):\t$event => ${self.path.name} - ProductsShipped: $nrOfProductsShipped")
    }
  }

  // =========================================================
  // Event Sourced Aggregate
  // =========================================================
  class Payment extends PersistentActor {
    val persistenceId = "Payment"

    var uniqueTransactionNr = 0 // Mutable state, persisted in memory (AKA Memory Image)

    def processPayment(userId: Int, txId: Int): Event = {
      uniqueTransactionNr += 1
      println(s"SIDE-EFFECT:\tProcessing Payment => ${self.path.name} - TxNumber: $uniqueTransactionNr")
      PaymentAuthorized(userId, uniqueTransactionNr)
    }

    def receiveCommand = {
      case cmd: SubmitPayment =>                                      // Receive SubmitPayment Command
        val paymentStatus = processPayment(cmd.userId, cmd.productId) // Try to pay product
        persist(paymentStatus) { event =>                             // Try to persist Event
          context.system.eventStream.publish(event)                   // If successful, publish Event to Event Stream
        }
        println(s"COMMAND:\t\t$cmd => ${self.path.name}")
    }


    def receiveRecover = {
      case evt: PaymentAuthorized => // Replay the PaymentAuthorized events
        uniqueTransactionNr += 1
        println(s"EVENT (REPLAY):\t$evt => ${self.path.name} - TxNumber: $uniqueTransactionNr")

      case evt: PaymentDeclined =>   // Replay the PaymentDeclined events
        println(s"EVENT (REPLAY):\t$evt => ${self.path.name}")
    }
  }

  // =========================================================
  // Running the Order Management simulation
  // =========================================================
  val system = ActorSystem("OrderManagement")

  // Plumbing for "client"
  val clientInbox = Inbox.create(system)
  val client = clientInbox.getRef()

  // Create the services (cheating with "DI" by exploiting enclosing object scope)
  val inventory = system.actorOf(Props(classOf[Inventory]), "Inventory")
  val payment   = system.actorOf(Props(classOf[Payment]), "Payment")
  val orders    = system.actorOf(Props(classOf[Orders], client, inventory, payment), "Orders")

  // Submit an order
  clientInbox.send(orders, CreateOrder(9, 1337)) // Send a CreateOrder Command to the Orders service
  clientInbox.receive(5.seconds) match {         // Wait for OrderCompleted Event
    case confirmation: OrderCompleted =>
      println(s"EVENT:\t\t\t$confirmation => Client")
  }

  system.terminate().foreach(_ => println("System has terminated"))
}
