package akka.actor.typed.scaladsl

import scala.language.implicitConversions

import akka.{ actor => untyped }
import akka.actor.typed.ActorRef

final class EventStream[E <: AnyRef](self: untyped.ActorRef, system: untyped.ActorSystem) {
  type Event = E
  type Classifier = Class[_ <: E]
  type Subscriber = ActorRef[E]

  def subscribe(to: Class[_ <: E]): Boolean     = system.eventStream.subscribe(self, to)
  def unsubscribe(from: Class[_ <: E]): Boolean = system.eventStream.unsubscribe(self, from)
  def unsubscribe(): Unit                       = system.eventStream.unsubscribe(self)
  def publish(event: E): Unit                   = system.eventStream.publish(event)
}

object EventStream {
  // WORKAROUND for akka/akka#25887
  def fromAnyActorContext[E <: AnyRef](context: ActorContext[_]): EventStream[E] = {
    import akka.actor.typed.scaladsl.adapter._
    new EventStream[E](context.self.toUntyped, context.system.toUntyped)
  }
}
