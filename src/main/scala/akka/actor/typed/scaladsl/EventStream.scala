package akka.actor.typed.scaladsl

import scala.language.implicitConversions

import akka.actor.ActorRef

final class EventStream[E <: AnyRef](eventStream: akka.event.EventStream, self: ActorRef) {
  type Event = E
  type Classifier = Class[_ <: E]
  type Subscriber = ActorRef

  def subscribe(to: Class[_ <: E]): Boolean     = eventStream.subscribe(self, to)
  def unsubscribe(from: Class[_ <: E]): Boolean = eventStream.unsubscribe(self, from)
  def unsubscribe(): Unit                       = eventStream.unsubscribe(self)
  def publish(event: E): Unit                   = eventStream.publish(event)

  def narrow[A <: E]: EventStream[A] = this.asInstanceOf[EventStream[A]]
}
