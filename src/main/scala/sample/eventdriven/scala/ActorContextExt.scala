package sample.eventdriven.scala

import scala.language.implicitConversions

import akka.actor.typed.scaladsl.{ ActorContext, EventStream }
import akka.actor.typed.scaladsl.adapter._

final class ActorContextExt[T <: AnyRef](private val context: ActorContext[T]) extends AnyVal {
  def eventStream[E <: T]: EventStream[E] =
    new EventStream[E](context.self.toUntyped, context.system.toUntyped)
}

object ActorContextExt {
  object ops {
    implicit def toActorContextExt[T <: AnyRef](x: ActorContext[T]): ActorContextExt[T] =
      new ActorContextExt[T](x)
  }
}
