package sample.eventdriven.scala

import scala.language.implicitConversions

import akka.actor.typed.scaladsl.{ ActorContext, EventStream }

final class ActorContextExt[T <: AnyRef](private val context: ActorContext[T]) extends AnyVal {
  def eventStream: EventStream[AnyRef] = {
    import akka.actor.typed.scaladsl.adapter._
    new EventStream[AnyRef](context.system.toUntyped.eventStream, context.self.toUntyped)
  }
}

object ActorContextExt {
  object ops {
    implicit def toActorContextExt[T <: AnyRef](x: ActorContext[T]): ActorContextExt[T] =
      new ActorContextExt[T](x)
  }
}
