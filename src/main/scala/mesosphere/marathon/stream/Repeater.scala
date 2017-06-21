package mesosphere.marathon
package stream

import akka.actor.Cancellable
import akka.stream.scaladsl.{ Source, SourceQueueWithComplete }
import akka.stream.stage.{ GraphStageLogic, InHandler, GraphStageWithMaterializedValue }
import akka.stream.{ Attributes, Inlet, OverflowStrategy, SinkShape }
import scala.collection.mutable

/**
  * Sink which works similar to BroadcastHub, but does not buffer and will emit the last element recieved to any new
  * subscribers. If there are no subscribers, then it behaves as Sink.ignore (continually remembering the last element
  * received)
  *
  * All messages are relayed asynchronously, the individual consumption rates of each subscriber do not affect
  * others. Backpressure mechanism not supported.
  *
  * Upstream completion or failures are propagated to the subscribers (event late subscribers)
  */
class Repeater[T](bufferSize: Int, overflowStrategy: OverflowStrategy)
    extends GraphStageWithMaterializedValue[SinkShape[T], Source[T, Cancellable]] {

  require(overflowStrategy != OverflowStrategy.backpressure, "backpressure not supported for repeater sink")

  val input = Inlet[T]("Repeater.in")

  override def shape: SinkShape[T] = new SinkShape(input)
  type SQ = SourceQueueWithComplete[T]
  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Source[T, Cancellable]) = {
    class RepeaterLogic extends GraphStageLogic(shape) {
      type Result = Either[Option[T], Throwable]

      private def propagateCompletion(sq: SQ, result: Result) = result match {
        case Left(result) =>
          result.foreach(sq.offer)
          sq.complete()
        case Right(ex) => sq.fail(ex)
      }

      class AtomicGate {
        private[this] var finished: Option[Result] = None
        private[this] var pending = List.empty[SQ]
        def offer(sq: SQ): Unit = synchronized {
          finished match {
            case Some(result) =>
              propagateCompletion(sq, result)
            case None =>
              pending = sq :: pending
              scheduleTransfer.invoke(())
          }
        }

        def close(result: Result): Unit = synchronized { finished = Some(result) }
        def consume(): List[SQ] = synchronized {
          val l = pending
          pending = Nil
          l
        }
      }

      private[this] var lastElement: T = _
      private[this] var subscribers = mutable.Set.empty[SQ]

      private[this] def transfer(): Unit = {
        val newSubscribers = gate.consume
        if (lastElement != null)
          newSubscribers.foreach(_.offer(lastElement))
        subscribers ++= newSubscribers
      }
      val gate = new AtomicGate

      setHandler(input, new InHandler {
        override def onPush(): Unit = {
          lastElement = grab(input)
          pull(input)
          subscribers.foreach(_.offer(lastElement))
        }

        override def onUpstreamFinish(): Unit =
          finalize(Left(Option(lastElement)))

        override def onUpstreamFailure(ex: Throwable): Unit =
          finalize(Right(ex))

        private[this] def finalize(result: Result): Unit = {
          gate.close(result)
          transfer()
          subscribers.foreach(propagateCompletion(_, result))
          subscribers.clear()
        }
      })

      val scheduleTransfer = getAsyncCallback[Unit] { _ => transfer() }
      val remove = getAsyncCallback[SQ] { sq =>
        transfer()
        subscribers -= sq
      }

      override def preStart(): Unit = {
        pull(input)
      }
    }

    val logic = new RepeaterLogic
    val mat = Source.queue[T](bufferSize, overflowStrategy).mapMaterializedValue { sq =>
      logic.gate.offer(sq)
      new Cancellable {
        private var _cancelled = false
        override def cancel(): Boolean = {
          logic.remove.invoke(sq)
          sq.complete()
          _cancelled = true
          true
        }
        override def isCancelled: Boolean = _cancelled
      }
    }
    (logic, mat)
  }
}
