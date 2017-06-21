package mesosphere.marathon
package stream

import akka.NotUsed
import akka.stream.scaladsl.Flow

object EnrichedFlow {
  @SuppressWarnings(Array("AsInstanceOf"))
  def ignore[T]: Flow[T, Nothing, NotUsed] =
    Flow[T].filter(_ => false).asInstanceOf[Flow[T, Nothing, NotUsed]]
}
