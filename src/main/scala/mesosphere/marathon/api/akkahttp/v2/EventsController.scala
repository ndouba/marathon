package mesosphere.marathon
package api.akkahttp.v2

import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.EventStream
import akka.http.scaladsl.model.RemoteAddress
import akka.http.scaladsl.server.Route
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{ Sink, Source }
import com.typesafe.scalalogging.StrictLogging
import de.heikoseeberger.akkasse.ServerSentEvent
import mesosphere.marathon.api.akkahttp.Controller
import mesosphere.marathon.api.v2.json.Formats
import mesosphere.marathon.core.async.ExecutionContexts
import mesosphere.marathon.core.election.{ ElectionService, LeadershipState }
import mesosphere.marathon.core.event.{ EventConf, EventStreamAttached, EventStreamDetached, MarathonEvent }
import mesosphere.marathon.plugin.auth.{ Authenticator, AuthorizedResource, Authorizer, ViewResource }
import mesosphere.marathon.stream.{ EnrichedFlow, EnrichedSource }
import play.api.libs.json.Json

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
  * The EventsController provides a route to all MarathonEvents published via the event bus.
  */
class EventsController(
    val conf: EventConf,
    val eventBus: EventStream)(
    implicit
    val actorSystem: ActorSystem,
    val executionContext: ExecutionContext,
    val authenticator: Authenticator,
    val authorizer: Authorizer,
    val electionService: ElectionService
) extends Controller with StrictLogging {

  import mesosphere.marathon.api.akkahttp.Directives._
  import de.heikoseeberger.akkasse.EventStreamMarshalling._

  /**
    * GET /v2/events SSE endpoint
    * Query Parameters: event_type*
    * Listen to all MarathonEvents published via the event stream.
    */
  def eventsSSE(): Route = {
    def isAllowed(allowed: Set[String])(event: MarathonEvent): Boolean = allowed.isEmpty || allowed(event.eventType)
    authenticated.apply { implicit identity =>
      authorized(ViewResource, AuthorizedResource.Events).apply {
        parameters('event_type.*) { events =>
          extractClientIP { clientIp =>
            complete {

              EventsController.eventStreamLogic(eventBus, electionService.leaderStateEvents,
                conf.eventStreamMaxOutstandingMessages(), clientIp)
                .filter(isAllowed(events.toSet))
                .map(event => ServerSentEvent(`type` = event.eventType, data = Json.stringify(Formats.eventToJson(event))))
                .keepAlive(5.second, () => ServerSentEvent.heartbeat)
            }
          }
        }
      }
    }
  }

  override val route: Route = {
    asLeader(electionService) {
      get {
        pathEnd {
          eventsSSE()
        }
      }
    }
  }
}

object EventsController extends StrictLogging {
  /**
    * An event source which:
    * - Yields all MarathonEvent's for the event bus whilst a leader.
    * - is leader aware. The stream completes if this instance abdicates.
    * - publishes an EventStreamAttached when the stream is materialized
    * - publishes an EventStreamDetached when the stream is completed or fails
    * @param eventStream the event stream to subscribe to
    * @param bufferSize the size of events to buffer, if there is no demand.
    * @param remoteAddress the remote address
    */
  def eventStreamLogic(eventStream: EventStream, leaderEvents: Source[LeadershipState, Any], bufferSize: Int, remoteAddress: RemoteAddress) = {
    val notifyAttach: Sink[MarathonEvent, NotUsed] = Sink.ignore.mapMaterializedValue { m =>
      eventStream.publish(EventStreamAttached(remoteAddress = remoteAddress.toString()))
      logger.info(s"EventStream attached: $remoteAddress")

      m.onComplete {
        case _ =>
          eventStream.publish(EventStreamDetached(remoteAddress = remoteAddress.toString()))
          logger.info(s"EventStream detached: $remoteAddress")
      }(ExecutionContexts.callerThread)
      NotUsed
    }

    // Used to propagate a "stream close" signal when we see a LeadershipState.Standy event
    val leaderLossKillSwitch: Source[Nothing, Any] =
      leaderEvents.collect { case evt: LeadershipState.Standby => evt }.take(1).via(EnrichedFlow.ignore)

    EnrichedSource.eventBusSource(classOf[MarathonEvent], eventStream, bufferSize, OverflowStrategy.fail)
      .merge(leaderLossKillSwitch, eagerComplete = true)
      .alsoTo(notifyAttach)
  }
}
