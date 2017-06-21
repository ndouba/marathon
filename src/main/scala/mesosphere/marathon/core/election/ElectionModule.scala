package mesosphere.marathon
package core.election

import akka.actor.ActorSystem
import akka.event.EventStream
import mesosphere.marathon.core.base.{ CrashStrategy, LifecycleState }
import mesosphere.marathon.core.async.ExecutionContexts
import scala.concurrent.duration._

class ElectionModule(
    config: MarathonConf,
    system: ActorSystem,
    eventStream: EventStream,
    hostPort: String,
    lifecycleState: LifecycleState,
    crashStrategy: CrashStrategy) {

  lazy val electionBackend = if (config.highlyAvailable()) {
    config.leaderElectionBackend.get match {
      case Some("curator") =>
        ElectionService.curatorElectionStream(config, hostPort)(system)
      case backend: Option[String] =>
        throw new IllegalArgumentException(s"Leader election backend $backend not known!")
    }
  } else {
    ElectionService.psuedoElectionStream()
  }

  private def onSuicide(): Unit = {
    crashStrategy.crash()
  }

  lazy val service: ElectionService = new ElectionServiceImpl(hostPort, electionBackend, crashStrategy)(system)
}
