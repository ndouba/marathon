package mesosphere.marathon
package core.election

import akka.NotUsed
import akka.actor.{ ActorSystem, Cancellable }
import akka.stream.ClosedShape
import akka.stream.scaladsl.{ Broadcast, GraphDSL, RunnableGraph, Flow, Keep, Sink, Source }
import akka.stream.{ ActorMaterializer, OverflowStrategy }
import com.typesafe.scalalogging.StrictLogging
import java.util
import java.util.Collections
import java.util.concurrent.{ ExecutorService, Executors, TimeUnit }
import kamon.Kamon
import kamon.metric.instrument.Time
import mesosphere.marathon.core.async.ExecutionContexts
import mesosphere.marathon.core.base.CrashStrategy
import mesosphere.marathon.metrics.{ Metrics, ServiceMetric, Timer }
import mesosphere.marathon.stream.Repeater
import org.apache.curator.framework.api.ACLProvider
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.curator.framework.recipes.leader.{ LeaderLatch, LeaderLatchListener }
import org.apache.curator.framework.{ AuthInfo, CuratorFrameworkFactory }
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.data.ACL
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal
import scala.util.{ Failure, Success }

/**
  * ElectionService is implemented by leadership election mechanisms.
  *
  * This trait is used in conjunction with [[ElectionCandidate]]. From their point of view,
  * a leader election works as follow:
  *
  * -> ElectionService.offerLeadership(candidate)     |      - A leader election is triggered.
  *                                                          — Once `candidate` is elected as a leader,
  *                                                            its `startLeadership` is called.
  *
  * Please note that upon a call to [[ElectionService.abdicateLeadership]], or
  * any error in any of method of [[ElectionService]], or a leadership loss,
  * [[ElectionCandidate.stopLeadership]] is called if [[ElectionCandidate.startLeadership]]
  * has been called before, and JVM gets shutdown.
  *
  * It effectively means that a particular instance of Marathon can be elected at most once during its lifetime.
  */
trait ElectionService {
  /**
    * isLeader checks whether this instance is the leader
    *
    * @return true if this instance is the leader
    */
  def isLeader: Boolean

  /**
    * localHostPort return a host:port pair of this running instance that is used for discovery.
    *
    * @return host:port of this instance.
    */
  def localHostPort: String

  /**
    * leaderHostPort return a host:port pair of the leader, if it is elected.
    *
    * @return Some(host:port) of the leader, or None if no leader exists or is known
    */
  def leaderHostPort: Option[String]

  /**
    * abdicateLeadership is called to resign from leadership.
    */
  def abdicateLeadership(): Unit

  def leaderStateEvents: Source[LeadershipState, Cancellable]
  def leaderTransitionEvents: Source[LeadershipTransition, Cancellable]
}

class ElectionServiceImpl(
    hostPort: String,
    leaderEventsSource: Source[LeadershipState, Cancellable],
    crashStrategy: CrashStrategy)(implicit system: ActorSystem) extends ElectionService with StrictLogging {

  import ElectionService._
  @volatile private[this] var lastState: LeadershipState = LeadershipState.Standby(None)
  implicit private lazy val materializer = ActorMaterializer()

  override def isLeader: Boolean =
    lastState == LeadershipState.ElectedAsLeader

  override def localHostPort: String = hostPort

  override def leaderHostPort: Option[String] = lastState match {
    case LeadershipState.ElectedAsLeader =>
      Some(hostPort)
    case LeadershipState.Standby(currentLeader) =>
      currentLeader
  }

  override def abdicateLeadership(): Unit = {
    leaderSubscription.cancel()
  }

  private def initializeStream() = {
    val localEventListenerSink = Sink.foreach[LeadershipState] { event =>
      lastState = event
    }
    val leadershipTransitionsBroadcast = new Repeater[LeadershipTransition](32, OverflowStrategy.fail)
    val leaderEventsBroadcastSink = new Repeater[LeadershipState](32, OverflowStrategy.fail)

    val (leaderStream, leaderStreamDone, leaderTransitionEvents, leaderStateEvents) =
      RunnableGraph.fromGraph(GraphDSL.create(
        leaderEventsSource, localEventListenerSink, leadershipTransitionsBroadcast, leaderEventsBroadcastSink)(
        (_, _, _, _)) { implicit b =>
          { (leaderEventsSource, localEventListenerSink, leadershipTransitionsBroadcast, leaderEventsBroadcastSink) =>
            import GraphDSL.Implicits._
            val broadcast = b.add(Broadcast[LeadershipState](4))
            leaderEventsSource ~> broadcast.in
            broadcast ~> localEventListenerSink
            broadcast ~> leaderEventsBroadcastSink
            broadcast ~> leaderTransitionFlow ~> leadershipTransitionsBroadcast
            broadcast ~> leaderTransitionFlow ~> metricsSink
            ClosedShape
          }
        }).run

    // When the leadership stream terminates, for any reason, we suicide
    leaderStreamDone.onComplete {
      case Failure(ex) =>
        logger.info(s"Leadership ended with failure; exiting", ex)
        crashStrategy.crash()
      case Success(_) =>
        logger.info(s"Leadership ended gracefully; exiting")
        crashStrategy.crash()
    }(ExecutionContexts.callerThread)

    (leaderStream, leaderTransitionEvents, leaderStateEvents)
  }

  val (leaderSubscription, leaderTransitionEvents, leaderStateEvents) = initializeStream()
}

object ElectionService extends StrictLogging {
  private val leaderDurationMetric = "service.mesosphere.marathon.leaderDuration"

  val metricsSink = Sink.foreach[LeadershipTransition] {
    case LeadershipTransition.ObtainedLeadership =>
      val startedAt = System.currentTimeMillis()
      Kamon.metrics.gauge(leaderDurationMetric, Time.Milliseconds)(System.currentTimeMillis() - startedAt)
    case LeadershipTransition.LostLeadership =>
      Kamon.metrics.removeGauge(leaderDurationMetric)
  }

  def leaderTransitionFlow: Flow[LeadershipState, LeadershipTransition, NotUsed] =
    Flow[LeadershipState].statefulMapConcat { () =>
      var haveLeadership = false

      {
        case LeadershipState.ElectedAsLeader if !haveLeadership =>
          haveLeadership = true
          List(LeadershipTransition.ObtainedLeadership)
        case LeadershipState.Standby(_) if haveLeadership =>
          haveLeadership = false
          List(LeadershipTransition.LostLeadership)
        case _ =>
          Nil
      }
    }

  val emptyCancellableSource: Source[Nothing, Cancellable] =
    Source.queue[Nothing](1, OverflowStrategy.fail).mapMaterializedValue { m =>
      new Cancellable {
        @volatile var _cancelled = false
        override def cancel(): Boolean = {
          m.complete()
          _cancelled = true
          true
        }
        override def isCancelled: Boolean = _cancelled
      }
    }

  /**
    * Stream which immediately emits that we are leader; upon cancellation, emits that we are not
    */
  def psuedoElectionStream(): Source[LeadershipState, Cancellable] = {
    Source.single(LeadershipState.ElectedAsLeader)
      .concatMat(emptyCancellableSource)(Keep.right)
      .concat(Source.single(LeadershipState.Standby(None)))
  }

  /**
    * Connects to Zookeeper and offers leadership; monitors leader state. Watches for leadership changes (leader
    * changed, was elected leader, lost leadership), and emits events accordingly.
    *
    * Materialized cancellable is used to abdicate leadership; which will do so followed by a closing of the stream.
    */
  def curatorElectionStream(config: ZookeeperConf, hostPort: String)(implicit actorSystem: ActorSystem): Source[LeadershipState, Cancellable] = {
    val leaderEvents = Source.queue[LeadershipState](16, OverflowStrategy.dropHead).mapMaterializedValue { m =>
      val threadExecutor: ExecutorService = Executors.newSingleThreadExecutor()
      implicit val singleThreadEC: ExecutionContext = ExecutionContext.fromExecutor(threadExecutor)
      val leaderHostPortMetric: Timer = Metrics.timer(ServiceMetric, getClass, "current-leader-host-port")

      val init = Future {
        val client = newCuratorConnection(config)

        val latch = new LeaderLatch(client, config.zooKeeperLeaderPath + "-curator", hostPort)

        var _isLeader: Boolean = false
        var _currentLeader: Option[String] = None

        def getCurrentLeader: Option[String] = leaderHostPortMetric.blocking {
          if (client.getState == CuratorFrameworkState.STOPPED)
            None
          else {
            try {
              val participant = latch.getLeader
              if (participant.isLeader) Some(participant.getId) else None
            } catch {
              case NonFatal(ex) =>
                logger.error("Error while getting current leader", ex)
                None
            }
          }
        }

        val listener: LeaderLatchListener = new LeaderLatchListener {
          override def notLeader(): Unit = {
            _isLeader = true
            _currentLeader = getCurrentLeader
            m.offer(LeadershipState.Standby(_currentLeader))
            logger.info(s"Leader defeated. New leader: ${_currentLeader.getOrElse("-")}")
          }

          override def isLeader(): Unit = {
            _isLeader = true
            m.offer(LeadershipState.ElectedAsLeader)
          }
        }

        latch.addListener(listener, threadExecutor)
        latch.start()
        // Note: runs in same executor as listener; therefore serial
        actorSystem.scheduler.schedule(1.seconds, 1.second){
          if (!_isLeader) {
            val nextLeader = getCurrentLeader
            if (nextLeader != _currentLeader) {
              logger.info(s"New leader: ${nextLeader.getOrElse("-")}")
              _currentLeader = nextLeader
              m.offer(LeadershipState.Standby(nextLeader))
            }
          }
        }

        // Note: runs in same executor as listener; therefore serial
        m.watchCompletion().onComplete {
          case _ =>
            latch.removeListener(listener)
            logger.info(s"Closing leader latch")
            latch.close()
            logger.info(s"Leader latch closed")
        }
      }
      init.onFailure { case ex => m.fail(ex) }

      new Cancellable {
        @volatile private var _cancelled = false
        override def cancel(): Boolean = {
          _cancelled = true
          m.complete()
          true
        }

        override def isCancelled: Boolean = _cancelled
      }
    }

    // When the stream closes, we want the last word to be that there is no leader
    leaderEvents
      .initialTimeout(config.zooKeeperConnectionTimeout().millis)
      .concat(Source.single(LeadershipState.Standby(None)))
  }

  def newCuratorConnection(config: ZookeeperConf) = {
    logger.info(s"Will do leader election through ${config.zkHosts}")

    // let the world read the leadership information as some setups depend on that to find Marathon
    val defaultAcl = new util.ArrayList[ACL]()
    defaultAcl.addAll(config.zkDefaultCreationACL)
    defaultAcl.addAll(ZooDefs.Ids.READ_ACL_UNSAFE)

    val aclProvider = new ACLProvider {
      override def getDefaultAcl: util.List[ACL] = defaultAcl
      override def getAclForPath(path: String): util.List[ACL] = defaultAcl
    }

    val retryPolicy = new ExponentialBackoffRetry(1.second.toMillis.toInt, 10)
    val builder = CuratorFrameworkFactory.builder().
      connectString(config.zkHosts).
      sessionTimeoutMs(config.zooKeeperSessionTimeout().toInt).
      connectionTimeoutMs(config.zooKeeperConnectionTimeout().toInt).
      aclProvider(aclProvider).
      retryPolicy(retryPolicy)

    // optionally authenticate
    val client = (config.zkUsername, config.zkPassword) match {
      case (Some(user), Some(pass)) =>
        builder.authorization(Collections.singletonList(
          new AuthInfo("digest", (user + ":" + pass).getBytes("UTF-8"))))
          .build()
      case _ =>
        builder.build()
    }

    client.start()
    client.blockUntilConnected(config.zkTimeoutDuration.toMillis.toInt, TimeUnit.MILLISECONDS)
    client
  }
}

/** Local leadership events. They are not delivered via the event endpoints. */
sealed trait LeadershipState

object LeadershipState {
  case object ElectedAsLeader extends LeadershipState
  case class Standby(currentLeader: Option[String]) extends LeadershipState
}

sealed trait LeadershipTransition
object LeadershipTransition {
  case object LostLeadership extends LeadershipTransition
  case object ObtainedLeadership extends LeadershipTransition
}
