package io.reactors.concurrent

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import io.reactors.{Channel, Reactor, ReactorSystem}
import org.scalameter.api.{MongoDbReporter, RegressionReporter}
import org.scalameter.{Context, Reporter}
import org.scalameter.api._
import org.scalameter.japi.JBench

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._
import scala.util.Random

/**
  * Created by remi on 27/03/17.
  */
class QuickSortBench extends JBench.OfflineReport {

  override def defaultConfig = Context(
    exec.minWarmupRuns -> 20,
    exec.maxWarmupRuns -> 40,
    exec.benchRuns -> 48,
    exec.independentSamples -> 4,
    verbose -> true
  )

  override def reporter = Reporter.Composite(
    new RegressionReporter(tester, historian),
    new MongoDbReporter[Double]
  )

  val lists: Gen[List[Long]] = Gen.range("size")(1000000, 10000000, 1000000).map(QuickSortConfig.randomlyInitArray(_))

  @transient lazy val system = new ReactorSystem("reactor-bench")

  @gen("lists")
  @benchmark("io.reactors.quicksort")
  @curve("onEvent")
  def reactorOnEvent(list: List[Long]) = {

    val done = Promise[List[Long]]()

    def quicksort(parent: Channel[Message], positionRelativeToParent: Position): Channel[Message] = {

      val ch = system.spawn(Reactor[Message] { self =>

        var result = List.empty[Long]
        var numFragments = 0


        def notifyParentAndTerminate() {
          parent ! ResultMessage(result, positionRelativeToParent)
          self.main.seal()
        }

        self.main.events.onMatch {
          case SortMessage(data) =>

            val dataLength: Int = data.length
            if (dataLength < QuickSortConfig.T) {

              result = data.sorted
              notifyParentAndTerminate()

            } else {

              val dataLengthHalf = dataLength / 2
              val pivot = data(dataLengthHalf)

              val leftUnsorted = data.filter(_ < pivot)
              val rightUnsorted = data.filter(_ > pivot)
              result = data.filter(_ == pivot)

              val leftActor = quicksort(self.main.channel, PositionLeft)
              leftActor ! SortMessage(leftUnsorted)

              val rightActor = quicksort(self.main.channel, PositionRight)
              rightActor ! SortMessage(rightUnsorted)

              numFragments += 1
            }

          case ResultMessage(data, position) =>

            if (data.nonEmpty) {

              if (position eq PositionLeft) {

                result = data ++ result

              } else if (position eq PositionRight) {

                result = data ++ result

              }
            }

            numFragments += 1
            if (numFragments == 3) {
              notifyParentAndTerminate()
            }
        }

      })
      ch
    }

    system.spawn(Reactor[Message] { self =>
      self.main.events.onEvent {
        case ResultMessage(l, PositionInitial) => {
          val sorted = QuickSortConfig.checkSorted(l.length, l)
          if(sorted) done.success(l)
          else done.failure(new RuntimeException("List isn't sorted"))
        }
      }
      quicksort(self.main.channel, PositionInitial) ! SortMessage(list)
    })

    Await.ready(done.future, 10.seconds)
  }

  var actorSystem: ActorSystem = _

  def akkaSetup() {
    actorSystem = ActorSystem("actor-bench")
  }

  def akkaTeardown() {
    actorSystem.shutdown()
  }

  @gen("lists")
  @benchmark("io.reactors.quicksort")
  @curve("akka")
  @setupBeforeAll("akkaSetup")
  @teardownAfterAll("akkaTeardown")
  def akka(l: List[Long]): Unit = {

    val done = Promise[List[Long]]()

    actorSystem.actorOf(
      Props.create(classOf[QuickSortRootActor], done, l))

    Await.ready(done.future, 10.seconds)
  }


}

protected object QuickSortConfig {

  private val M: Long = 1L << 60 // max value
  val T = 2048 // threshold to perform sort sequentially
  private val S = 1024 // seed for random number generator

 def checkSorted(N: Long, data: List[Long]): Boolean = {
   val length = data.length

   if (length != N) {
     return false
   }

   data.zip(data.tail).forall(x => x._1 <= x._2)

  }

 def randomlyInitArray(N: Long): List[Long] = {

   val l = List.newBuilder[Long]

   val random = new Random(S)

   (0L until N).foreach{_ => l += Math.abs(random.nextLong() % M)}

   l.result()
  }

}

abstract class Position

case object PositionRight extends Position

case object PositionLeft extends Position

case object PositionInitial extends Position

abstract class Message

case class SortMessage(data: List[Long]) extends Message

case class ResultMessage(data: List[Long], position: Position) extends Message

class QuickSortRootActor(done: Promise[List[Long]], list: List[Long]) extends Actor {
  override def preStart() {
    val child = context.actorOf(Props.create(classOf[QuickSortActor], self, PositionInitial))
    child ! SortMessage(list)
  }
  def receive: Receive = {
    case ResultMessage(l, PositionInitial) => {
      val sorted = QuickSortConfig.checkSorted(l.length, l)
      if(sorted) done.success(l)
      else done.failure(new RuntimeException("List isn't sorted"))
    }
  }
}

class QuickSortActor(parent: ActorRef, positionRelativeToParent: Position) extends Actor {

  private var result = List.empty[Long]
  private var numFragments = 0

  def notifyParentAndTerminate() {
    parent ! ResultMessage(result, positionRelativeToParent)
    context.stop(self)
  }

  override def receive: Receive = {
      case SortMessage(data) =>

        val dataLength: Int = data.length
        if (dataLength < QuickSortConfig.T) {

          result = data.sorted
          notifyParentAndTerminate()

        } else {

          val dataLengthHalf = dataLength / 2
          val pivot = data(dataLengthHalf)

          val leftUnsorted = data.filter(_ < pivot)
          val rightUnsorted = data.filter(_ > pivot)
          result = data.filter(_ == pivot)

          val leftActor = context.system.actorOf(Props.create(classOf[QuickSortActor], self, PositionLeft))
          leftActor ! SortMessage(leftUnsorted)

          val rightActor = context.system.actorOf(Props.create(classOf[QuickSortActor], self, PositionRight))
          rightActor ! SortMessage(rightUnsorted)

          numFragments += 1
        }

      case ResultMessage(data, position) =>

        if (data.nonEmpty) {

          if (position eq PositionLeft) {

            result = data ++ result

          } else if (position eq PositionRight) {

            result = data ++ result

          }
        }

        numFragments += 1
        if (numFragments == 3) {
          notifyParentAndTerminate()
        }
  }
}

