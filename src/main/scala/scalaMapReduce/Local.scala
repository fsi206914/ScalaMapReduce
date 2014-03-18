package scalaMapReduce

import akka.actor._
import common._
import scala.concurrent.duration.Duration;
import java.util.concurrent.TimeUnit;

object Local extends App {

  implicit val system = ActorSystem("LocalSystem", Config.JobSubmitterConfig)
  val localActor = system.actorOf(Props[LocalActor], name = "LocalActor")  // the local actor
  localActor ! Start                                                       // start the action

}

class LocalActor extends Actor {

  import context._
  // create the remote actor
  implicit val system = ActorSystem("LocalSystem", Config.JobSubmitterConfig)
  val remote = context.actorFor("JobTrackerSystem.akka://HelloRemoteSystem@127.0.0.1:7890/user/RemoteActor")
  var counter = 0

  def receive = {
    case Start =>
			// new Thread(new Runnable(){

			// 	def run() {
			// 		system.scheduler.schedule(Duration.create(1000, TimeUnit.MILLISECONDS), Duration.create(1000, TimeUnit.MILLISECONDS),
			// 			remote, Message("foo"));
			// 	}
			// }).start;
      // println("after schedule.")
      val r = remote
      printActor(r)
      println("woca")
//      remote ! Message("woca")
      remote ! "woca"
    case Message(msg) => 
        println(s"LocalActor received message: '$msg'")
        if (counter < 5) {
            sender ! End
            counter += 1
        }
    case _ =>
        println("LocalActor got something unexpected.")
  }

  def printActor(s: ActorRef){
    println(s.path.toString)
    println(s.path.address.toString)
    println(s.path.address.host)
    println(s.path.address.port)
  }

}
