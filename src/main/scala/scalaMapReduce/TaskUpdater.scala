package scalaMapReduce

import akka.actor._
import common._
import scala.concurrent.duration.Duration;
import java.util.concurrent.TimeUnit;
import java.net._
import java.io._
import javaMapReduce.TaskProgress;

class TaskUpdater(val progress:TaskProgress, val name: String){

	def run(){
		val system = ActorSystem("TaskUpdaterSystem" + name);
 		val tuActor = system.actorOf(Props(new TaskUpdateActor(1)) , name = "TaskUpdateActor")
		tuActor ! Start
		tuActor ! progress
	}
}


class TaskUpdateActor(taskID: Int) extends Actor {

	import context._
	// create the remote actor
  val remote = context.actorSelection("akka.tcp://TaskTracker" + taskID.toString() + "System@127.0.0.1:10041/user/TaskTrackerActor")

	def receive = transmit

	def transmit: Receive = {

		case Start =>
			println("JobSubmitter started working...")

		case "message" =>
			println("JobSubmitterActor receive a message.")

		case prog: TaskProgress =>
			remote ! prog;

		case _ =>
			println("JobSubmitterActor(BeforeStart) got something unexpected.")
	}

	def printActor(s: ActorRef){
		println(s.path.toString)
		println(s.path.address.toString)
		println(s.path.address.host)
		println(s.path.address.port)
	}

}
