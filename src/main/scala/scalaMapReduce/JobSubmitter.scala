package scalaMapReduce

import akka.actor._
import javaMapReduce.JobConf;
import common._
import scala.concurrent.duration.Duration;
import java.util.concurrent.TimeUnit;
import java.net._
import java.io._

class Job(val jc:JobConf){

	def run(){
	  implicit val system = ActorSystem("JobSubmitterSystem", Config.JobSubmitterConfig)
	  val localActor = system.actorOf(Props(new JobSubmitterActor(jc)), name = "JobSubmitterActor") 
		localActor ! Start
	}
}

object JobApp extends App {

   val ja = new Job(new JobConf())
   ja.run()
}

class JobSubmitterActor(jconf: JobConf) extends Actor {

	import context._
	// create the remote actor
	val remote = context.actorSelection("akka.tcp://JobTrackerSystem@127.0.0.1:10021/user/JobTrackerActor")

	def receive = beforeStart

	def beforeStart: Receive = {

		case Start =>
			println("JobSubmitter(BeforeStart) started working...")
			context.become(started)
		  remote ! "RequestJobID"

		case _ =>
			println("JobSubmitterActor(BeforeStart) got something unexpected.")
	}


	def started: Receive = {

		case id: Int =>
			println("JobSubmitter(started) started working...")
			if( jconf == null || !jconf.isValid())
				println("invalid job configuration.")

			// 1. request id from jobtracker.
			jconf.setJobID(id)
			if(jconf.getJobName().length() == 0)
				jconf.setJobName("Job "+ id.toString)

			// 2. Send the job to the jobtracker.
		  remote ! jconf
			self ! "stop"

		case "stop" =>
			println("stopping...")
			context.system.shutdown()

		case _ =>
			println("JobSubmitterActor(started) got something unexpected.")
	}


	def printActor(s: ActorRef){
		println(s.path.toString)
		println(s.path.address.toString)
		println(s.path.address.host)
		println(s.path.address.port)
	}
}
