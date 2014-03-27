package scalaMapReduce

import akka.actor._
import javaMapReduce.JobConf;
import common._
import scala.concurrent.duration.Duration;
import java.util.concurrent.TimeUnit;
import java.net._
import java.io._

object JobSubmitterApp extends App {

  implicit val system = ActorSystem("JobSubmitterSystem", Config.JobSubmitterConfig)
  val localActor = system.actorOf(Props[JobSubmitterActor], name = "JobSubmitterActor")  // the local actor
  localActor ! Start      

  // start the action

// 	try {
// 			val serverSocket = new ServerSocket(6789)
// 			println("Receiving input through socket...")
// 			while(true) {
// 				val clientSocket = serverSocket.accept()
// 				println("accept")
// /*       val in = new BufferedInputStream(clientSocket.getInputStream());*/
// 				//val out = new BufferedOutputStream(new FileOutputStream("newWelcome"));

// 				//var c = in.read() 
// 				//while (c != -1) {
// 					//println("sdfsd");
// 					//out.write(c);
// 					//c = in.read()
// 				//}
// 				//out.flush();
// 				//in.close();
// 				/*out.close();*/
// 			}
// 	} catch {
// 		case e: IOException =>
// 			println("Exception caught when trying to listen on port 6789 or listening for a connection")
// 			println(e.getMessage)
// 			e.printStackTrace()
// 	}
}

class JobSubmitterActor extends Actor {

	import context._
		// create the remote actor
		implicit val system = ActorSystem("JobSubmitterSystem", Config.JobSubmitterConfig)
		val remote = context.actorSelection("akka.tcp://JobTrackerSystem@127.0.0.1:7890/user/JobTrackerActor")
		var counter = 0

		def receive = {
			case Start =>

				// new Thread(new Runnable(){
				// 	def run() {
				// 		system.scheduler.schedule(Duration.create(1000, TimeUnit.MILLISECONDS), Duration.create(1000, TimeUnit.MILLISECONDS),
				// 			remote, Message("foo"));
				// 	}
				// }).start;

				remote ! new JobConf()
				self ! "stop"
			case "stop" =>
				println("stopping...")
				context.stop(self)

			case Message(msg) => 
				println(s"JobSubmitterActor received message: '$msg'")
				if (counter < 5) {
					sender ! End
					counter += 1
				}

			case jc: JobConf => 
				println("JobSubmitterActor received message: JobConf")
				sender ! Message("succeed received")

			case _ =>
				println("JobSubmitterActor got something unexpected.")
		}

	def printActor(s: ActorRef){
		println(s.path.toString)
		println(s.path.address.toString)
		println(s.path.address.host)
		println(s.path.address.port)
	}
}
