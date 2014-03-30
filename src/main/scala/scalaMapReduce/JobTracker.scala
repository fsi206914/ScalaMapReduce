package scalaMapReduce

import akka.actor._
import common._
import javaMapReduce.JobConf;
import rapture.io._
import strategy.throwExceptions

object JobTrackerApp extends App  {
//  Http / "rapture.io" / "welcome.txt" > File / "home" / "liang" / "welcome.txt"
//  File / "home" / "liang" / "welcome.txt" > Socket("localhost", 6789)


  val system = ActorSystem("JobTrackerSystem", Config.JobTrackerConfig)
  val jtActor = system.actorOf(Props[JobTrackerActor], name = "JobTrackerActor")
//  jtActor ! Message("The JobTracker is alive")
}


class JobTrackerActor extends Actor {
  def receive = {
    case "RequestJobID" =>
        println("JobTracker received message RequestJobID")
        val s = sender
        printActor(s)
        sender ! 123
    case jc: JobConf => 
      println("jobTrackerActor received an object JobConf")
      submitJob(jc)
    case _ => 
        println("JobTracker got something unexpected.")
  }

  def printActor(s: ActorRef){
    println(s.path.toString)
    println(s.path.address.toString)
    println(s.path.address.host)
    println(s.path.address.port)
  }

  def submitJob(jc: JobConf){
    
  }

}

class JobTracker{
	
	
	
}
