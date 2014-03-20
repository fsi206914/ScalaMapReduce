package scalaMapReduce

import akka.actor._
import common._
import javaMapReduce.JobConf;
import rapture.io._
import strategy.throwExceptions

object JobTrackerApp extends App  {
  def printActor(s: ActorRef){
    println(s.path.toString)
    println(s.path.address.toString)
  }
//  Http / "rapture.io" / "welcome.txt" > File / "home" / "liang" / "welcome.txt"
  File / "home" / "liang" / "welcome.txt" > Socket("localhost", 6789)
  // val system = ActorSystem("JobTrackerSystem", Config.JobTrackerConfig)
  // val remoteActor = system.actorOf(Props[JobTrackerActor], name = "JobTrackerActor")
  // printActor(remoteActor)
  // remoteActor ! Message("The JobTracker is alive")
}


class JobTrackerActor extends Actor {
  def receive = {
    case Message(msg) =>
        println(s"JobTracker received message '$msg'")
        val s = sender
        printActor(s)
        sender ! Message("Hello from the JobTracker")
    case jc: JobConf => 
      println("LocalActor received message: JobConf")
    case _ => 
        println("JobTracker got something unexpected.")
  }

  def printActor(s: ActorRef){
    println(s.path.toString)
    println(s.path.address.toString)
    println(s.path.address.host)
    println(s.path.address.port)
  }
}
