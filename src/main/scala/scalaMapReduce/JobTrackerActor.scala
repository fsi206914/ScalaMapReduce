package scalaMapReduce

import akka.actor._
import common._
import javaMapReduce._;
import java.util._;
import java.io._;

import scala.collection.mutable.StringBuilder

object JobTrackerApp extends App  {
//  Http / "rapture.io" / "welcome.txt" > File / "home" / "liang" / "welcome.txt"
//  File / "home" / "liang" / "welcome.txt" > Socket("localhost", 6789)

  val system = ActorSystem("JobTrackerSystem", Config.JobTrackerConfig)
  val jtActor = system.actorOf(Props(new JobTrackerActor(new JobTracker())) , name = "JobTrackerActor")
}


class JobTrackerActor(val jt: JobTracker) extends Actor {
  def receive = {
    case "RequestJobID" =>
      println("JobTracker received message RequestJobID")
      val s = sender
      printActor(s)
      sender ! 123
    case jconf: JobConf => 
      println("jobTrackerActor received an object JobConf")
      submitJob(jconf)

    case ttup:TaskTrackerUpdatePkg =>
      update(ttup);
    case _ => 
        println("JobTracker got something unexpected.")
  }

  def printActor(s: ActorRef){
    println(s.path.toString)
    println(s.path.address.toString)
    println(s.path.address.host)
    println(s.path.address.port)
  }

  def submitJob(jconf: JobConf){
    val newJob = new JobMeta(jconf);
    jt.submitJob(newJob);
    jt.distributeTasks();
  }

  def update(ttup:TaskTrackerUpdatePkg ){

    val ttName = ttup.getTaskTrackerName();
    var ttmeta = jt.getTaskTracker(ttName);
    if(ttmeta == null){
      ttmeta = new TaskTrackerMeta(ttup.getTaskTrackerName(), null);
      if(jt.registerTaskTracker(ttmeta))
        println( ttup.getTaskTrackerName() + " register successfully.");
      else
        println( ttup.getTaskTrackerName() + " register failed")
    }
    
    ttmeta.setNumOfMapperSlots(ttup.getNumOfMapperSlots());
    ttmeta.setNumOfReducerSlots(ttup.getNumOfReducerSlots());
    ttmeta.setTimestamp(System.currentTimeMillis());
    jt.updateTaskStatus(ttup)
  }

}
