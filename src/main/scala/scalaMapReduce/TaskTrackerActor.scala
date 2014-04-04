package scalaMapReduce

import akka.actor._
import common._
import javaMapReduce._;
import java.util._;
import java.io._;

import scala.collection.mutable.StringBuilder

object TaskTrackerApp extends App  {
//  Http / "rapture.io" / "welcome.txt" > File / "home" / "liang" / "welcome.txt"
//  File / "home" / "liang" / "welcome.txt" > Socket("localhost", 6789)

  val system = ActorSystem("TaskTracker1System", Config.JobTrackerConfig)
  val jtActor = system.actorOf(Props(new TaskTrackerActor(new TaskTracker("tt1"))) , name = "JobTrackerActor")
}


class TaskTrackerActor(val tt: TaskTracker) extends Actor {
  def receive = {
    case "RequestJobID" =>
      println("JobTracker received message RequestJobID")
      sender ! 123
    case taskInfo: TaskInfo => 
      println("TaskTrackerActor received an object taskInfo");
      runTask(taskInfo);
    case _ => 
      println("JobTracker got something unexpected.")
  }

  def runTask(taskInfo: TaskInfo){

    if(taskInfo.getType() == TaskMeta.TaskType.MAPPER){
      val mapperTaskInfo = taskInfo.asInstanceOf[MapperTaskInfo];
      tt.mapperCounter.synchronized{
      if(tt.mapperCounter < TaskTracker.NUM_OF_MAPPER_SLOTS){

        println("task tracker " + tt.getTaskTrackerName()
          + " received runTask request taskid:" + taskInfo.getTaskID() + " "
          + taskInfo.getType());

        tt.mapperCounter += 1;

        val args: Array[String] = Array( classOf[MapperWorker].getName(),
              mapperTaskInfo.getTaskID().toString(), mapperTaskInfo.getInputPath(),
              mapperTaskInfo.getOffset().toString,
              mapperTaskInfo.getBlockSize().toString(), mapperTaskInfo.getOutputPath(),
              mapperTaskInfo.getMapper(), mapperTaskInfo.getPartitioner(),
              mapperTaskInfo.getInputFormat(), mapperTaskInfo.getReducerNum().toString(),
              tt.getTaskTrackerName(), tt.getRPort().toString );

          try {
            Utility.startJavaProcess(args, taskInfo.getJobID());
          } catch {
            case e: Exception => e.printStackTrace();
          }

      }}
    } else{
      val reducerTaskInfo = taskInfo.asInstanceOf[ReducerTaskInfo];
      tt.reducerCounter.synchronized{
      if(tt.reducerCounter < TaskTracker.NUM_OF_REDUCER_SLOTS){

        println("task tracker " + tt.getTaskTrackerName()
          + " received runTask request taskid:" + taskInfo.getTaskID() + " "
          + taskInfo.getType());

        tt.reducerCounter += 1;

        val args = Array( classOf[ReducerWorker].getName(),
              reducerTaskInfo.getTaskID().toString(), reducerTaskInfo.getOrderId().toString(),
              reducerTaskInfo.getReducer(),
              reducerTaskInfo.getOutputFormat(), reducerTaskInfo.getInputPath(),
              reducerTaskInfo.getOutputPath(), tt.getTaskTrackerName(),
              tt.getRPort().toString() );

          try {
            Utility.startJavaProcess(args, taskInfo.getJobID());
          } catch {
            case e: Exception => e.printStackTrace();
          }

      }}
    }

  }

}
