package scalaMapReduce

import akka.actor._
import common._
import javaMapReduce._;
import scala.collection.mutable._;
import java.io._;

import scala.collection.mutable.StringBuilder;


class TaskTracker(val taskTrackerName: String, val rPort: Int){
  var mapperCounter: Integer = 0; 
  var reducerCounter: Integer = 0; 

  val taskStatus = new HashMap[Int, TaskProgress]() with SynchronizedMap[Int, TaskProgress]


  def getTaskTrackerName()  = taskTrackerName;
  def getRPort() = rPort;

  def runTask(taskInfo: TaskInfo){

    if(taskInfo.getType() == TaskMeta.TaskType.MAPPER){
      val mapperTaskInfo = taskInfo.asInstanceOf[MapperTaskInfo];
      mapperCounter.synchronized{
      if(mapperCounter < TaskTracker.NUM_OF_MAPPER_SLOTS){

        println("task tracker " + getTaskTrackerName()
          + " received runTask request taskid:" + taskInfo.getTaskID() + " "
          + taskInfo.getType());

        mapperCounter += 1;

        val args: Array[String] = Array( classOf[MapperWorker].getName(),
              mapperTaskInfo.getTaskID().toString(), mapperTaskInfo.getInputPath(),
              mapperTaskInfo.getOffset().toString,
              mapperTaskInfo.getBlockSize().toString(), mapperTaskInfo.getOutputPath(),
              mapperTaskInfo.getMapper(), mapperTaskInfo.getPartitioner(),
              mapperTaskInfo.getInputFormat(), mapperTaskInfo.getReducerNum().toString(),
              getTaskTrackerName(), getRPort().toString );

          try {
            Utility.startJavaProcess(args, taskInfo.getJobID());
          } catch {
            case e: Exception => e.printStackTrace();
          }

      }}
    } else{
      val reducerTaskInfo = taskInfo.asInstanceOf[ReducerTaskInfo];
      reducerCounter.synchronized{
      if(reducerCounter < TaskTracker.NUM_OF_REDUCER_SLOTS){

        println("task tracker " + getTaskTrackerName()
          + " received runTask request taskid:" + taskInfo.getTaskID() + " "
          + taskInfo.getType());

        reducerCounter += 1;

        val args = Array( classOf[ReducerWorker].getName(),
              reducerTaskInfo.getTaskID().toString(), reducerTaskInfo.getOrderId().toString(),
              reducerTaskInfo.getReducer(),
              reducerTaskInfo.getOutputFormat(), reducerTaskInfo.getInputPath(),
              reducerTaskInfo.getOutputPath(), getTaskTrackerName(),
              getRPort().toString() );

          try {
            Utility.startJavaProcess(args, taskInfo.getJobID());
          } catch {
            case e: Exception => e.printStackTrace();
          }

      }}
    }

  }


}

/*
 * Companion object to store static variables, since JobTracker is 
 * only created once.
 */
object TaskTracker{

  val NUM_OF_MAPPER_SLOTS = 4;
  val NUM_OF_REDUCER_SLOTS = 4;
  val TASK_MAPPER_OUTPUT_PREFIX = "mapper_output_task_";
  val JOB_CLASSPATH_PREFIX = "job";
  var currentMaxTaskId = (math.random*1000).toInt;
}

class TaskStatusChecker(val tt: TaskTracker) extends Runnable {

  val ALIVE_CYCLE = 8000; // milliseconds

  /**
   * periodically check time stamp and set status
   */
  def run(){
    tt.taskStatus.synchronized{
      for( (id, taskProgress) <- tt.taskStatus){
        val curTime = System.currentTimeMillis();
        if ((curTime - taskProgress.getTimestamp() > ALIVE_CYCLE)
            && taskProgress.getStatus() != TaskMeta.TaskStatus.SUCCEED)
                taskProgress.setStatus(TaskMeta.TaskStatus.FAILED);
      }
    }
  }
}