package scalaMapReduce

import akka.actor._
import common._
import javaMapReduce._;
import java.util._;
import java.io._;

import scala.collection.mutable.StringBuilder;


class TaskTracker(val taskTrackerName: String){
  var mapperCounter: Integer = 0; 
  var reducerCounter: Integer = 0; 

  var rPort = 10041

  def getTaskTrackerName()  = taskTrackerName;
  def getRPort() = rPort;

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

  def getTASK_MAPPER_OUTPUT_PREFIX() = TASK_MAPPER_OUTPUT_PREFIX;

  val mapTasks: Map[Integer, TaskMeta] = Collections.synchronizedMap(new HashMap[Integer, TaskMeta]());
  val reduceTasks: Map[Integer, TaskMeta] = Collections.synchronizedMap(new HashMap[Integer, TaskMeta]());
  val jobs: Map[Integer, JobMeta] = Collections.synchronizedMap(new HashMap[Integer, JobMeta]());


  val mapTasksQueue = new PriorityQueue[TaskMeta](10,

            new Comparator[TaskMeta]() {
              def compare(o1: TaskMeta, o2: TaskMeta) = o1.getJobID() - o2.getJobID();
      });

  val reduceTasksQueue = new PriorityQueue[TaskMeta](10,

            new Comparator[TaskMeta]() {
              def compare(o1: TaskMeta, o2: TaskMeta) = o1.getJobID() - o2.getJobID();
      });

  val tasktrackers = Collections.synchronizedMap(new HashMap[String, TaskTrackerMeta]());

}