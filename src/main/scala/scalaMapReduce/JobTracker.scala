package scalaMapReduce

import akka.actor._
import common._
import javaMapReduce._;
import java.util._;
import java.io._;

import scala.collection.mutable.StringBuilder;



class JobTracker{

  def submitJob(newjob: JobMeta){

    newjob.splitInput();
    val blocks: List[JobMeta.InputBlock] = newjob.getInputBlocks();
    if(blocks == null || blocks.size() == 0)
      println("Sth wrong in allocating splitInput");

    val jobMapperOutputDirPath = new StringBuilder().append("./tmp").append(File.separator)
                   .append(JobTracker.JOB_MAPPER_OUTPUT_PREFIX).append(newjob.getJobId())

    (new File(jobMapperOutputDirPath.toString)).mkdir();

    var mapTasks = new HashMap[Int, TaskMeta]();
    var reduceTasks = new HashMap[Int, TaskMeta]();

    for (i<-0 until blocks.size()) {
      val block = blocks.get(i);
      val taskid = this.requestTaskId();
      val minfo = new MapperTaskInfo(newjob.getJobId(), taskid, block.getFilePath(),
          block.getOffset(), block.getLength(), newjob.getMapperClassName(),
          newjob.getPartitionerClassName(), newjob.getInputFormatClassName(),
          jobMapperOutputDirPath.toString(), newjob.getReducerNum());
      val mtask = new TaskMeta(taskid, newjob.getJobId(), minfo, new TaskProgress(taskid,
            TaskMeta.TaskType.MAPPER));

      mapTasks.put(taskid, mtask);
      newjob.addMapperTask(taskid);
    }

    for(i<- 0 until newjob.getReducerNum()){
      val taskid = this.requestTaskId();
      val rinfo = new ReducerTaskInfo(newjob.getJobId(), taskid, i, jobMapperOutputDirPath.toString(),
              newjob.getReducerClassName(), newjob.getOutputFormatClassName(),
              newjob.getOutputPath());
      val rtask = new TaskMeta(taskid, newjob.getJobId(), rinfo, new TaskProgress(taskid,
              TaskMeta.TaskType.REDUCER));

      reduceTasks.put(taskid, rtask);
      newjob.addReducerTask(taskid);
    }

    // submit these tasks into the system
    JobTracker.mapTasks.putAll(mapTasks.asInstanceOf[ Map[Integer, TaskMeta] ]  );
    JobTracker.reduceTasks.putAll(reduceTasks.asInstanceOf[ Map[Integer, TaskMeta] ]);
    JobTracker.mapTasksQueue.addAll(mapTasks.values());
    JobTracker.reduceTasksQueue.addAll(reduceTasks.values());
    JobTracker.jobs.put(newjob.getJobId(), newjob);
    newjob.setStatus(JobMeta.JobStatus.INPROGRESS)
  }
	
  def requestTaskId() = {
    JobTracker.currentMaxTaskId += 1
    JobTracker.currentMaxTaskId-1
  }
	
  def distributeTasks(){

  }

  /**
   * Register a new tasktracker in this jobtracker
   */
  def registerTaskTracker(tt: TaskTrackerMeta) = {
    if (tt == null)false;

    if (JobTracker.tasktrackers.containsKey(tt.getTaskTrackerName())) {
      println("The TaskTracker \"" + tt.getTaskTrackerName() + "\" already exist.");
      false;
    } else {
      System.err.println("Register a new tasktracker : " + tt.getTaskTrackerName());
      JobTracker.tasktrackers.put(tt.getTaskTrackerName(), tt);
      true;
    }
  }


  def getTaskTracker(id: String) = {
    if(JobTracker.tasktrackers.containsKey(id))
      JobTracker.tasktrackers.get(id);
    else null
  }


  def deleteTaskTracker(ttname: String){
    if(ttname != null && JobTracker.tasktrackers.containsKey(ttname)) 
      JobTracker.tasktrackers.remove(ttname);
  }

}

object JobTracker{

  val JOB_MAPPER_OUTPUT_PREFIX = "mapper_output_job_";
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