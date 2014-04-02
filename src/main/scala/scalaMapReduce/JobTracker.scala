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

}

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

  }
	
  def requestTaskId() = {
    JobTracker.currentMaxTaskId += 1
    JobTracker.currentMaxTaskId-1
  }
	
  def distributeTasks(){

  }

}

object JobTracker{

  val JOB_MAPPER_OUTPUT_PREFIX = "mapper_output_job_";
  val TASK_MAPPER_OUTPUT_PREFIX = "mapper_output_task_";
  val JOB_CLASSPATH_PREFIX = "job";
  var currentMaxTaskId = (math.random*1000).toInt;

  def getTASK_MAPPER_OUTPUT_PREFIX() = TASK_MAPPER_OUTPUT_PREFIX;
}