package scalaMapReduce

import akka.actor._
import common._
import javaMapReduce._;
import java.util._;
import java.io._;
import scala.collection.mutable._;

import scala.collection.mutable.StringBuilder

object TaskTrackerApp extends App  {
//  Http / "rapture.io" / "welcome.txt" > File / "home" / "liang" / "welcome.txt"
//  File / "home" / "liang" / "welcome.txt" > Socket("localhost", 6789)
  override def main(args: Array[String]){
    val ttName = args(0);
    val rPort = args(1).toInt;
    val system = ActorSystem("TaskTracker" + ttName + "System", Config.TaskTracker1Config)
    val ttActor = system.actorOf(Props(new TaskTrackerActor(new TaskTracker("tt" + ttName, rPort))) , name = "TaskTracker"+ ttName)
    ttActor ! Start
  }
}


class TaskTrackerActor(val tt: TaskTracker) extends Actor {

  import context._
  val remote = context.actorSelection("akka.tcp://JobTrackerSystem@127.0.0.1:10021/user/JobTrackerActor")
  def receive = {

    case Start =>
      println("TaskTrackerActor begin working")
 //     run();
    case "RequestJobID" =>
      println("JobTracker received message RequestJobID")
      sender ! 123
    case taskInfo: TaskInfo => 
      println("TaskTrackerActor received an object taskInfo");
      tt.runTask(taskInfo);
    case _ => 
      println("JobTracker got something unexpected.")
  }


  /* start the task status checker */
  def run(){
    import java.util.concurrent.ScheduledExecutorService;
    import java.util.concurrent.Executors;
    import java.util.concurrent.ScheduledFuture;
    import java.util.concurrent.TimeUnit;

    val taskStatusChecker = new Thread( new TaskStatusChecker(tt));


    taskStatusChecker.setDaemon(true);
    val schExec = Executors.newScheduledThreadPool(8);
    val schFutureChecker = schExec.scheduleAtFixedRate(taskStatusChecker, 0,
            2, TimeUnit.SECONDS);


    val taskStatusUpdater = new Thread( new Runnable(){

      def run(){
        val toDelete = new ListBuffer[Int]();
        val taskStatus = tt.taskStatus;
        taskStatus.synchronized{
          
          /* 
           * In order to reuse TaskTrackerUpdatePkg source class,
           * We have to use ArrayList to represent taskList.
           */
          val taskList = new java.util.ArrayList[TaskProgress]()
          for(tp <- taskStatus.values) taskList.add(tp);


          for( (id, taskProgress) <- taskStatus)
            if (taskProgress.getStatus() != TaskMeta.TaskStatus.INPROGRESS){
                toDelete += id;

                if(taskProgress.getType() == TaskMeta.TaskType.MAPPER)
                  tt.mapperCounter.synchronized{ tt.mapperCounter -= 1;}
                else tt.reducerCounter.synchronized{ tt.reducerCounter -= 1;}
            }

          for (id<- toDelete) taskStatus.remove(id);

          tt.mapperCounter.synchronized{
            tt.reducerCounter.synchronized{
              val pkg = new TaskTrackerUpdatePkg(tt.taskTrackerName, TaskTracker.NUM_OF_MAPPER_SLOTS - tt.mapperCounter,
                        TaskTracker.NUM_OF_REDUCER_SLOTS - tt.reducerCounter, "12",
                        taskList, 123);
              remote ! pkg;
            }
          }
        }
      }

      });

  }

}
