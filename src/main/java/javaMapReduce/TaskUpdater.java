package javaMapReduce;

import akka.actor.*;
import java.util.concurrent.TimeUnit;
import javaMapReduce.TaskProgress;
import akka.japi.Creator;

public class TaskUpdater{

	private TaskProgress tp;
	private String name;
	public TaskUpdater(TaskProgress tp, String name){
		this.name = name;
		this.tp = tp;
	}

	public void run(){
    ActorSystem system = ActorSystem.create("worker"+name);
    final ActorRef listener = system.actorOf(TaskUpdateActor.props("1"), "TaskUpdateActor");
    listener.tell("sdf", ActorRef.noSender());
	}

	public static void main(String [] args){

		TaskUpdater tu = new TaskUpdater(null, "1");
		tu.run();
	}

}


class TaskUpdateActor extends UntypedActor {

	private String ttName;
	private String remoteAddr;
	private ActorSelection selection;

  public static Props props(final String name) {
    return Props.create(new Creator<TaskUpdateActor>() {
      private static final long serialVersionUID = 1L;
 
      @Override
      public TaskUpdateActor create() throws Exception {
        return new TaskUpdateActor(name);
      }
    });
  }

  public void setActorSelection(){
  	selection = getContext().system().actorSelection(remoteAddr);
  }

  public TaskUpdateActor(String ttName){
  	this.ttName = ttName;
  	this.remoteAddr = "akka.tcp://TaskTracker" + ttName + "System@127.0.0.1:10041/user/TaskTrackerActor";
  	setActorSelection();
  }

  public void onReceive(Object message) {
    if (message instanceof TaskProgress) {
      TaskProgress work = (TaskProgress) message;

    } else if (message instanceof String) {
      String work = (String) message;
      System.out.println("work = " + work);
    	selection.tell(work, getSelf());
    }
  }
}
