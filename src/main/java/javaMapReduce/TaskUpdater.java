package javaMapReduce;

import akka.actor.*;
import scala.concurrent.duration.Duration;
import java.util.concurrent.TimeUnit;
import javaMapReduce.TaskProgress;
import akka.japi.Creator;
import com.typesafe.config.ConfigFactory;
import akka.japi.Procedure;

class HelloMessage{

    private ActorRef receiver;
    private String text;
    HelloMessage() {}
    HelloMessage(ActorRef receiver){ this.receiver = receiver;}
    public ActorRef getReceiver(){ return receiver;}
    public void setText(String text) { this.text = text;}
    public String getText() {return text;}
}


public class TaskUpdater{

	private TaskProgress tp;
	private String name;
	public TaskUpdater(TaskProgress tp, String name){
		this.name = name;
		this.tp = tp;
	}

	public void run(){
    ActorSystem system = ActorSystem.create("worker"+name, ConfigFactory.load("taskupdate"));
    ActorRef remoteActor = system
      .actorFor("akka.tcp://TaskTracker1System@127.0.0.1:10041/user/TaskTrackerActor");

    final ActorRef listener = system.actorOf(TaskUpdateActor.props("1", remoteActor), "TaskUpdateActor");

    listener.tell(tp, null);
	}

	public static void main(String [] args){

    TaskProgress tp = new TaskProgress(5235, TaskMeta.TaskType.MAPPER);
		TaskUpdater tu = new TaskUpdater(tp, "1");
		tu.run();
	}

}


class TaskUpdateActor extends UntypedActor {

	private String ttName;
	private String remoteAddr;
  private ActorRef remote = null;

  public static Props props(final String name) {
    return Props.create(new Creator<TaskUpdateActor>() {
      private static final long serialVersionUID = 1L;
 
      @Override
      public TaskUpdateActor create() throws Exception {
        return new TaskUpdateActor(name);
      }
    });
  }

  public static Props props(final String name, final ActorRef fremote) {
    return Props.create(new Creator<TaskUpdateActor>() {
      private static final long serialVersionUID = 1L;
 
      @Override
      public TaskUpdateActor create() throws Exception {
        return new TaskUpdateActor(name, fremote);
      }
    });
  }

  private void sendIdentifyRequest() {
    getContext().actorSelection(remoteAddr).tell(new Identify(remoteAddr), getSelf());
    // getContext()
    //     .system()
    //     .scheduler()
    //     .scheduleOnce(Duration.create(3, TimeUnit.SECONDS), getSelf(),
    //         ReceiveTimeout.getInstance(), getContext().dispatcher(), getSelf());
  }

  public TaskUpdateActor(String ttName){
  	this.ttName = ttName;
  	this.remoteAddr = "akka.tcp://TaskTracker" + ttName + "System@127.0.0.1:10041/user/TaskTrackerActor";
  	sendIdentifyRequest();
  }

  public TaskUpdateActor(String ttName, ActorRef remoteRef){
    this.ttName = ttName;
    this.remote = remoteRef;
  }

  public void onReceive(Object message) throws Exception {
    if (message instanceof ActorIdentity) {
      System.out.println("TaskUpdateActor received Info from self.");
      remote = ((ActorIdentity) message).getRef();

      if (remote == null) {
        System.out.println("Remote actor not available: " + remoteAddr);
      } else {
        System.out.println("remote.toString =  " + remote.toString());

        getContext().watch(remote);
        getContext().become(active, true);
      }

    } else if (message instanceof HelloMessage) {
        HelloMessage msg = (HelloMessage) message;
        if (msg.getReceiver() !=null){
           msg.setText("Hello");
           msg.getReceiver().tell(msg, getSelf());
        }

    } else if (message instanceof ReceiveTimeout) {
//      sendIdentifyRequest();
    } else if (message instanceof String) {
        System.out.println("Sending string messages");
        String sent = (String) message;
        System.out.println("sent = "+ sent);
        remote.tell(sent, getSelf());

    } else if (message instanceof TaskProgress) {
        System.out.println("Sending TaskProgress");
        TaskProgress sent = (TaskProgress) message;
        remote.tell(sent, getSelf());

    } else {
      System.out.println("Not ready yet");
    }
  }

  Procedure<Object> active = new Procedure<Object>() {
    @Override
    public void apply(Object message) {
      if (message instanceof String) {
        // send message to server actor
        System.out.println("Sending string messages");
        remote.tell(message, getSelf());

      } else {
        unhandled(message);
      }
    }
  };


}
