package javaMapReduce;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;
/**
 * This interface is used to submit task to task tracker 
 *
 */
public interface TaskLauncher extends Remote {
  boolean runTask(TaskInfo taskinfo) throws RemoteException;
	boolean transferFolder(int jid, Map<Integer, TaskMeta> mapTasks, Map<String, TaskTrackerMeta> tasktrackers) throws RemoteException;
}
