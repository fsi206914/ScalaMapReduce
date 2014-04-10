package scalaMapReduce;

import scala.collection.mutable.ListBuffer
import akka.actor._
import com.typesafe.config._
import scala.math;


object Config {
  private val root = ConfigFactory.load()
  val JobTrackerConfig = root.getConfig("JobTrackerSystem")
  val JobSubmitterConfig = root.getConfig("JobSubmitterSystem")
  val TaskTracker1Config = root.getConfig("TaskTracker1System")


  val ClusterConfig = root.getConfig("ClusterConfig")
  val arrDouble = computeArrDouble()

  def computeArrDouble() = {
  	val dim = ClusterConfig.getInt("dim");
  	val splitNum = ClusterConfig.getInt("splitNum");

		val retList = new ListBuffer[ListBuffer[Double]](); 
		if(dim == 2){
			val arr = new ListBuffer[Double](); 
			for(i<-0 until splitNum) 
				arr.append(math.Pi/2/splitNum*(i+1));
			
			retList.append(arr);
		}
		else if (dim == 3){
			val arr = new ListBuffer[Double]();
			for(i<-0 until 3) 
				arr.append(math.Pi/3*(i+1)/2);	

			retList.append(arr);

			val arr2 = new ListBuffer[Double]();
			for(i<-0 until splitNum/3 )
				arr2.append(math.Pi/(splitNum/3)*(i+1)/2);	

			retList.append(arr2);
		}
		retList
	}
	
}
