package probSkyline.genData

import scalaMapReduce.Config;
import probSkyline.Util;
import probSkyline.dataStructure._;

import java.io.IOException;
import scala.collection.immutable.HashSet;

object SplitData extends App{
	
	val CC = Config.ClusterConfig;
	val fileName = CC.getString("srcName");

	/*
	 * read original data file's data to memory.
	 * from file to itemList, which represents the list of items.
	 */
	var itemList = Util.getItemList(fileName)

	// /*
	//  * write instance to each file based on angle partitioning scheme.
	//  * first, we create files, each of which corresponds a partition.
	//  */
	// val TIWs = new Array[TextInstanceWriter](CC.splitNum)
	// for(i<-0 until CC.splitNum)
	// 	TIWs(i) = new TextInstanceWriter(i.toString, "part")

	// //Based on the partition return result, we put instance to that file.:w
	// try{
	// 	for(i<-0 until itemList.size()){
	// 		val aItem = itemList.get(i);
	// 		for(j<-0 until aItem.instances.size()){
	// 			val aInst = aItem.instances.get(j);
	// 			val ret_area = Util.getPartition(aInst, CC);
	// 			TIWs(ret_area).write(aInst);
	// 		}
	// 	}

	// 	// after writing finishes, it should close the file.
	// 	for(i<-0 until CC.splitNum)
	// 		TIWs(i).close()
	// }catch {
	// 	case ioe: IOException => ioe.printStackTrace()	
	// }


	// /*
	//  * find left bottom and right top extreme value of every item, and add to outputList.
	//  * first, we create files, each of which corresponds a partition.
	//  */

	// var outputLists = new ArrayList[PartitionInfo]();
	// for(i<-0 until CC.splitNum)
	// 	outputLists.add(new PartitionInfo(i, CC.dim))

	// for(i<-0 until itemList.size())
	// 	findExtreme(itemList.get(i), outputLists)
	
	// import java.io.FileOutputStream;
	// import java.io.ObjectOutputStream;
	// try{

	// 	val fileOut = new FileOutputStream( "MAX_MIN" );
	// 	var outStream = new ObjectOutputStream(fileOut);
	// 	outStream.writeObject(outputLists);
	// 	outStream.flush();
	// 	outStream.close();
	// 	fileOut.close();
	// }catch{case e: IOException =>  e.printStackTrace(); }



	// /*
	//  * the function to compute the min and max extreme value of
	//  * every object.
	//  */
	// def findExtreme(aItem: item, outputList: ArrayList[PartitionInfo]){

	// 	val areaSet = new HashSet[Int]
	// 		var min= new instance.point(SplitData.CC.dim)
	// 		min.setOneValue(1.0)
	// 		var max= new instance.point(SplitData.CC.dim)
	// 		max.setOneValue(0.0)

	// 		for(i<-0 until aItem.instances.size()){
	// 			val aInstance = aItem.instances.get(i);	
	// 			for(j<-0 until SplitData.CC.dim){

	// 				if(aInstance.a_point.__coordinates(j) < min.__coordinates(j))
	// 					min.__coordinates(j) = aInstance.a_point.__coordinates(j);

	// 				if(aInstance.a_point.__coordinates(j) > max.__coordinates(j))
	// 					max.__coordinates(j) = aInstance.a_point.__coordinates(j);
	// 			}
	// 		}

	// 	for(i<-0 until aItem.instances.size()){
	// 		val aInst = aItem.instances.get(i);		
	// 		val area = Util.getPartition(aInst, SplitData.CC);
	// 		if(!areaSet.contains(area)){

	// 			val aPartInfo = outputList.get(area);
	// 			aPartInfo.addMax(aItem.objectID, max);
	// 			aPartInfo.addMin(aItem.objectID, min);
	// 		}
	// 	}
	// }

}
