package probSkyline

import scala.collection.mutable.ListBuffer
import java.io.File
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.io.Source
import scala.math;

import scalaMapReduce.Config;
import probSkyline.dataStructure._

object Util{

	val CC = Config.ClusterConfig;
	val srcName = CC.getString("srcName");
	val dim = CC.getInt("dim")

	/*
	 * get the whole map data[Int, item], filter objects whose item is still in 
	 * objectSet, which is returned by the first phase.
	 */
	// def getItemListFilter(objSet: HashSet[Int]) ={

	// 	var aMap = new HashMap[Integer, item]();
	// 	for(line <- Source.fromFile(fileName).getLines()){

	// 		val curr = stringToInstance(line);
	// 		if(objSet.contains(curr.objectID)){
	// 			if(!aMap.contains(curr.objectID)){
	// 				val aItem = new item(curr.objectID);              
	// 				aMap.update(curr.objectID, aItem);
	// 			}
	// 			aMap(curr.objectID).addInstance(curr);
	// 		}
	// 	}
	// 	val itemList = aMap.values.toList
	// 	itemList
	// }

	/*
	 * get the whole map data[Int, item], filter objects whose item is still in 
	 * objectSet, which is returned by the first phase.
	 */
	def getItemList(fileName: String) = {

		var aMap = new HashMap[Integer, Item]();
		for(line <- Source.fromFile(fileName).getLines()){

			val curr = stringToInstance(line);
			if(!aMap.contains(curr.objID)){
				val aItem = new Item(curr.objID);              
				aMap.update(curr.objID, aItem);
			}
			aMap(curr.objID).addInstance(curr);
		}
		val itemList = aMap.values.toList
		itemList
	}


	def stringToInstance(line: String) = {
		val div = line.split(" ");
		var inst: Instance = null
		if(div.length == dim + 3){
			inst = new Instance(div(0).toInt, div(1).toInt, div(div.length-1).toDouble, dim);
			inst.setPoint(getPoint(div))
		}
		else println("Sth wrong in StringToInstance in Util.")
		inst
	}


	def getPoint(div: Array[String]) = {
		var arrRet = new Array[Double](dim)
		for(i<-0 until dim) arrRet(i) = div(i+2).toDouble
		arrRet
	}

	/*
	 * Given an instance, getPartition finds the partition number for the 
	 * instance.
	 */
	def getPartition(aInst: Instance) = {
		if(aInst != null){

			/*
			 * compute the length of the radius firstly.
			 */
			var r = 0.0;
			for(i<- 0 until dim)
				r += aInst.pt(i) * aInst.pt(i);

			/*
			 * Detailed computation procedure could be referred in Angle-based Space Partitioning for Efficient Parallel
			 * Skyline Computation.
			 */
			var angle = new Array[Double](dim-1)
			for(i<-0 until angle.length){
				r -= aInst.pt(i) * aInst.pt(i);
				val tanPhi = math.sqrt(r)/aInst.pt(i);
				angle(i) = math.atan(tanPhi);	
			}

			val arrDouble = Config.arrDouble;

			/* 
			 * Current partitioning scheme only supports two and three dimensional cases.
			 * if the returned value is -1, it denotes that sth wrong happened in the get partition number Process.
			 */

			if(dim == 2){

				val angles = arrDouble(0);
				for{i<-0 until angles.length; if angles(i) > angle(0) } i;
				-1;
			}
			else if(dim == 3){

				val anglesX = arrDouble(0);
				var partitionX = -1;
				for{i<-0 until anglesX.length; if anglesX(i) > angle(0)}
						partitionX = i;

				val anglesY = arrDouble(1);
				var partitionY = -1;
				for{i<-0 until anglesY.length ; if anglesX(i) > angle(1) }{
						partitionY = i;
				}
				if(partitionX == -1 || partitionY == -1)
					-1;
				else{
					val ret = partitionX * anglesX.length + partitionY;
					ret;
				}
			}
		}
		-1;
	}


}
