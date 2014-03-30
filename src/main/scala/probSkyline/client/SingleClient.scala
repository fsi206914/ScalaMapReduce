package probSkyline.client

import probSkyline.query._
import scalaMapReduce.Config;
import scala.collection.mutable.ListBuffer
import java.io.File
import probSkyline.query._


/**
 * SingleClient reads data from the folder part as the source,
 * and do pruning based on rectangle pruning, and write data to
 * .result file/
 */
object SingleClient extends App{

	val CC = Config.ClusterConfig;

	if(args.length != 1){
		println("usage: scala SingleClient single(all)")
		System.exit(1);
	}
	val argStr = args(0)

	/**
	 * some variables initialization of preprocess
	 */
	if(argStr == "single"){
		val nqClient = new NaiveQuery(CC.getString("testArea"))
		nqClient.compProb(nqClient.getItemList)
	}
	else{
		val splitNum = CC.getInt("splitNum");
		val nqClient = new NaiveQuery(CC.getString("testArea"))
		for(i<- 0 until splitNum){
			nqClient.changeTestArea(i.toString)
			nqClient.compProb(nqClient.getItemList)
		}
	}


	/**
	 * get the files uner a folder recursively.
	 * Since part folder also has other files, the function filters files which are ".txt" files
	 */
	def listAllFiles(fList: ListBuffer[File], path:File){
		if(path.isDirectory()){
			for{f<-path.listFiles() if f.getName().substring(f.getName().length()-3) == "txt" }{
				if(f.isFile) fList += f.getAbsoluteFile() 
				else listAllFiles(fList, f)
			}
		}
	}
}
