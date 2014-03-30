package javaMapReduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This class holds the configurations for the cluster.
 * 
 * Singleton pattern is utilized for this class to guarantee only one instance.
 * @author liang 
 */
public class ClusterConfig implements Serializable{

	public int dim;
	public int maxObjectNum;
	public String testArea;
	public double threshold;
	public String srcName;
	public String intermediate;
	public String partFolder;
	/*insert an comment
	 */
	public int numWorkers;
	public String configFile;
	public double[] splitValue;
	public int splitNum;
	public int numDiv;
	public ArrayList< ArrayList<Double>> arrDouble;
	transient static final int MaxSplitNum = 10;
	
	private static  ClusterConfig CC = new ClusterConfig();

	public ClusterConfig(String configFile) {
		this.configFile = configFile;
		parseConfigFile();
	}

	private ClusterConfig() {
		
		parseConfigFile();
	}

	public static ClusterConfig getInstance(){
		CC = new ClusterConfig();
		return CC;
	}


	public void updateTestArea(){

		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream("liang.prop"));;
		}catch (IOException ex) {
			ex.printStackTrace();
		}
		this.testArea = prop.getProperty("testArea");
	}

	/**
	 * Parse the configuration file and populate all the 
	 * fields in configuration
	 */
	public void parseConfigFile() {

		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream("liang.prop"));;

			splitValue = new double[MaxSplitNum];
			FileReader fd = new FileReader("liang.split");

			BufferedReader br = new BufferedReader(fd);
			int split_index = 0;
			String line = br.readLine();
			while(line != null && split_index < MaxSplitNum){

				if(Double.parseDouble(line) >=0 ){
					splitValue[split_index] = Double.parseDouble(line);
					split_index ++;
				}

				line = br.readLine();
			}
			numWorkers = split_index;

		}catch (IOException ex) {
			ex.printStackTrace();
		}

		this.maxObjectNum = Integer.parseInt(prop.getProperty("objectNum"));
		this.dim = Integer.parseInt(prop.getProperty("dim"));
		this.testArea = prop.getProperty("testArea");
		this.partFolder = prop.getProperty("partFolder");

		this.splitNum = this.numDiv = Integer.parseInt(prop.getProperty("numDiv"));
		threshold = Double.parseDouble(prop.getProperty("threshold"));
		this.srcName= prop.getProperty("srcName");
		this.intermediate= prop.getProperty("intermediate");

		arrDouble = new ArrayList< ArrayList<Double> >();
		if(dim == 2){
			ArrayList<Double> arr = new ArrayList<Double>();
			for(int i=0; i<splitNum; i++){
				arr.add(Math.PI/2/splitNum*(i+1));
			}
			arrDouble.add(arr);
		}
		else if (dim == 3){
			ArrayList<Double> arr = new ArrayList<Double>();
			for(int i=0; i<3; i++){
				arr.add(Math.PI/3*(i+1)/2);	
			}
			arrDouble.add(arr);

			ArrayList<Double> arr2 = new ArrayList<Double>();
			for(int i=0; i<splitNum/3; i++){
				arr2.add(Math.PI/(splitNum/3)*(i+1)/2);	
			}
			arrDouble.add(arr2);
		}
		else
			System.out.println("current partitioning scheme only support two or three dimension.");
	}



   public static void replace(String target, int areaID) {
      String oldFileName = "liang.prop";
      String tmpFileName = "liang.prop.tmp";

      BufferedReader br = null;
      BufferedWriter bw = null;
      try {
         br = new BufferedReader(new FileReader(oldFileName));
         bw = new BufferedWriter(new FileWriter(tmpFileName));
         String line;
         while ((line = br.readLine()) != null) {
            if (line.contains(target))
               line = target + " = "+Integer.toString(areaID);
            bw.write(line+"\n");
         }
      } catch (Exception e) {
         return;
      } finally {
         try {
            if(br != null)
               br.close();
         } catch (IOException e) {
            //
         }
         try {
            if(bw != null)
               bw.close();
         } catch (IOException e) {
            //
         }
      }
      // Once everything is complete, delete old file..
      File oldFile = new File(oldFileName);
      oldFile.delete();

      // And rename tmp file's name to old file name
      File newFile = new File(tmpFileName);
      newFile.renameTo(oldFile);

   }


}

