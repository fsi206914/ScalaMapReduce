package javaMapReduce;

import java.io.*;
import java.util.Map;
import scalaMapReduce.JobTracker;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.Enumeration;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
/**
 * 
 * This is a utility class. methods included are: 
 * 1. String getParam(String key) 
 * 2. void startJavaProcess(String[] args, int jid) throws Exception 
 * 3. void startProcess(String[] args) throws Exception
 */
public class Utility {

	/* MAPREDUCE_HOME used to locate config file */
	//private static String MAPREDUCE_HOME = System.getenv().get("MAPREDUCE_HOME");
	private static String MAPREDUCE_HOME = ".";

	/**
	 * method used to read config information from config file
	 * 
	 * @param key
	 *          of config info
	 * @return value of config info
	 */
	public static String getParam(String key) {

		/* locate the path of config file */
		String configPath = MAPREDUCE_HOME + "/config/config";
		FileInputStream fis = null;
		BufferedReader br = null;
		try {
			fis = new FileInputStream(configPath);
			br = new BufferedReader(new InputStreamReader(fis));
			String line = "";
			while ((line = br.readLine()) != null) {
				int ind = line.indexOf('=');
				String k = line.substring(0, ind);
				if (k.equals(key)) {
					String s = line.substring(ind + 1);
					return line.substring(ind + 1);
				}
			}
			throw new RuntimeException("cannot find param " + key);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null && fis != null)
				try {
					br.close();
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
		return "";
	}

	/**
	 * method used to start of new java process
	 * 
	 * @param args
	 * @param jid
	 * @throws Exception
	 */
	public static void startJavaProcess(String[] args, int jid) throws Exception {
		/* build arguments */
		String separator = System.getProperty("file.separator");
		String classpath = System.getProperty("java.class.path");
		String path = System.getProperty("java.home") + separator + "bin" + separator + "java";
		// String path = "scala";
		String[] newargs = new String[args.length + 3]; /* three more args for path, -cp, classpath */
		newargs[0] = path;
		newargs[1] = "-cp";
		newargs[2] = "userpath" + separator + "job" + jid + separator;
		newargs[2] += ":./lib/scala-library-2.10.3.jar:./lib/typesafe-config.jar:./lib/akka-actors.jar:./lib/akka-remote.jar";

		for (int i = 3, j = 0; j < args.length; i++, j++) {
			newargs[i] = args[j];
		}

		for(int i=0; i<newargs.length; i++)
			System.out.print(newargs[i] + "  ");

		System.out.println(" ");


		/* start a process with above arguments */
		ProcessBuilder processBuilder = new ProcessBuilder(newargs);
		Process process = processBuilder.start();

		InputStream is = process.getInputStream();
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		String line;
		System.out.printf("Output of running %s is:\n", Arrays.toString(newargs));
		while ((line = br.readLine()) != null) {
			System.out.println(line);
		}

        //Wait to get exit value
		try {
			int exitValue = process.waitFor();
			System.out.println("\n\nExit Value is " + exitValue);
		} catch (InterruptedException e) {
            // TODO Auto-generated catch block
			e.printStackTrace();
		}

	}


	/**
	 * method used to start a general process
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void startProcess(String[] args) throws Exception {
		ProcessBuilder processBuilder = new ProcessBuilder(args);
		processBuilder.start();
	}


  public static boolean extractJobClassJar(int jobid, String jarpath) {
    try {
      JarFile jar = new JarFile(jarpath);
      Enumeration enums = jar.entries();

      // find the path to which the jar file should be extracted
      String destDirPath = JobTracker.getJOB_CLASSPATH() + File.separator
              + JobTracker.getJOB_CLASSPATH_PREFIX() + jobid + File.separator;
      File destDir = new File(destDirPath);
      if (!destDir.exists()) {
        destDir.mkdirs();
      }

      // copy each file in jar archive one by one
      while (enums.hasMoreElements()) {
        JarEntry file = (JarEntry) enums.nextElement();

        File outputfile = new File(destDirPath + file.getName());
        if (file.isDirectory()) {
          outputfile.mkdirs();
          continue;
        }
        InputStream is = jar.getInputStream(file);
        FileOutputStream fos = null;
        try {
          fos = new FileOutputStream(outputfile);
        } catch (FileNotFoundException e) {
          outputfile.getParentFile().mkdirs();
          fos = new FileOutputStream(outputfile);
        }
        while (is.available() > 0) {
          fos.write(is.read());
        }
        fos.close();
        is.close();
      }
    } catch (IOException e) {
      // TODO : handle this exception if the jar file cannot be found
      return false;
    }

    return true;
  }

	public static String getSystemTempDir() {
		String res = "tmp";
		if (res.compareTo("") == 0)
			res = System.getProperty("java.io.tmpdir");

		File tmpdir = new File(res);
		if (!tmpdir.exists()) {
			tmpdir.mkdirs();
		}

		return res;
	}


}
