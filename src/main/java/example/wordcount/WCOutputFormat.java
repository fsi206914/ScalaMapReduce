package example.wordcount;

import javaMapReduce.OutputFormat;

public class WCOutputFormat  extends OutputFormat {

  @Override
  public String format(String key, String value) {
    return key + "\t" + value + "\n";
  }

}