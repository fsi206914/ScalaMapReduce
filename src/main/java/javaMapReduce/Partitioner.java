package javaMapReduce;
public interface Partitioner {

  public int getPartition(String key);

  public int getReducerNum();
}
