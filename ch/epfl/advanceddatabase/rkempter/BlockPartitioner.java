package ch.epfl.advanceddatabase.rkempter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

import ch.epfl.advanceddatabase.rkempter.initialization.TupleValueWritable;

/*
 * Elements with lying in the same block are sent to the same
 * reducer.
 */
public class BlockPartitioner implements Partitioner<IntPair, TupleValueWritable> {

	public void configure(JobConf job) {}

	public int getPartition(IntPair key, TupleValueWritable value, int numPartitions) {
		return Math.abs(key.getBlock() * 127) % numPartitions; 
	}
}
