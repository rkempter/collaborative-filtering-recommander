package ch.epfl.advanceddatabase.rkempter.initialization;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class InitializationMapper extends MapReduceBase implements Mapper<IntWritable, TupleValueWritable, IntWritable, TupleValueWritable>
{
	public void map(IntWritable key, TupleValueWritable value, OutputCollector<IntWritable, TupleValueWritable> output, Reporter reporter) throws IOException {
		
		// Identity function
		output.collect(key, value);
	}
}