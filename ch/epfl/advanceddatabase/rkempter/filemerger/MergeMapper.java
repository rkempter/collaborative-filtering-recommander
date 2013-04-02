package ch.epfl.advanceddatabase.rkempter.filemerger;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import ch.epfl.advanceddatabase.rkempter.UVDecomposer;
import ch.epfl.advanceddatabase.rkempter.rmseminimizer.MatrixInputValueWritable;

public class MergeMapper extends MapReduceBase implements Mapper<IntWritable, MatrixInputValueWritable, IntWritable, MatrixInputValueWritable>
{
	private int xPos;
	
	public void configure(JobConf conf) {
		xPos = conf.getInt(UVDecomposer.MATRIX_X_POSITION, 1);
	}
	
	public void map(IntWritable key, MatrixInputValueWritable value, OutputCollector<IntWritable, MatrixInputValueWritable> output, Reporter reporter) throws IOException {
		int position = value.getColumn();
		char type = value.getType();
		
		// Check for matrix type! If coming from new set, then accept!
		if((position == xPos && type == 'X') || (position != xPos)) { 
			output.collect(key, value);
		}
	}
}
