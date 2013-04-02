package ch.epfl.advanceddatabase.rkempter.filemerger;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import ch.epfl.advanceddatabase.rkempter.rmseminimizer.MatrixUVValueWritable;

public class MergeReducer extends MapReduceBase implements Reducer<IntWritable, MatrixUVValueWritable, IntWritable, MatrixUVValueWritable>
{

	public void reduce(IntWritable key, Iterator<MatrixUVValueWritable> values,
			OutputCollector<IntWritable, MatrixUVValueWritable> output, Reporter reporter)
			throws IOException {
		
		while(values.hasNext()){
			MatrixUVValueWritable value = values.next();
			output.collect(key, value);
		}
	}

}
