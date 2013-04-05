package ch.epfl.advanceddatabase.rkempter.rmseminimizer;
import java.io.IOException;

import ch.epfl.advanceddatabase.rkempter.UVDecomposer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class MatrixUVMapper extends MapReduceBase implements Mapper<IntWritable, MatrixUVValueWritable, IntWritable, MatrixUVValueWritable> {
	
	private int xPos;
	private int matrixUOrV;
	
	public void configure(JobConf conf) {
		xPos = conf.getInt(UVDecomposer.MATRIX_X_POSITION, 1);
		
		// Change to int
		//matrixUOrV = conf.get(UVDecomposer.MATRIX_TYPE); 
	}
	
	public void map(IntWritable key, MatrixUVValueWritable element,
			OutputCollector<IntWritable, MatrixUVValueWritable> output, Reporter reporter) throws IOException {
		
		System.out.println("Row: "+key.get()+" Column: "+element.getColumn()+" Value: "+element.getValue_k());
		output.collect(key, element);
	}
}
