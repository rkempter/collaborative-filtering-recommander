package ch.epfl.advanceddatabase.rkempter.rmseminimizer;

import java.io.IOException;


import ch.epfl.advanceddatabase.rkempter.initialization.TupleValueWritable;
import ch.epfl.advanceddatabase.rkempter.UVDecomposer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class MatrixMMapper extends MapReduceBase implements Mapper<IntWritable, TupleValueWritable, IntWritable, MatrixUVValueWritable> {

	// Identity Mapper
	public void map(IntWritable key, TupleValueWritable value,
			OutputCollector<IntWritable, MatrixUVValueWritable> output, Reporter reporter) throws IOException {
		
		MatrixUVValueWritable outputValue = new MatrixUVValueWritable(UVDecomposer.MATRIX_M, value.getIndex(), 0, value.getGrade());
		
		output.collect(key, outputValue);
		 
	}

}
