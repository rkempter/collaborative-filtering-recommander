package ch.epfl.advanceddatabase.rkempter.rmseminimizer;
import java.io.IOException;

import ch.epfl.advanceddatabase.rkempter.UVDecomposer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class MatrixUVMapper extends MapReduceBase implements Mapper<MatrixInputValueWritable, MatrixInputValueWritable, IntWritable, MatrixUVValueWritable> {
	
	private int xPos;
	private int matrixUOrV;
	
	public void configure(JobConf conf) {
		xPos = conf.getInt(UVDecomposer.MATRIX_X_POSITION, 1);
		
		// Change to int
		//matrixUOrV = conf.get(UVDecomposer.MATRIX_TYPE); 
	}
	
	public void map(MatrixInputValueWritable uElement, MatrixInputValueWritable vElement,
			OutputCollector<IntWritable, MatrixUVValueWritable> output, Reporter reporter) throws IOException {

		int row = uElement.getRow();
		int column = vElement.getColumn();
		float uValue = uElement.getValue();
		float vValue = vElement.getValue();
		float result_x = 0, result_k = 0;
		System.out.println("uElement: "+uElement.getRow()+" "+uElement.getColumn()+" "+uElement.getValue());
		System.out.println("vElement: "+vElement.getRow()+" "+vElement.getColumn()+" "+vElement.getValue());
		
		if(column != xPos) {
			result_k = uValue * vValue;
		} else {
			result_x = vValue;
		}
		
		MatrixUVValueWritable outValue = new MatrixUVValueWritable(matrixUOrV, column, result_x, result_k);
		IntWritable outKey = new IntWritable(row);
		
		output.collect(outKey, outValue);
	}
}
