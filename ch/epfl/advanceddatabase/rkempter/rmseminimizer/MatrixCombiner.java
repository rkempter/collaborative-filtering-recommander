package ch.epfl.advanceddatabase.rkempter.rmseminimizer;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import ch.epfl.advanceddatabase.rkempter.UVDecomposer;


public class MatrixCombiner extends MapReduceBase implements Reducer<IntWritable, MatrixUVValueWritable, IntWritable, MatrixUVValueWritable> {

	public void reduce(IntWritable key, Iterator<MatrixUVValueWritable> values,
			OutputCollector<IntWritable, MatrixUVValueWritable> output, Reporter reporter) throws IOException {
		
		float total_x = 0;
		float total_k = 0;
		int matrixType = 0, column = 0;
		int lastMatrixType = 0;
		int lastColumn = 0;
		
		while (values.hasNext()) {
			// replace ValueType with the real type of your value
			MatrixUVValueWritable element = values.next(); // This does not work!!
			matrixType = element.getType();
			column = element.getColumn();
			if(lastColumn != column) {
				MatrixUVValueWritable outValue = new MatrixUVValueWritable(lastMatrixType, lastColumn, total_x, total_k);
				output.collect(key, outValue);
				total_x = 0;
				total_k = 0;
				
				System.out.println("Row: "+key.get()+" column: "+lastColumn);
			}
			lastColumn = column;
			lastMatrixType = matrixType;
			
			if(matrixType == UVDecomposer.MATRIX_U ||  matrixType == UVDecomposer.MATRIX_V) {
				total_x += element.getValue_x();
				total_k += element.getValue_k();
			}
		}
		
		MatrixUVValueWritable outValue = new MatrixUVValueWritable(matrixType, column, total_x, total_k);
		output.collect(key, outValue);
	}

}
