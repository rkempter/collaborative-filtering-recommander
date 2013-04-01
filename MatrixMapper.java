import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class MatrixMapper extends MapReduceBase implements Mapper<MatrixInputValueWritable, MatrixInputValueWritable, IntWritable, MatrixValueWritable> {
	
	public void map(MatrixInputValueWritable uElement, MatrixInputValueWritable vElement,
			OutputCollector<IntWritable, MatrixValueWritable> output, Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		int row = uElement.getRow();
		int column = uElement.getColumn();
		float uValue = uElement.getValue();
		float vValue = vElement.getValue();
		float result = uValue * vValue;
		
		// write out <(row), ([1...10] of u, uValue, rmse_result_x, rmse_result_k)>
		MatrixValueWritable outValue = new MatrixValueWritable(column, uValue, 0, result);
		IntWritable outKey = new IntWritable(row);
		
		output.collect(outKey, outValue);
	}
}
