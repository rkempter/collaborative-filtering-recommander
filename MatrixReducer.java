import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class MatrixReducer extends MapReduceBase implements 
	Reducer<IntWritable, MatrixValueWritable, MatrixUVKeyWritable, MatrixUVValueWritable> {

	public void reduce(IntWritable key, Iterator<MatrixValueWritable> values, OutputCollector<MatrixUVKeyWritable, MatrixUVValueWritable> output, Reporter reporter) 
			throws IOException {
		
		float total_x = 0;
		float total_k = 0;
		float total_m = 0;
		
		Float[] matrixValues = new Float[10];

		while (values.hasNext()) {
			// replace ValueType with the real type of your value
			MatrixValueWritable element = values.next();
			int matrixType = element.getMatrixType();
			int column = element.getColumn();
			matrixValues[column] = element.getValue();
			
			switch(matrixType) {
			case UVDecomposer.MATRIX_X_VALUE:
				total_x += element.getResult();
				break;
			case UVDecomposer.MATRIX_K_VALUE:
				total_k += element.getResult();
				break;
			case UVDecomposer.MATRIX_M_VALUE:
				total_m += element.getResult();
				break;
			}
		}
		
		float result = (total_m - total_k) / total_x;
		
		for(int i = 0; i < matrixValues.length; i++) {
			MatrixUVKeyWritable outKey = new MatrixUVKeyWritable(key, i, "U");
			if(matrixValues[i] == UVDecomposer.MATRIX_X_MARKER) {
				MatrixUVValueWritable outValue = new MatrixUVValueWritable(result);
			} else {
				MatrixUVValueWritable outValue = new MatrixUVValueWritable(matrixValues[i]);
			}
			
			output.collect(outKey, outValue);
		}
		
	}

}
