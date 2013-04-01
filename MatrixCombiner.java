import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class MatrixCombiner extends MapReduceBase implements Reducer<IntWritable, MatrixValueWritable, IntWritable, MatrixValueWritable> {

	public void reduce(IntWritable key, Iterator<MatrixValueWritable> values,
			OutputCollector<IntWritable, MatrixValueWritable> output, Reporter reporter) throws IOException {
		
		Float[] cellValues = new Float[10];
		float total_x = 0;
		float total_k = 0;
		
		while (values.hasNext()) {
			// replace ValueType with the real type of your value
			MatrixValueWritable value = values.next(); // This does not work!!
			total_x += value.getResult_x();
			total_k += value.getResult_k();
			cellValues[value.getColumn()] = value.getValue();
			// process value
		}
		
		for(int i = 0; i < cellValues.length; i++) {
			MatrixValueWritable outValue = new MatrixValueWritable(i, cellValues[i], total_x, total_k);
			output.collect(key, outValue);
		}
	}

}
