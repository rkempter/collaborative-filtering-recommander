package ch.epfl.advanceddatabase.rkempter.rmseminimizer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import ch.epfl.advanceddatabase.rkempter.IntPair;
import ch.epfl.advanceddatabase.rkempter.UVDecomposer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class MatrixUVReducer extends MapReduceBase implements 
	Reducer<IntPair, InputWritable, IntWritable, MatrixInputValueWritable> {
	
	private int matrix = 1;
	
	public void configure(JobConf conf) {
		matrix = conf.getInt(UVDecomposer.MATRIX_TYPE, 1);
	}

	public void reduce(IntPair key, Iterator<InputWritable> values, OutputCollector<IntWritable, MatrixInputValueWritable> output, Reporter reporter) 
			throws IOException {
		
		// Store all elements
		ArrayList<InputWritable> elements = new ArrayList<InputWritable>();
		char matrixType;
		
		// Stock all incoming values
		while(values.hasNext()) {
			InputWritable el = (InputWritable) values.next();
			elements.add(new InputWritable(el.getIndex(), el.getmElement(), el.getuElements(), el.getvElements()));
		}
		
		Float[] xValues;
		if(matrix == UVDecomposer.MATRIX_U) {
			matrixType = 'U';
			xValues = (elements.get(0)).getuElements();
		} else {
			matrixType = 'V';
			xValues = (elements.get(0)).getvElements();
		}
		
		// Iterate over all 10 positions in the U-row, resp. V-column
		for(int pos = 0; pos < UVDecomposer.D_DIMENSION; pos++) {
			float xSum = 0;
			float kSum = 0;
			float mSum = 0;
			// For each element in the M-row / M-column
			for(int j = 0; j < elements.size(); j++) {
				InputWritable el = elements.get(j);
				float grade = el.getmElement();
				mSum += grade;
				Float otherArray[];
				if(matrix == UVDecomposer.MATRIX_U) {
					otherArray = el.getvElements();
				} else {
					otherArray = el.getuElements();
				}
				for(int i = 0; i < UVDecomposer.D_DIMENSION; i++) {
					if(pos == i) {
						xSum += otherArray[i];
					} else {
						kSum += otherArray[i] * xValues[i];
					}
				}
			}
			
			// Compute the derivative of the rmse in this row / column
			float xOut = (mSum - kSum) / xSum;
			// Save the value for further computations
			xValues[pos] = xOut;
			// Get the block of the element
			int index = (key.getIndex()-1) * UVDecomposer.D_DIMENSION + pos;
			
			if(matrix == UVDecomposer.MATRIX_U) {
				output.collect(new IntWritable(index), new MatrixInputValueWritable(matrixType, key.getIndex(), pos+1, xOut));
			} else {
				output.collect(new IntWritable(index), new MatrixInputValueWritable(matrixType, pos+1, key.getIndex(), xOut));
			}	
		}
		
		// RMSE
		for(int j = 0; j < elements.size(); j++) {
			InputWritable el = elements.get(j);
			float grade = el.getmElement();
			Float[] vals;
			if(matrix == UVDecomposer.MATRIX_U)
				vals = el.getvElements();
			else
				vals = el.getuElements();
			float xsumme = 0;
			for(int i = 0; i < 10; i++) {
				xsumme += xValues[i] * vals[i];
			}
			float rm = (float) Math.pow((double) (grade-xsumme), 2);
			Counter counter = reporter.getCounter(UVDecomposer.OVERALL_COUNTERS.RMSE_COUNTER);
			counter.increment((long) (rm*UVDecomposer.COUNTER_MULTIPLICATOR));
		}
	}

}
