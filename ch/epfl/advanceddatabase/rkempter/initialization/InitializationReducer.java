package ch.epfl.advanceddatabase.rkempter.initialization;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import ch.epfl.advanceddatabase.rkempter.IntPair;
import ch.epfl.advanceddatabase.rkempter.UVDecomposer;

public class InitializationReducer extends MapReduceBase implements Reducer<IntPair, TupleValueWritable, IntWritable, TupleValueWritable>
{

	public void reduce(IntPair key, Iterator<TupleValueWritable> values,
			OutputCollector<IntWritable, TupleValueWritable> output, Reporter reporter)
			throws IOException {
		
			int sum = 0;
			int counter = 0;
			ArrayList<TupleValueWritable> tupleValueList= new ArrayList<TupleValueWritable>();
			
			// Compute the average of the user rating
			while (values.hasNext()) {
				TupleValueWritable tv = values.next();
				tupleValueList.add(new TupleValueWritable(tv.getRow(), tv.getColumn(), tv.getGrade()));
				sum += tv.getGrade();
				counter++;
			}
			
			float avg = (float) sum / counter;
			
			// Normalize each element in the row
			for(int i = 0; i < tupleValueList.size(); i++) {
				TupleValueWritable value = tupleValueList.get(i);
				float grade = value.getGrade() - avg;
				value.setGrade(grade);
				output.collect(new IntWritable(UVDecomposer.MATRIX_M), value);
			}
			
			// For this row, generate the U values around the avg
			String matrix = "U";
			Random generator = new Random();
			for(int i = 0; i < 10; i++) {
				float val = generator.nextFloat() / 10;
				output.collect(new IntWritable(UVDecomposer.MATRIX_U), new TupleValueWritable(tupleValueList.get(0).getRow(), i, avg/10+val));
			}
	}
}
