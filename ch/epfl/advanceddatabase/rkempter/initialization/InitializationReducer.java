package ch.epfl.advanceddatabase.rkempter.initialization;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class InitializationReducer extends MapReduceBase implements Reducer<IntWritable, TupleValueWritable, IntWritable, TupleValueWritable>
{

	public void reduce(IntWritable key, Iterator<TupleValueWritable> values,
			OutputCollector<IntWritable, TupleValueWritable> output, Reporter reporter)
			throws IOException {
		
			int sum = 0;
			int counter = 0;
			ArrayList<TupleValueWritable> tupleValueList= new ArrayList<TupleValueWritable>();
			
			// Compute the average of the user rating
			while (values.hasNext()) {
				// Hate this!
				TupleValueWritable tv = values.next();
				TupleValueWritable tupleValue = new TupleValueWritable(tv.getIndex(), tv.getGrade());
				tupleValueList.add(tupleValue);
				sum += tv.getGrade();
				counter++;
			}
			
			float avg = (float) sum / counter;
			
			// Write adjusted matrix row out
			for(int i = 0; i < tupleValueList.size(); i++) {
				TupleValueWritable value = tupleValueList.get(i);
				float grade = value.getGrade() - avg;
				value.setGrade(grade);
				output.collect(key, value);
			}
			
			// Need to write U and V
	}

}
